import streamlit as st
import shopify
import pandas as pd
import json
import time
from io import BytesIO
import datetime as dt
from pyactiveresource.connection import ClientError
# from update_app import run_update_app  # unused
from update_app import sync_product_fields

# =========================
# App & Shopify Setup
# =========================
st.set_page_config(layout="wide")

SHOP_URL = st.secrets["STORE_A_URL"]
TOKEN = st.secrets["TOKEN_A"]

STORE_B_URL = st.secrets.get("STORE_B_URL")
STORE_C_URL = st.secrets.get("STORE_C_URL")
TOKEN_B = st.secrets.get("TOKEN_B")
TOKEN_C = st.secrets.get("TOKEN_C")

API_VERSION = "2024-07"

SYNC_NAMESPACE = "sync"
SYNC_KEY = "sync_fields"

# =========================
# Helpers
# =========================
def _get_query_params():
    """Handle Streamlit's old/new query params APIs safely."""
    # Newer API
    try:
        qp = st.query_params
        try:
            return {k: qp.get(k) for k in qp.keys()}
        except Exception:
            pass
    except Exception:
        pass
    # Older API
    try:
        qp = st.experimental_get_query_params()
        return {k: (v[0] if isinstance(v, list) and v else v) for k, v in qp.items()}
    except Exception:
        return {}

def get_store_config():
    qp = _get_query_params()
    selected = (qp.get("store") or "").upper() if qp else ""

    store_options = {
        "A-(INT)": {"key": "A", "url": SHOP_URL,     "token": TOKEN,   "label": "Store A"},
        "B-(DA)":  {"key": "B", "url": STORE_B_URL,  "token": TOKEN_B, "label": "Store B"},
        "C-(DE)":  {"key": "C", "url": STORE_C_URL,  "token": TOKEN_C, "label": "Store C"},
    }

    keys = list(store_options.keys())
    default_index = 0
    if selected in ("A", "B", "C"):
        for i, k in enumerate(keys):
            if store_options[k]["key"] == selected:
                default_index = i
                break

    st.markdown("#### Store")
    store_label = st.selectbox(
        "Choose which shop to view/edit",
        keys,
        index=default_index,
        help="Tip: open multiple browser windows with ?store=A, ?store=B, ?store=C to compare side-by-side."
    )
    cfg = store_options[store_label]
    if not cfg["url"] or not cfg["token"]:
        st.error(f"Missing secrets for {cfg['label']}. Please set {cfg['label']} URL and token in Streamlit secrets.")
        st.stop()
    return cfg  # dict with key/url/token/label

def connect_to_store(shop_url=None, token=None):
    url = shop_url if shop_url else f"https://{SHOP_URL}"
    if shop_url and not str(shop_url).startswith("https://"):
        url = f"https://{shop_url}"
    tok = token if token else TOKEN
    session = shopify.Session(url, API_VERSION, tok)
    shopify.ShopifyResource.activate_session(session)

@st.cache_data(show_spinner=False)
def get_all_products_cached(shop_url, token):
    url = shop_url if str(shop_url).startswith("https://") else f"https://{shop_url}"
    session = shopify.Session(url, API_VERSION, token)
    shopify.ShopifyResource.activate_session(session)
    # DO NOT clear the session here
    all_products = []
    page = shopify.Product.find(limit=250)
    while page:
        all_products.extend(page)
        try:
            page = page.next_page()
        except Exception:
            break
    return all_products

def get_all_products():
    # Use cached fetch keyed by current store creds
    return get_all_products_cached(store_cfg["url"], store_cfg["token"])

# ---- Explicit metafield finders (avoid mixin) + pagination ----
def find_product_metafields_all(product_id, **kwargs):
    page = shopify.Metafield.find(resource="products", resource_id=product_id, limit=250, **kwargs)
    items = []
    while page:
        items.extend(page)
        try:
            page = page.next_page()
        except Exception:
            break
    return items

def find_variant_metafields_all(variant_id, **kwargs):
    page = shopify.Metafield.find(resource="variants", resource_id=variant_id, limit=250, **kwargs)
    items = []
    while page:
        items.extend(page)
        try:
            page = page.next_page()
        except Exception:
            break
    return items

def _metafields_for_resource(resource, **kwargs):
    """Fetch metafields for either a product or a variant without using the mixin."""
    try:
        if isinstance(resource, shopify.Product):
            return find_product_metafields_all(resource.id, **kwargs) or []
        else:
            # Assume variant for anything else passed here
            return find_variant_metafields_all(resource.id, **kwargs) or []
    except Exception:
        return []

def _product_metafield_map(product):
    """
    Return {(namespace, key): metafield_obj} for a product.
    Uses explicit Metafield.find to avoid mixins.
    """
    m = {}
    for mf in find_product_metafields_all(product.id):
        ns = getattr(mf, "namespace", None)
        k = getattr(mf, "key", None)
        if ns and k:
            m[(ns, k)] = mf
    return m

def _normalize_value_for_type(value, mtype):
    """
    Convert raw value to the appropriate python type for the declared metafield type.
    We keep it conservative; only coerce when safe.
    """
    try:
        if mtype in ("integer", "number", "float", "decimal"):
            return int(value) if mtype == "integer" else float(value)
        if mtype == "boolean":
            return str(value).lower() in ("true", "1", "yes", "y", "t")
        if mtype == "json":
            if isinstance(value, (dict, list)):
                return value
            try:
                return json.loads(value)
            except Exception:
                return value
        return "" if value is None else str(value)
    except Exception:
        return value

def get_sync_keys(resource):
    """Return list of keys marked for sync (stored in SYNC_NAMESPACE/SYNC_KEY json)."""
    try:
        for m in _metafields_for_resource(resource):
            if getattr(m, "namespace", None) == SYNC_NAMESPACE and getattr(m, "key", None) == SYNC_KEY:
                try:
                    return json.loads(m.value)
                except Exception:
                    return []
    except Exception:
        pass
    return []

def save_sync_keys(resource, keys):
    """
    Create/update the metafield that stores the list of keys to sync.
    Avoids resource.metafields() mixin calls.
    """
    try:
        # Find existing sync metafield (if any)
        existing = None
        for m in _metafields_for_resource(resource):
            if getattr(m, "namespace", None) == SYNC_NAMESPACE and getattr(m, "key", None) == SYNC_KEY:
                existing = m
                break

        meta = existing or shopify.Metafield()
        meta.namespace = SYNC_NAMESPACE
        meta.key = SYNC_KEY
        meta.owner_id = resource.id
        meta.owner_resource = "product" if isinstance(resource, shopify.Product) else "variant"
        meta.type = "json"
        meta.value = json.dumps(keys)
        return meta.save()
    except Exception:
        return False

def apply_sync_keys_to_category(products, product_type, product_sync_keys, variant_sync_keys):
    for p in products:
        if p.product_type == product_type and p.id != selected_product.id:
            try:
                save_sync_keys(p, product_sync_keys)
                for variant in p.variants:
                    save_sync_keys(variant, variant_sync_keys)
                    time.sleep(0.6)
            except Exception:
                time.sleep(2)
                save_sync_keys(p, product_sync_keys)

# ---------- Copy logic ----------
def copy_product_metafields(
    donor_product,
    receiver_product,
    keys_to_copy=None,              # list[str] (without namespace) or None
    namespace_filter=None,          # str | list[str] | None
    overwrite=False,                # do not touch existing if False
    only_synced=False,              # copy only keys marked for sync on donor
    dry_run=False,                  # simulate changes without saving
):
    logs = []
    ns_filter = set([namespace_filter]) if isinstance(namespace_filter, str) else (set(namespace_filter) if namespace_filter else None)

    donor_metafields = list(find_product_metafields_all(donor_product.id))
    receiver_map = _product_metafield_map(receiver_product)

    synced_keyset = set(get_sync_keys(donor_product)) if only_synced else None

    total = copied = skipped_exists = skipped_ns = skipped_key = skipped_unsynced = errors = 0

    for dm in donor_metafields:
        ns = getattr(dm, "namespace", None)
        key = getattr(dm, "key", None)
        if not ns or not key:
            continue

        if ns_filter and ns not in ns_filter:
            skipped_ns += 1
            continue

        if synced_keyset is not None and key not in synced_keyset:
            skipped_unsynced += 1
            continue

        if keys_to_copy is not None and key not in set(keys_to_copy):
            skipped_key += 1
            continue

        total += 1
        receiver_existing = receiver_map.get((ns, key))

        if receiver_existing and not overwrite:
            skipped_exists += 1
            continue

        mtype = getattr(dm, "type", None) or "string"
        value = _normalize_value_for_type(getattr(dm, "value", None), mtype)

        try:
            if dry_run:
                action = "UPDATE" if receiver_existing else "CREATE"
                logs.append(f"[DRY RUN] {action} {ns}.{key} = {value!r} (type={mtype})")
                copied += 1
                continue

            if receiver_existing:
                receiver_existing.value = value
                try:
                    if getattr(receiver_existing, "type", None) in (None, "", "string"):
                        receiver_existing.type = mtype
                except Exception:
                    pass
                ok = receiver_existing.save()
            else:
                mf = shopify.Metafield()
                mf.namespace = ns
                mf.key = key
                mf.type = mtype
                mf.value = value
                mf.owner_id = receiver_product.id
                mf.owner_resource = "product"
                ok = mf.save()

            if ok:
                copied += 1
            else:
                errors += 1
                logs.append(f"❌ Failed to save '{ns}.{key}' on receiver {receiver_product.id}")
            time.sleep(0.5)
        except Exception as e:
            errors += 1
            logs.append(f"❌ Error copying '{ns}.{key}': {e}")

    summary = (
        f"Copied {copied}/{total} metafields "
        f"(skipped existing: {skipped_exists}, skipped by namespace: {skipped_ns}, "
        f"skipped by key filter: {skipped_key}, skipped not-synced: {skipped_unsynced}, errors: {errors})."
    )
    logs.insert(0, f"🧬 Metafield copy: donor {donor_product.id} → receiver {receiver_product.id} "
                   f"{'(DRY RUN)' if dry_run else ''}")
    logs.append(summary)
    return {"summary": summary, "logs": logs}

# ---------- EXPORT HELPERS ----------
def _is_effectively_empty(v):
    # None or blank strings count as empty; numbers 0, False, "0" are NOT empty.
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False

def _drop_all_empty_columns(df, keep_always=None):
    """
    Remove columns where every row is empty (None or blank string).
    keep_always: set of column names to always keep (e.g., ids/titles)
    """
    if df is None or df.empty:
        return df
    keep_always = keep_always or set()
    cols_to_keep = []
    for col in df.columns:
        if col in keep_always:
            cols_to_keep.append(col)
            continue
        series = df[col]
        if not all(_is_effectively_empty(v) for v in series):
            cols_to_keep.append(col)
    return df[cols_to_keep]

def metafields_dict(resource, only_synced=False):
    """
    Returns a flat dict of metafields for a product or variant:
    keys look like 'namespace.key' -> value (None/str preserved)
    """
    allowed_keys = set(get_sync_keys(resource)) if only_synced else None
    out = {}
    try:
        for m in _metafields_for_resource(resource):
            if only_synced and (m.key not in allowed_keys):
                continue
            ns = getattr(m, "namespace", "mf")
            key = f"{ns}.{m.key}"
            val = m.value
            if isinstance(val, str) and val.strip() == "":
                val = None
            out[key] = val
    except Exception:
        pass
    return out

def build_category_export(products_in_type, only_synced=False, include_variants=True):
    """
    Build two DataFrames:
      - products_df: one row per product (standard fields + metafields)
      - variants_df: one row per variant (standard fields + metafields)
    Drops any columns that are completely empty across the sheet,
    while always keeping key identifiers.
    """
    product_rows, variant_rows = [], []

    for idx, p in enumerate(products_in_type, 1):
        # --- Product row (standard fields)
        base = {
            "product_id": p.id,
            "title": getattr(p, "title", None) or None,
            "handle": getattr(p, "handle", None) or None,
            "vendor": getattr(p, "vendor", None) or None,
            "product_type": getattr(p, "product_type", None) or None,
            "status": getattr(p, "status", None) or None,
            "tags": (
                ", ".join(p.tags) if isinstance(getattr(p, "tags", ""), list)
                else (getattr(p, "tags", None) or None)
            ),
            "created_at": getattr(p, "created_at", None) or None,
            "updated_at": getattr(p, "updated_at", None) or None,
        }
        base.update(metafields_dict(p, only_synced=only_synced))
        product_rows.append(base)

        # --- Variant rows (optional)
        if include_variants:
            for v in getattr(p, "variants", []):
                vbase = {
                    "product_id": p.id,
                    "product_title": getattr(p, "title", None) or None,
                    "variant_id": getattr(v, "id", None) or None,
                    "variant_title": getattr(v, "title", None) or None,
                    "sku": getattr(v, "sku", None) or None,
                    "barcode": getattr(v, "barcode", None) or None,
                    "price": getattr(v, "price", None) or None,
                    "compare_at_price": getattr(v, "compare_at_price", None) or None,
                    "position": getattr(v, "position", None) or None,
                    "option1": getattr(v, "option1", None) or None,
                    "option2": getattr(v, "option2", None) or None,
                    "option3": getattr(v, "option3", None) or None,
                }
                vbase.update(metafields_dict(v, only_synced=only_synced))
                variant_rows.append(vbase)
                time.sleep(0.4)  # be gentle with rate limits

        if idx % 10 == 0:
            time.sleep(0.2)

    products_df = pd.DataFrame(product_rows) if product_rows else pd.DataFrame()
    variants_df = pd.DataFrame(variant_rows) if variant_rows else pd.DataFrame()

    # Drop columns that are entirely empty, but always keep identifiers
    products_df = _drop_all_empty_columns(
        products_df,
        keep_always={"product_id", "title", "handle", "product_type"}
    )
    if not variants_df.empty:
        variants_df = _drop_all_empty_columns(
            variants_df,
            keep_always={"product_id", "product_title", "variant_id", "variant_title", "sku", "barcode"}
        )

    return products_df, variants_df

def make_xlsx_download(products_df, variants_df, store_key, category_label):
    """
    Write the two DataFrames into an in-memory XLSX with 2 sheets.
    Uses XlsxWriter to avoid openpyxl dependency issues.
    """
    # Ensure DataFrame objects exist
    if products_df is None:
        products_df = pd.DataFrame()
    if variants_df is None:
        variants_df = pd.DataFrame()

    buf = BytesIO()
    try:
        # Use XlsxWriter for writing
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            products_df.to_excel(writer, index=False, sheet_name="Products")
            variants_df.to_excel(writer, index=False, sheet_name="Variants")
    except Exception as e:
        st.error(
            "Failed to build the Excel file. Make sure 'XlsxWriter' is installed "
            "(add it to requirements.txt). Error: {}".format(e)
        )
        raise

    buf.seek(0)
    safe_cat = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(category_label))[:60]
    fname = f"export_{store_key}_{dt.date.today().isoformat()}.xlsx" if not safe_cat else \
            f"export_{store_key}_{safe_cat}_{dt.date.today().isoformat()}.xlsx"
    return fname, buf
# ---------- END EXPORT HELPERS ----------

# =========================
# UI
# =========================
st.title("🔧 Shopify Product + Variant Metafield Sync Tool")

store_cfg = get_store_config()
connect_to_store(store_cfg["url"], store_cfg["token"])
store_key = store_cfg["key"]
store_label = store_cfg["label"]

st.info(f"Viewing & editing: **{store_label}**", icon="🏬")

# Cache per store key in session as well (fast switching)
state_key = f"products_{store_key}"
if state_key not in st.session_state:
    with st.spinner(f"Loading products from {store_label}..."):
        st.session_state[state_key] = get_all_products()

if st.button("🔄 Refresh product list"):
    st.session_state.pop(state_key, None)
    st.rerun()

products = st.session_state.get(state_key, [])
if not products:
    st.warning(f"No products found in {store_label}.")
    st.stop()

product_types = sorted(set(p.product_type for p in products if getattr(p, "product_type", None)))
selected_type = st.selectbox("Select a Product Category", product_types)
filtered_products = [p for p in products if p.product_type == selected_type]

selected_product = st.selectbox(
    "Select a Product",
    filtered_products,
    format_func=lambda p: f"{getattr(p, 'title', '—')} (ID: {getattr(p, 'id', '—')})"
)

show_only_sync = st.checkbox("🔁 Show only synced metafields", value=False)

# ---------- EXPORT UI ----------
st.markdown("### 📤 Export this Category")
colx1, colx2, colx3 = st.columns([1, 1, 2])
with colx1:
    export_only_synced = st.checkbox(
        "Only synced metafields",
        value=show_only_sync,
        help="Exports only metafields you’ve marked to sync."
    )
with colx2:
    export_include_variants = st.checkbox("Include variants", value=True)
build_and_show = st.button("⬇️ Build export file")

if build_and_show:
    with st.spinner("Building export…"):
        prod_df, var_df = build_category_export(
            filtered_products,
            only_synced=export_only_synced,
            include_variants=export_include_variants,
        )
        if prod_df.empty and (var_df.empty or not export_include_variants):
            st.warning("Nothing to export for this category.")
        else:
            fname, data = make_xlsx_download(prod_df, var_df, store_key, selected_type)
            st.success("Export ready.")
            st.download_button(
                "Download XLSX",
                data=data.getvalue() if hasattr(data, "getvalue") else data,
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            with st.expander("Preview: Products (first 50 rows)"):
                st.dataframe(prod_df.head(50), use_container_width=True)
            if export_include_variants and not var_df.empty:
                with st.expander("Preview: Variants (first 50 rows)"):
                    st.dataframe(var_df.head(50), use_container_width=True)
# ---------- END EXPORT UI ----------

# ---------- Copy Product Metafields UI ----------
with st.expander("🧬 Copy Product Metafields", expanded=False):

    # Choose donor/receiver from *any* products (not just filtered category)
    colcp1, colcp2 = st.columns(2)
    with colcp1:
        donor_product = st.selectbox(
            "Donor product (copy FROM)",
            products,
            format_func=lambda p: f"{getattr(p, 'title', '—')} (ID: {getattr(p, 'id', '—')})",
            key=f"donor_select_{store_key}",
        )
    with colcp2:
        receiver_product = st.selectbox(
            "Receiver product (copy TO)",
            products,
            format_func=lambda p: f"{getattr(p, 'title', '—')} (ID: {getattr(p, 'id', '—')})",
            key=f"receiver_select_{store_key}",
        )

    # Fetch donor keys for selection (namespaced & plain)
    donor_metafields_list = list(find_product_metafields_all(getattr(donor_product, "id", 0))) if donor_product else []
    donor_keys_plain = sorted({getattr(m, "key", "") for m in donor_metafields_list if getattr(m, "key", "")})
    donor_namespaced = sorted({
        f"{getattr(m, 'namespace', 'mf')}.{getattr(m, 'key', '')}"
        for m in donor_metafields_list if getattr(m, "key", "")
    })

    # Advanced options
    with st.expander("Advanced copy options", expanded=False):
        ns_filter_text = st.text_input(
            "Namespace filter (comma-separated, leave blank for all)",
            value="",
            help="Example: custom, seo, my_namespace",
            key=f"ns_filter_{store_key}",
        )
        namespace_filter = [s.strip() for s in ns_filter_text.split(",") if s.strip()] or None

        overwrite_existing = st.checkbox(
            "Overwrite existing receiver metafields",
            value=False,
            help="If unchecked, existing (namespace,key) pairs on the receiver are not changed.",
            key=f"overwrite_{store_key}",
        )

        st.caption("Select specific keys (by key name, not namespace) or leave empty to copy all keys in the chosen namespaces.")
        keys_to_copy = st.multiselect(
            "Keys to copy",
            donor_keys_plain,
            default=[],
            key=f"keys_to_copy_{store_key}",
        )

        only_synced_keys = st.checkbox(
            "Copy only keys marked for sync on donor",
            value=False,
            help="Uses the donor's sync metafield (sync/sync_fields) to restrict which keys are copied.",
            key=f"only_synced_{store_key}",
        )

        dry_run = st.checkbox(
            "Dry run (no writes)",
            value=False,
            help="Preview what would be created/updated without saving anything.",
            key=f"dry_run_{store_key}",
        )

    # Always-visible list of donor namespaced keys
    if donor_namespaced:
        st.caption("Donor has the following namespaced keys available:")
        st.code("\n".join(donor_namespaced), language="text")
    else:
        st.caption("Donor has no metafields (or none fetched yet).")

    # Action button
    col_btn, _ = st.columns([1, 3])
    with col_btn:
        copy_clicked = st.button("➡️ Copy metafields from donor → receiver", type="primary", use_container_width=True)

    if copy_clicked:
        if not donor_product or not receiver_product:
            st.warning("Pick both a donor and receiver product.")
        elif donor_product.id == receiver_product.id:
            st.warning("Donor and receiver must be different products.")
        else:
            # ensure session is live right before API calls
            connect_to_store(store_cfg["url"], store_cfg["token"])
            with st.spinner("Copying metafields..."):
                result = copy_product_metafields(
                    donor_product=donor_product,
                    receiver_product=receiver_product,
                    keys_to_copy=keys_to_copy if keys_to_copy else None,
                    namespace_filter=namespace_filter,
                    overwrite=overwrite_existing,
                    only_synced=only_synced_keys,
                    dry_run=dry_run,
                )
                if dry_run:
                    st.info("Dry run complete — no changes saved.")
                st.success(result["summary"])
                with st.expander("Copy details", expanded=False):
                    for line in result["logs"]:
                        st.write(line)


# ---------- Info & Actions ----------
product_save_logs = []
variant_save_logs = []
sync_logs = []

st.markdown("""
💡 **How it works:**
- Edit any metafield values and click **Save All Changes** to update the data for the store you are in. (Shop A, B or C). Nothing changed in A will change in B or C.
- Tick the **sync** box if you want that field included when syncing across stores.
- The button **Sync This Product to Shop B & C** only pushes fields that are marked for sync,
  and it uses the **latest saved values** from this shop.
""")

st.markdown("### 💾 Save & Synchronize Metafields")
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    save_clicked = st.button("✅ Save All Changes")
with col2:
    apply_sync_clicked = st.button("📦 Apply Sync Settings to All in Category")
with col3:
    if store_key == "A":
        cross_sync_clicked = st.button("📡 Sync This Product to Shop B & C (via EAN)")
    else:
        st.caption("Cross-store sync is available when viewing Store A.")
        cross_sync_clicked = False

# ---------- Standard Product Fields ----------
standard_fields_schema = [
    ("title", "Title"),
    ("handle", "Handle"),
    ("vendor", "Vendor"),
    ("product_type", "Product Type"),
    ("tags", "Tags (comma-separated)"),
    ("status", "Status (active/draft/archived)"),
]

def _std_get_val(attr):
    v = getattr(selected_product, attr, "")
    if attr == "tags":
        if isinstance(v, list):
            return ", ".join(v)
        return str(v or "")
    return "" if v is None else str(v)

_std_rows = [{"field": label, "attr": attr, "value": _std_get_val(attr)}
             for attr, label in standard_fields_schema]
_std_df = pd.DataFrame(_std_rows, columns=["field", "attr", "value"])

st.markdown("### 📦 Standard Product Fields")
edited_std_df = st.data_editor(
    _std_df.drop(columns=["attr"]),
    num_rows="fixed",
    use_container_width=True,
    key="standard_editor"
)
st.session_state["_std_attr_map"] = {row["field"]: row["attr"] for row in _std_rows}

# ---------- Product Metafields ----------
product_fields = []
sync_keys = get_sync_keys(selected_product)
existing_fields = {m.key: m for m in find_product_metafields_all(selected_product.id)}
for key, m in existing_fields.items():
    if show_only_sync and key not in sync_keys:
        continue
    product_fields.append({
        "key": key,
        "value": str(m.value) if m.value is not None else "",
        "sync": key in sync_keys,
        "product_id": selected_product.id,
        "product_title": selected_product.title,
        "type": getattr(m, "type", "string"),
        "metafield_obj": m
    })

if product_fields:
    st.markdown(f"### 🔍 Product Metafields — {store_label}")
    df = pd.DataFrame(product_fields).drop(columns=["metafield_obj"])
    edited_df = st.data_editor(df, num_rows="fixed", use_container_width=True, key=f"product_editor_{store_key}")
else:
    edited_df = None

# ---------- Variant Metafields ----------
variant_rows = []
variant_map = {}
variant_sync_map = {}

for variant in selected_product.variants:
    time.sleep(0.6)
    try:
        variant_map[variant.id] = variant
        variant_sync_keys = get_sync_keys(variant)
        variant_sync_map[variant.id] = variant_sync_keys
        existing_fields = {m.key: m for m in find_variant_metafields_all(variant.id)}
        for key, m in existing_fields.items():
            if show_only_sync and key not in variant_sync_keys:
                continue
            variant_rows.append({
                "key": key,
                "value": str(m.value) if m.value is not None else "",
                "sync": key in variant_sync_keys,
                "variant_id": variant.id,
                "variant_title": variant.title,
                "product_id": selected_product.id,
                "type": getattr(m, "type", "string"),
                "metafield_obj": m
            })
    except ClientError as e:
        if '429' in str(e):
            time.sleep(2)

if variant_rows:
    st.markdown(f"### 🔍 Variant Metafields — {store_label}")
    df_v = pd.DataFrame(variant_rows).drop(columns=["metafield_obj"])
    edited_df_v = st.data_editor(df_v, num_rows="fixed", use_container_width=True, key=f"variant_editor_{store_key}")
else:
    edited_df_v = None

# ---------- Save Logic ----------
if save_clicked:
    # --- Save standard fields (Title, Handle, Vendor, Product Type, Tags, Status) ---
    try:
        std_map = st.session_state.get("_std_attr_map", {})
        std_updates = {}

        if edited_std_df is not None:
            df_std_current = edited_std_df
            for _, row in df_std_current.iterrows():
                field_label = row["field"]
                new_val = str(row["value"]) if row["value"] is not None else ""
                attr = std_map.get(field_label)
                if not attr:
                    continue

                current = getattr(selected_product, attr, "")
                if attr == "tags":
                    current_str = ", ".join(current) if isinstance(current, list) else (current or "")
                    if new_val.strip() != str(current_str).strip():
                        std_updates[attr] = new_val
                else:
                    if str(current or "") != new_val:
                        std_updates[attr] = new_val

        # Optional: validate status
        if "status" in std_updates:
            val = std_updates["status"].strip().lower()
            if val not in {"active", "draft", "archived"}:
                product_save_logs.append(
                    f"⚠️ Skipped invalid status '{std_updates['status']}'. Use active/draft/archived."
                )
                std_updates.pop("status", None)
            else:
                std_updates["status"] = val

        # --- Save standard fields ---
        if std_updates:
            # Normalize tags BEFORE saving
            if "tags" in std_updates:
                std_updates["tags"] = ", ".join(
                    [t.strip() for t in str(std_updates["tags"]).split(",") if t.strip()]
                )

            # Apply updates to the product object once
            for attr, v in std_updates.items():
                setattr(selected_product, attr, v)

            # Save, with clearer 422 handling and a key=value success log
            try:
                selected_product.save()
                saved_pairs = [f"{k}='{v}'" for k, v in std_updates.items()]
                product_save_logs.append("✅ Saved standard fields: " + ", ".join(saved_pairs))
                time.sleep(0.6)
            except Exception as e:
                msg = str(e)
                if "422" in msg:
                    product_save_logs.append(
                        f"❌ Error saving standard fields (422 Unprocessable Entity): {e} — "
                        "check required fields and valid values (e.g., status active/draft/archived)."
                    )
                else:
                    product_save_logs.append(
                        f"❌ Error saving standard fields: {e} — "
                        "check that required fields are valid for this product type."
                    )

    except Exception as e:
        product_save_logs.append(f"❌ Error preparing standard field saves: {e}")

    # --- Save product metafields & sync keys ---
    if edited_df is not None:
        updated_product_sync_keys = []
        row_lookup = {row["key"]: row["metafield_obj"] for row in product_fields}
        type_lookup = {row["key"]: row["type"] for row in product_fields}

        for _, row in edited_df.iterrows():
            key = row["key"]
            new_value = row["value"]
            sync_flag = bool(row["sync"])
            original = row_lookup.get(key)
            original_type = type_lookup.get(key, "string")

            if sync_flag:
                updated_product_sync_keys.append(key)

            if original and str(original.value) != str(new_value):
                try:
                    if original_type == "integer":
                        original.value = int(new_value)
                    elif original_type == "boolean":
                        original.value = str(new_value).lower() in ["true", "1", "yes"]
                    elif original_type == "json":
                        original.value = json.loads(new_value)
                    elif original_type in ["float", "decimal"]:
                        original.value = float(new_value)
                    else:
                        original.value = new_value
                    original.save()
                    time.sleep(0.6)
                except Exception as e:
                    product_save_logs.append(f"❌ Error saving product metafield '{key}': {e}")

        if save_sync_keys(selected_product, updated_product_sync_keys):
            product_save_logs.append(f"✅ Saved product sync fields: {', '.join(updated_product_sync_keys)}")

    # --- Save variant metafields & sync keys ---
    if edited_df_v is not None:
        row_lookup = {(row["variant_id"], row["key"]): row["metafield_obj"] for row in variant_rows}
        type_lookup = {(row["variant_id"], row["key"]): row["type"] for row in variant_rows}

        grouped = edited_df_v.groupby("variant_id")
        for variant_id, rows in grouped:
            variant = variant_map[variant_id]
            keys_to_sync = []

            for _, row in rows.iterrows():
                key = row["key"]
                new_value = row["value"]
                sync_flag = bool(row["sync"])
                original = row_lookup.get((variant_id, key))
                original_type = type_lookup.get((variant_id, key), "string")

                if sync_flag:
                    keys_to_sync.append(key)

                if original and str(original.value) != str(new_value):
                    try:
                        if original_type == "integer":
                            original.value = int(new_value)
                        elif original_type == "boolean":
                            original.value = str(new_value).lower() in ["true", "1", "yes"]
                        elif original_type == "json":
                            original.value = json.loads(new_value)
                        elif original_type in ["float", "decimal"]:
                            original.value = float(new_value)
                        else:
                            original.value = new_value
                        original.save()
                        time.sleep(0.6)
                    except Exception as e:
                        variant_save_logs.append(f"❌ Error saving variant {variant_id} metafield '{key}': {e}")

            if save_sync_keys(variant, keys_to_sync):
                variant_save_logs.append(f"✅ Saved variant {variant_id} sync fields: {', '.join(keys_to_sync)}")

# --- Apply Sync (within selected store) ---
if apply_sync_clicked:
    current_product_keys = get_sync_keys(selected_product)
    current_variant_keys = set()
    for v in selected_product.variants:
        current_variant_keys.update(get_sync_keys(v))

    apply_sync_keys_to_category(
        products,
        selected_product.product_type,
        current_product_keys,
        list(current_variant_keys)
    )
    product_save_logs.append("✅ Sync settings applied to all products and variants in this category.")

# --- Cross-store Sync (kept as-is) ---
if cross_sync_clicked:
    results = sync_product_fields(selected_product)
    if results:
        st.markdown("### 🌐 Cross-Store Sync Results")
        for shop, result in results.items():
            st.write(f"**{shop}**")
            if "error" in result:
                st.write(f"❌ {result['error']}")
            else:
                for key, status in result.items():
                    st.write(f"{key}: {status}")

# --- Log Display ---
with st.expander("💬 Save/Sync Output Logs", expanded=False):
    if product_save_logs:
        st.markdown("### 🛍️ Product Save Logs")
        for log in product_save_logs:
            st.write(log)
    if variant_save_logs:
        st.markdown("### 🎯 Variant Save Logs")
        for log in variant_save_logs:
            st.write(log)
    if sync_logs:
        st.markdown("### 🔄 Sync Logs")
        for log in sync_logs:
            st.write(log)
