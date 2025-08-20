import streamlit as st
import shopify
import pandas as pd
import json
import time
from pyactiveresource.connection import ClientError
from update_app import run_update_app
from update_app import sync_product_fields

# --- Set wide layout ---
st.set_page_config(layout="wide")

# --- Shopify Setup (add B & C) ---
SHOP_URL = st.secrets["STORE_A_URL"]
TOKEN = st.secrets["TOKEN_A"]

# NEW: add B & C secrets
STORE_B_URL = st.secrets.get("STORE_B_URL")
STORE_C_URL = st.secrets.get("STORE_C_URL")
TOKEN_B = st.secrets.get("TOKEN_B")
TOKEN_C = st.secrets.get("TOKEN_C")

API_VERSION = "2024-07"

SYNC_NAMESPACE = "sync"
SYNC_KEY = "sync_fields"

# NEW: small helper to pick which store we connect to
def get_store_config():
    # Support query params so you can open separate windows like ?store=A/B/C
    # Works with Streamlit's newer st.query_params and older experimental_get_query_params
    selected = None
    try:
        qp = st.query_params  # modern API
        selected = qp.get("store", None)
    except Exception:
        try:
            qp = st.experimental_get_query_params()  # older API
            selected = qp.get("store", [None])[0] if "store" in qp else None
        except Exception:
            selected = None

    store_options = {
        "A-(INT)": {"key": "A", "url": SHOP_URL,     "token": TOKEN,   "label": "Store A"},
        "B-(DA)":          {"key": "B", "url": STORE_B_URL,  "token": TOKEN_B, "label": "Store B"},
        "C-(DE)":          {"key": "C", "url": STORE_C_URL,  "token": TOKEN_C, "label": "Store C"},
    }

    # Determine default index from query param
    default_index = 0
    if selected and selected.upper() in ("A", "B", "C"):
        keys = list(store_options.keys())
        for i, k in enumerate(keys):
            if store_options[k]["key"] == selected.upper():
                default_index = i
                break

    st.markdown("#### Store")
    store_label = st.selectbox(
        "Choose which shop to view/edit",
        list(store_options.keys()),
        index=default_index,
        help="Tip: open multiple browser windows with ?store=A, ?store=B, ?store=C to compare side-by-side."
    )
    cfg = store_options[store_label]
    if not cfg["url"] or not cfg["token"]:
        st.error(f"Missing secrets for {cfg['label']}. Please set {cfg['label']} URL and token in Streamlit secrets.")
        st.stop()
    return cfg  # dict with key/url/token/label

def connect_to_store(shop_url=None, token=None):
    # NEW: dynamic connection based on selected store
    url = shop_url if shop_url else f"https://{SHOP_URL}"
    if shop_url and not shop_url.startswith("https://"):
        url = f"https://{shop_url}"
    tok = token if token else TOKEN
    session = shopify.Session(url, API_VERSION, tok)
    shopify.ShopifyResource.activate_session(session)

def get_all_products():
    all_products = []
    page = shopify.Product.find(limit=250)
    while page:
        all_products.extend(page)
        try:
            page = page.next_page()
        except Exception:
            break
    return all_products

def get_sync_keys(resource):
    for m in resource.metafields():
        if m.namespace == SYNC_NAMESPACE and m.key == SYNC_KEY:
            try:
                return json.loads(m.value)
            except:
                return []
    return []

def save_sync_keys(resource, keys):
    meta = next((m for m in resource.metafields() if m.namespace == SYNC_NAMESPACE and m.key == SYNC_KEY), None)
    if not meta:
        meta = shopify.Metafield()
        meta.namespace = SYNC_NAMESPACE
        meta.key = SYNC_KEY
        meta.owner_id = resource.id
        meta.owner_resource = "product" if isinstance(resource, shopify.Product) else "variant"
        meta.type = "json"
    meta.value = json.dumps(keys)
    return meta.save()

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

# --- Streamlit UI ---
st.title("🔧 Shopify Product + Variant Metafield Sync Tool")

# NEW: choose store (dropdown + query param support)
store_cfg = get_store_config()
connect_to_store(store_cfg["url"], store_cfg["token"])
store_key = store_cfg["key"]
store_label = store_cfg["label"]

st.info(f"Viewing & editing: **{store_label}**", icon="🏬")

# Load products (cache per store so switching is fast)
state_key = f"products_{store_key}"
if state_key not in st.session_state:
    with st.spinner(f"Loading products from {store_label}..."):
        st.session_state[state_key] = get_all_products()

products = st.session_state[state_key]
if not products:
    st.warning(f"No products found in {store_label}.")
    st.stop()

product_types = sorted(set(p.product_type for p in products if p.product_type))
selected_type = st.selectbox("Select a Product Category", product_types)
filtered_products = [p for p in products if p.product_type == selected_type]

selected_product = st.selectbox(
    "Select a Product",
    filtered_products,
    format_func=lambda p: f"{p.title} (ID: {p.id})"
)

show_only_sync = st.checkbox("🔁 Show only synced metafields", value=False)

# --- Logging containers ---
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

# --- Save & Sync Buttons ---
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


# --- Standard Product Fields (editable) ---
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
        # Shopify often stores tags as a comma-separated string
        if isinstance(v, list):
            return ", ".join(v)
        return str(v or "")
    return "" if v is None else str(v)

_std_rows = [{"field": label, "attr": attr, "value": _std_get_val(attr)}
             for attr, label in standard_fields_schema]
_std_df = pd.DataFrame(_std_rows, columns=["field", "attr", "value"])

st.markdown("### 📦 Standard Product Fields")
# Show a 2-column editable table (Field | Value). Keep "attr" in session so we know what to save.
edited_std_df = st.data_editor(
    _std_df.drop(columns=["attr"]),
    num_rows="fixed",
    use_container_width=True,
    key="standard_editor"
)
st.session_state["_std_attr_map"] = {row["field"]: row["attr"] for row in _std_rows}




# --- Product Fields ---
product_fields = []
sync_keys = get_sync_keys(selected_product)
existing_fields = {m.key: m for m in selected_product.metafields()}
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

# --- Variant Fields ---
variant_rows = []
variant_map = {}
variant_sync_map = {}

for variant in selected_product.variants:
    time.sleep(0.6)
    try:
        variant_map[variant.id] = variant
        variant_sync_keys = get_sync_keys(variant)
        variant_sync_map[variant.id] = variant_sync_keys
        existing_fields = {m.key: m for m in variant.metafields()}
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

# --- Save Logic ---
if save_clicked:
    # --- Save standard fields (Title, Handle, Vendor, Product Type, Tags, Status) ---
    try:
        std_map = st.session_state.get("_std_attr_map", {})
        std_updates = {}

        if "edited_std_df" in locals() or "edited_std_df" in globals():
            df_std_current = edited_std_df  # from the table we created above
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


    
    if "edited_df" in locals() and edited_df is not None:
        updated_product_sync_keys = []
        row_lookup = {row["key"]: row["metafield_obj"] for row in product_fields}
        type_lookup = {row["key"]: row["type"] for row in product_fields}

        for _, row in edited_df.iterrows():
            key = row["key"]
            new_value = row["value"]
            sync_flag = row["sync"]
            original = row_lookup.get(key)
            original_type = type_lookup.get(key, "string")

            if sync_flag:
                updated_product_sync_keys.append(key)

            if original and str(original.value) != str(new_value):
                try:
                    if original_type == "integer":
                        original.value = int(new_value)
                    elif original_type == "boolean":
                        original.value = new_value.lower() in ["true", "1", "yes"]
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

    if "edited_df_v" in locals() and edited_df_v is not None:
        row_lookup = {(row["variant_id"], row["key"]): row["metafield_obj"] for row in variant_rows}
        type_lookup = {(row["variant_id"], row["key"]): row["type"] for row in variant_rows}

        grouped = edited_df_v.groupby("variant_id")
        for variant_id, rows in grouped:
            variant = variant_map[variant_id]
            keys_to_sync = []

            for _, row in rows.iterrows():
                key = row["key"]
                new_value = row["value"]
                sync_flag = row["sync"]
                original = row_lookup.get((variant_id, key))
                original_type = type_lookup.get((variant_id, key), "string")

                if sync_flag:
                    keys_to_sync.append(key)

                if original and str(original.value) != str(new_value):
                    try:
                        if original_type == "integer":
                            original.value = int(new_value)
                        elif original_type == "boolean":
                            original.value = new_value.lower() in ["true", "1", "yes"]
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
        sync_logs.append("### 🌐 Cross-Store Sync Results")
        for shop, result in results.items():
            sync_logs.append(f"**{shop}**")
            if "error" in result:
                sync_logs.append(f"❌ {result['error']}")
            else:
                for key, status in result.items():
                    sync_logs.append(f"{key}: {status}")

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
