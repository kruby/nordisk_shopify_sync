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

# --- Shopify Setup ---
SHOP_URL = st.secrets["STORE_A_URL"]
STORE_B_URL = st.secrets["STORE_B_URL"]
STORE_C_URL = st.secrets["STORE_C_URL"]
TOKEN = st.secrets["TOKEN_A"]
TOKEN_B = st.secrets["TOKEN_B"]
TOKEN_C = st.secrets["TOKEN_C"]
API_VERSION = "2024-07"

SYNC_NAMESPACE = "sync"
SYNC_KEY = "sync_fields"

def connect_to_store(shop_url, token):
    session = shopify.Session(f"https://{shop_url}", API_VERSION, token)
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
# --- Select which shop to view ---
store_options = {
    "Store A (source)": {"key": "A", "url": SHOP_URL, "token": TOKEN},
    "Store B": {"key": "B", "url": STORE_B_URL, "token": TOKEN_B},
    "Store C": {"key": "C", "url": STORE_C_URL, "token": TOKEN_C},
}
selected_store_label = st.selectbox("View metafields from:", list(store_options.keys()), index=0)
selected_store = store_options[selected_store_label]
connect_to_store(selected_store["url"], selected_store["token"])

# Load products
state_key = f"products_{selected_store['key']}"
if state_key not in st.session_state:
    with st.spinner(f"Loading products from {selected_store_label}..."):
        st.session_state[state_key] = get_all_products()

products = st.session_state[state_key]

# --- Persist selection from Store A and auto-match in B/C ---
if "selected_a_info" not in st.session_state:
    st.session_state.selected_a_info = {}

def get_product_barcodes(prod):
    codes = []
    try:
        for v in getattr(prod, "variants", []):
            bc = getattr(v, "barcode", None)
            if bc:
                codes.append(str(bc).strip())
    except Exception:
        pass
    return set([c for c in codes if c])

def find_matching_product_by_ean(all_products, barcode_set):
    if not barcode_set:
        return None
    for prod in all_products:
        prod_codes = get_product_barcodes(prod)
        if prod_codes.intersection(barcode_set):
            return prod
    return None

def find_matching_product_fallback(all_products, handle=None, title=None):
    # Try handle first
    if handle:
        for prod in all_products:
            if getattr(prod, "handle", None) == handle:
                return prod
    # Then loose title match
    if title:
        for prod in all_products:
            if getattr(prod, "title", "").strip().lower() == title.strip().lower():
                return prod
    return None
if not products:
    st.warning("No products found.")
    st.stop()

product_types = sorted(set(p.product_type for p in products if p.product_type))
# Determine default category/product based on Store A selection
default_type_index = 0
default_product_index = 0
matched_notice = None

a_info = st.session_state.get("selected_a_info")
if selected_store["key"] != "A" and a_info:
    # Prefer the same category
    try:
        if a_info.get("product_type") in product_types:
            default_type_index = product_types.index(a_info["product_type"])
    except Exception:
        pass

selected_type = st.selectbox("Select a Product Category", product_types, index=default_type_index)
filtered_products = [p for p in products if p.product_type == selected_type]

if selected_store["key"] != "A" and a_info:
    # Attempt to auto-select matching product by barcode, then handle, then title
    match = find_matching_product_by_ean(filtered_products, set(a_info.get("barcodes", [])))
    if not match:
        match = find_matching_product_fallback(filtered_products, a_info.get("handle"), a_info.get("title"))
    if match and match in filtered_products:
        default_product_index = filtered_products.index(match)
        matched_notice = "Showing the matching product from Store A."
    else:
        matched_notice = "Couldn't auto-match; choose the product manually."
else:
    matched_notice = None

selected_product = st.selectbox(
    "Select a Product",
    filtered_products,
    index=default_product_index if filtered_products else 0,
    format_func=lambda p: f"{p.title} (ID: {p.id})"
)
if matched_notice:
    st.caption(matched_notice)

show_only_sync = st.checkbox("🔁 Show only synced metafields", value=False)

# Persist current A selection
if selected_store["key"] == "A" and selected_product is not None:
    st.session_state.selected_a_info = {
        "id": getattr(selected_product, "id", None),
        "handle": getattr(selected_product, "handle", None),
        "title": getattr(selected_product, "title", None),
        "product_type": getattr(selected_product, "product_type", None),
        "barcodes": list(get_product_barcodes(selected_product))
    }

st.markdown("### 🌍 Side-by-side translation view (Store A vs B/C)")
# === Variant-by-Variant Comparison ===
st.markdown("### 🧩 Variant-by-variant comparison (Store A vs B/C)")
a_info = st.session_state.get("selected_a_info", {})
if not a_info:
    st.info("Pick a product in Store A to enable variant-level comparison.")
else:
    # Ensure we have reference product (Store A)
    if selected_store["key"] == "A":
        ref_product_v = selected_product
    else:
        connect_to_store(SHOP_URL, TOKEN)
        a_products_v = get_products_for_store("A", "Store A (source)")
        ref_product_v = find_matching_product_fallback(
            [p for p in a_products_v if getattr(p, "product_type", None) == a_info.get("product_type")],
            a_info.get("handle"), a_info.get("title")
        )
    if not ref_product_v:
        st.warning("Could not locate the Store A product for variant comparison.")
    else:
        # Collect candidate keys from A variants: prefer sync list if present, else union of keys
        def variant_sync_keys(v):
            try:
                return set(get_sync_keys(v))
            except Exception:
                return set()
        def variant_all_keys(v):
            keys = set()
            try:
                for m in v.metafields():
                    keys.add(m.key)
            except Exception:
                pass
            return keys
        a_variants = list(getattr(ref_product_v, "variants", []))
        sync_union = set()
        key_union = set()
        for v in a_variants:
            sync_union |= variant_sync_keys(v)
            key_union |= variant_all_keys(v)
        candidate_v_keys = sorted(sync_union or key_union)
        if not candidate_v_keys:
            st.info("No variant metafields found on Store A product.")
        else:
            selected_v_keys = st.multiselect("Variant metafields to compare (keys)", candidate_v_keys, default=candidate_v_keys[: min(10, len(candidate_v_keys))])

            # Find product matches in B/C
            match_b_v = find_match_in_store("B", "Store B", a_info)
            match_c_v = find_match_in_store("C", "Store C", a_info)

            # Build lookup for target variants by barcode, then by SKU, then by position index
            def build_variant_maps(prod):
                maps = {"barcode": {}, "sku": {}, "by_index": []}
                if not prod:
                    return maps
                try:
                    for idx, v in enumerate(prod.variants):
                        bc = (getattr(v, "barcode", None) or "").strip()
                        sku = (getattr(v, "sku", None) or "").strip()
                        if bc:
                            maps["barcode"][bc] = v
                        if sku:
                            maps["sku"][sku] = v
                        maps["by_index"].append(v)
                except Exception:
                    pass
                return maps

            b_maps = build_variant_maps(match_b_v)
            c_maps = build_variant_maps(match_c_v)

            def id_text(v):
                try:
                    opts = [getattr(v, f"option{i}", None) for i in [1,2,3]]
                    opts = [o for o in opts if o]
                except Exception:
                    opts = []
                bc = getattr(v, "barcode", "") or ""
                sku = getattr(v, "sku", "") or ""
                parts = ["/".join(opts) if opts else f"Variant {getattr(v, 'id', '')}", f"EAN:{bc}" if bc else None, f"SKU:{sku}" if sku else None]
                return " · ".join([p for p in parts if p])

            def metafield_value(resource, key):
                try:
                    for m in resource.metafields():
                        if m.key == key:
                            return m.value
                except Exception:
                    pass
                return None

            import pandas as _pd
            # For each selected key, show a table with A vs B vs C
            for k in selected_v_keys:
                rows = []
                meta_refs = []
                for idx, av in enumerate(a_variants):
                    # Match candidate in B and C
                    bv = None
                    cv = None
                    a_bc = (getattr(av, "barcode", None) or "").strip()
                    a_sku = (getattr(av, "sku", None) or "").strip()
                    if a_bc and a_bc in b_maps["barcode"]:
                        bv = b_maps["barcode"][a_bc]
                    elif a_sku and a_sku in b_maps["sku"]:
                        bv = b_maps["sku"][a_sku]
                    elif idx < len(b_maps["by_index"]):
                        bv = b_maps["by_index"][idx]

                    if a_bc and a_bc in c_maps["barcode"]:
                        cv = c_maps["barcode"][a_bc]
                    elif a_sku and a_sku in c_maps["sku"]:
                        cv = c_maps["sku"][a_sku]
                    elif idx < len(c_maps["by_index"]):
                        cv = c_maps["by_index"][idx]

                    rows.append({
                        "variant": id_text(av),
                        "Store A": "" if metafield_value(av, k) is None else str(metafield_value(av, k)),
                        "Store B": "" if (bv is None or metafield_value(bv, k) is None) else str(metafield_value(bv, k)),
                        "Store C": "" if (cv is None or metafield_value(cv, k) is None) else str(metafield_value(cv, k)),
                        "ΔB": "≠" if ((bv is None and metafield_value(av, k)) or (metafield_value(av, k) != metafield_value(bv, k) if bv else False)) else "",
                        "ΔC": "≠" if ((cv is None and metafield_value(av, k)) or (metafield_value(av, k) != metafield_value(cv, k) if cv else False)) else "" ,
                    })
                df_k = _pd.DataFrame(rows)
                st.markdown(f"**Variant metafield: `{k}`**")
                # Keep originals and refs for Save All
                st.session_state[f"original_variant_df_{k}"] = df_k.copy()
                st.session_state[f"meta_refs_{k}"] = meta_refs
                edited_k = st.data_editor(
                    df_k,
                    use_container_width=True,
                    disabled=["variant", "Store A (read-only)", "ΔB", "ΔC"],
                    key=f"variant_editor_{k}"
                )
                # Cache current edits for Save All
                st.session_state[f"edited_variant_df_{k}"] = edited_k
                if st.button(f"💾 Save changes for `{k}`"):
                    # helper to cast types
                    def cast_value(t, v):
                        try:
                            if v is None:
                                return None
                            s = str(v)
                            if t == "integer":
                                return int(s)
                            elif t == "boolean":
                                return s.lower() in ["true", "1", "yes"]
                            elif t == "json":
                                import json as _json
                                return _json.loads(s)
                            elif t in ["float", "decimal"]:
                                return float(s)
                            else:
                                return s
                        except Exception:
                            return v
                    save_logs = []
                    # derive A namespace/type for this key from first A variant that has it
                    a_ns = "global"
                    a_type = "string"
                    for rr in meta_refs:
                        try:
                            for m in rr["a_variant"].metafields():
                                if m.key == k:
                                    a_ns = getattr(m, "namespace", a_ns)
                                    a_type = getattr(m, "type", a_type)
                                    raise StopIteration
                        except StopIteration:
                            break
                        except Exception:
                            pass
                    # compare and push
                    for i, row in edited_k.iterrows():
                        refs = meta_refs[i]
                        bv = refs["b_variant"]
                        cv = refs["c_variant"]
                        original_b = df_k.loc[i, "Store B"] if "Store B" in df_k.columns else ""
                        original_c = df_k.loc[i, "Store C"] if "Store C" in df_k.columns else ""
                        new_b = row.get("Store B", "")
                        new_c = row.get("Store C", "")
                        def upsert_variant_mf(variant, store_label, new_val, old_val):
                            if variant is None:
                                return f"⚠️ {store_label}: No matching variant"
                            if str(new_val) == str(old_val):
                                return f"{store_label}: No change"
                            try:
                                existing = []
                                try:
                                    for m in variant.metafields():
                                        if m.key == k and (m.namespace == a_ns):
                                            existing.append(m)
                                except Exception:
                                    existing = []
                                if existing:
                                    m = existing[0]
                                    m.value = cast_value(getattr(m, "type", a_type), new_val)
                                    m.save()
                                    return f"{store_label}: ✅ updated"
                                else:
                                    m = shopify.Metafield()
                                    m.namespace = a_ns
                                    m.key = k
                                    m.value = cast_value(a_type, new_val)
                                    m.type = a_type
                                    m.owner_id = variant.id
                                    m.owner_resource = "variant"
                                    m.save()
                                    return f"{store_label}: ✅ created"
                            except ClientError as e:
                                return f"{store_label}: ❌ {e}"
                            except Exception as e:
                                return f"{store_label}: ❌ {e}"
                        if new_b != original_b:
                            save_logs.append(upsert_variant_mf(bv, "Store B", new_b, original_b))
                        if new_c != original_c:
                            save_logs.append(upsert_variant_mf(cv, "Store C", new_c, original_c))
                    if save_logs:
                        st.markdown("**Save results:**")
                        for msg in save_logs:
                            st.write(msg)

a_info = st.session_state.get("selected_a_info", {})
ref_product = None
ref_sync_keys = []
if a_info:
    # Ensure we have Store A product for reference
    if selected_store["key"] == "A":
        ref_product = selected_product
    else:
        # Switch session to Store A to read fields accurately
        connect_to_store(SHOP_URL, TOKEN)
        a_products = get_products_for_store("A", "Store A (source)")
        ref_product = find_matching_product_fallback(
            [p for p in a_products if getattr(p, "product_type", None) == a_info.get("product_type")],
            a_info.get("handle"), a_info.get("title")
        )
    if ref_product is not None:
        try:
            ref_sync_keys = get_sync_keys(ref_product)
        except Exception:
            ref_sync_keys = []

if ref_product is not None:
    # Build key options from reference product metafields
    ref_fields = {m.key: str(m.value) for m in ref_product.metafields()}
    candidate_keys = sorted(set(ref_sync_keys or list(ref_fields.keys())))
    selected_keys = st.multiselect("Metafields to compare (keys)", candidate_keys, default=candidate_keys[: min(12, len(candidate_keys))])

    # Find matches in B/C
    match_b = find_match_in_store("B", "Store B", a_info) if a_info else None
    match_c = find_match_in_store("C", "Store C", a_info) if a_info else None

    def mf_dict(resource):
        d = {}
        try:
            for m in resource.metafields():
                d[m.key] = m.value
        except Exception:
            pass
        return d

    a_vals = mf_dict(ref_product)
    b_vals = mf_dict(match_b) if match_b else {}
    c_vals = mf_dict(match_c) if match_c else {}

    import pandas as _pd
    rows = []
    for k in selected_keys:
        rows.append({
            "key": k,
            "Store A": "" if a_vals.get(k) is None else str(a_vals.get(k)),
            "Store B": "" if b_vals.get(k) is None else str(b_vals.get(k)),
            "Store C": "" if c_vals.get(k) is None else str(c_vals.get(k)),
        })
    df_compare = _pd.DataFrame(rows)
    st.dataframe(df_compare, use_container_width=True)
else:
    st.info("Pick a product in Store A to enable side-by-side comparison here.")

# --- Logging containers ---
product_save_logs = []
variant_save_logs = []
sync_logs = []

# --- Save & Sync Buttons ---
st.markdown("### 💾 Save & Synchronize Metafields")
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    save_clicked = st.button("✅ Save All Changes")
with col2:
    apply_sync_clicked = st.button("📦 Apply Sync Settings to All in Category")
with col3:
    if selected_store["key"] == "A":
        cross_sync_clicked = st.button("📡 Sync This Product to Shop B & C (via EAN)")
    else:
        st.caption("Cross-store sync is available when viewing Store A.")
        cross_sync_clicked = False

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
    st.markdown("### 🔍 Product Metafields")
    df = pd.DataFrame(product_fields).drop(columns=["metafield_obj"])
    edited_df = st.data_editor(df, num_rows="fixed", use_container_width=True, key="product_editor")

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
    st.markdown("### 🔍 Variant Metafields")
    df_v = pd.DataFrame(variant_rows).drop(columns=["metafield_obj"])
    edited_df_v = st.data_editor(df_v, num_rows="fixed", use_container_width=True, key="variant_editor")

# --- Save Logic ---
if save_clicked:
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

# --- Apply Sync ---
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

# --- Cross-store Sync ---
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

            # --- Save ALL selected keys ---
            if st.button("💾 Save ALL changes for selected variant metafield keys"):
                aggregate_logs = []
                def cast_value(t, v):
                    try:
                        if v is None:
                            return None
                        s = str(v)
                        if t == "integer":
                            return int(s)
                        elif t == "boolean":
                            return s.lower() in ["true", "1", "yes"]
                        elif t == "json":
                            import json as _json
                            return _json.loads(s)
                        elif t in ["float", "decimal"]:
                            return float(s)
                        else:
                            return s
                    except Exception:
                        return v

                for k in selected_v_keys:
                    edited_df = st.session_state.get(f"edited_variant_df_{k}")
                    original_df = st.session_state.get(f"original_variant_df_{k}")
                    meta_refs = st.session_state.get(f"meta_refs_{k}")
                    if edited_df is None or original_df is None or meta_refs is None:
                        continue
                    # derive A namespace/type for this key
                    a_ns = "global"
                    a_type = "string"
                    for rr in meta_refs:
                        try:
                            for m in rr["a_variant"].metafields():
                                if m.key == k:
                                    a_ns = getattr(m, "namespace", a_ns)
                                    a_type = getattr(m, "type", a_type)
                                    raise StopIteration
                        except StopIteration:
                            break
                        except Exception:
                            pass
                    # Iterate rows and push changes
                    for i in range(len(edited_df)):
                        row_e = edited_df.iloc[i]
                        row_o = original_df.iloc[i]
                        refs = meta_refs[i]
                        bv = refs["b_variant"]
                        cv = refs["c_variant"]
                        def upsert_variant_mf(variant, store_label, new_val, old_val):
                            if variant is None:
                                return f"⚠️ {store_label}: No matching variant"
                            if str(new_val) == str(old_val):
                                return f"{store_label}: No change"
                            try:
                                existing = []
                                try:
                                    for m in variant.metafields():
                                        if m.key == k and (m.namespace == a_ns):
                                            existing.append(m)
                                except Exception:
                                    existing = []
                                if existing:
                                    m = existing[0]
                                    m.value = cast_value(getattr(m, "type", a_type), row_e.get(store_label, ""))
                                    m.save()
                                    return f"{store_label}: ✅ updated"
                                else:
                                    m = shopify.Metafield()
                                    m.namespace = a_ns
                                    m.key = k
                                    m.value = cast_value(a_type, row_e.get(store_label, ""))
                                    m.type = a_type
                                    m.owner_id = variant.id
                                    m.owner_resource = "variant"
                                    m.save()
                                    return f"{store_label}: ✅ created"
                            except ClientError as e:
                                return f"{store_label}: ❌ {e}"
                            except Exception as e:
                                return f"{store_label}: ❌ {e}"

                        # Push B
                        nb, ob = row_e.get("Store B", ""), row_o.get("Store B", "")
                        if nb != ob:
                            aggregate_logs.append(upsert_variant_mf(bv, "Store B", nb, ob))
                        # Push C
                        nc, oc = row_e.get("Store C", ""), row_o.get("Store C", "")
                        if nc != oc:
                            aggregate_logs.append(upsert_variant_mf(cv, "Store C", nc, oc))

                if aggregate_logs:
                    st.markdown("**Save-all results:**")
                    for msg in aggregate_logs:
                        st.write(msg)
