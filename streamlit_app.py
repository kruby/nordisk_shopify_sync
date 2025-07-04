import streamlit as st
import shopify
import pandas as pd
import json
import time
from pyactiveresource.connection import ClientError
from update_app import run_update_app, sync_product_fields

# --- Shopify Setup ---
SHOP_URL = st.secrets["STORE_A_URL"]
TOKEN = st.secrets["TOKEN_A"]
API_VERSION = "2023-10"

SYNC_NAMESPACE = "sync"
SYNC_KEY = "sync_fields"

def connect_to_store():
    session = shopify.Session(f"https://{SHOP_URL}", API_VERSION, TOKEN)
    shopify.ShopifyResource.activate_session(session)

def get_all_products():
    all_products, page = [], shopify.Product.find(limit=250)
    while page:
        all_products.extend(page)
        try:
            page = page.next_page()
        except:
            break
    return all_products

def get_sync_keys(resource):
    for m in resource.metafields():
        if m.namespace == SYNC_NAMESPACE and m.key == SYNC_KEY:
            try: return json.loads(m.value)
            except: return []
    return []

def save_sync_keys(resource, keys):
    meta = next((m for m in resource.metafields()
                 if m.namespace==SYNC_NAMESPACE and m.key==SYNC_KEY), None)
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
                for v in p.variants:
                    save_sync_keys(v, variant_sync_keys)
                    time.sleep(0.6)
            except Exception:
                time.sleep(2)
                save_sync_keys(p, product_sync_keys)

# --- Streamlit App ---
st.title("🔧 Shopify Product + Variant Metafield Sync Tool")
connect_to_store()

# Load products once
if "products" not in st.session_state:
    with st.spinner("Loading products..."):
        st.session_state.products = get_all_products()
products = st.session_state.products
if not products:
    st.warning("No products found."); st.stop()

# Category & product selection
product_types = sorted({p.product_type for p in products if p.product_type})
selected_type = st.selectbox("Select a Product Category", product_types)
filtered_products = [p for p in products if p.product_type == selected_type]

selected_product = st.selectbox(
    "Select a Product",
    filtered_products,
    format_func=lambda p: f"{p.title} (ID: {p.id})"
)

show_only_sync = st.checkbox("🔁 Show only synced metafields", value=False)

# Prepare data structures for editor
product_fields, variant_rows = [], []
variant_map, variant_sync_map = {}, {}

# Build product_fields
sync_keys = get_sync_keys(selected_product)
for m in selected_product.metafields():
    key = m.key
    if show_only_sync and key not in sync_keys:
        continue
    product_fields.append({
        "key": key,
        "value": str(m.value) if m.value is not None else "",
        "type": getattr(m, "type", "string"),
        "product_id": selected_product.id,
        "sync": key in sync_keys,
        "metafield_obj": m
    })

# Build variant_rows
for variant in selected_product.variants:
    time.sleep(0.6)
    try:
        variant_map[variant.id] = variant
        v_sync = get_sync_keys(variant)
        variant_sync_map[variant.id] = v_sync
        for m in variant.metafields():
            key = m.key
            if show_only_sync and key not in v_sync:
                continue
            variant_rows.append({
                "variant_id": variant.id,
                "variant_title": variant.title,
                "key": key,
                "value": str(m.value) if m.value is not None else "",
                "type": getattr(m, "type", "string"),
                "sync": key in v_sync,
                "metafield_obj": m
            })
    except ClientError as e:
        if '429' in str(e):
            st.warning(f"Rate limit hit while fetching variant {variant.id} — retrying...")
            time.sleep(2)

# --- Save / Sync Buttons Moved Up ---
st.markdown("### 💾 Save Changes & Sync")
col1, col2, col3 = st.columns([1,1,2])
with col1:
    save_clicked = st.button("✅ Save All Changes")
with col2:
    apply_category_clicked = st.button("📦 Apply Sync Settings to All in Category")
with col3:
    cross_store_clicked = st.button("📡 Sync This Product to Shop B & C (via EAN)")

# --- Actions for Save / Sync Buttons ---
if save_clicked:
    # Product-level save
    success_p = 0
    updated_product_keys = []
    lookup_p = {row["key"]: row["metafield_obj"] for row in product_fields}
    type_p   = {row["key"]: row["type"] for row in product_fields}

    for row in st.session_state.product_editor.itertuples():
        key, new_val = row.key, row.value
        orig = lookup_p.get(key)
        t = type_p.get(key, "string")
        if not orig or str(orig.value)==str(new_val):
            if orig and row.sync: updated_product_keys.append(key)
            continue

        try:
            # cast back
            if t=="integer":   orig.value = int(new_val)
            elif t=="boolean": orig.value = new_val.lower() in ["true","1","yes"]
            elif t=="json":    orig.value = json.loads(new_val)
            elif t in ["float","decimal"]: orig.value = float(new_val)
            else:              orig.value = new_val

            if orig.save(): success_p+=1
            else: st.error(f"❌ Save failed for product '{key}'")
        except Exception as e:
            st.error(f"❌ Error saving product '{key}': {e}")
            continue

        if row.sync: updated_product_keys.append(key)

    if save_sync_keys(selected_product, updated_product_keys):
        st.success(f"✅ Updated {success_p} product metafields")

    # Variant-level save
    success_v = 0
    updated_variant_keys = set()
    lookup_v = {(r["variant_id"],r["key"]): r["metafield_obj"] for r in variant_rows}
    type_v   = {(r["variant_id"],r["key"]): r["type"] for r in variant_rows}

    for row in st.session_state.variant_editor.itertuples():
        vid, key, new_val = row.variant_id, row.key, row.value
        orig = lookup_v.get((vid,key)); t = type_v.get((vid,key),"string")
        if not orig or str(orig.value)==str(new_val):
            if orig and row.sync: updated_variant_keys.add(key)
            continue

        try:
            if t=="integer":   orig.value = int(new_val)
            elif t=="boolean": orig.value = new_val.lower() in ["true","1","yes"]
            elif t=="json":    orig.value = json.loads(new_val)
            elif t in ["float","decimal"]: orig.value = float(new_val)
            else:              orig.value = new_val

            if orig.save(): success_v+=1
            else: st.error(f"❌ Save failed for variant {vid} '{key}'")
        except Exception as e:
            st.error(f"❌ Error saving variant {vid} '{key}': {e}")
            continue

        if row.sync: updated_variant_keys.add(key)

    for v in selected_product.variants:
        save_sync_keys(v, list(updated_variant_keys))
    st.success(f"✅ Updated {success_v} variant metafields")

if apply_category_clicked:
    prod_keys = get_sync_keys(selected_product)
    var_keys = set().union(*variant_sync_map.values())
    apply_sync_keys_to_category(products, selected_product.product_type, prod_keys, list(var_keys))
    st.success("✅ Sync settings applied to category")

if cross_store_clicked:
    results = sync_product_fields(selected_product)
    if results:
        st.subheader("Cross-Store Sync Results")
        for shop, res in results.items():
            st.markdown(f"**{shop}**")
            if "error" in res: st.error(res["error"])
            else:
                for k, status in res.items(): st.write(f"{k}: {status}")

# --- Collapsible Editor Section ---
with st.expander("🧪 Advanced Metafield Editor (click to expand)", expanded=False):
    if product_fields:
        st.markdown("### 🔍 Product Metafields")
        st.caption(f"Synced: {', '.join(sync_keys) if sync_keys else 'None'}")
        df = pd.DataFrame(product_fields).drop(columns=["metafield_obj"])
        st.data_editor(df, use_container_width=True, key="product_editor", num_rows="fixed")

    if variant_rows:
        st.markdown("### 🔍 Variant Metafields")
        combined = set().union(*variant_sync_map.values())
        st.caption(f"Synced: {', '.join(sorted(combined)) if combined else 'None'}")
        df_v = pd.DataFrame(variant_rows).drop(columns=["metafield_obj"])
        st.data_editor(df_v, use_container_width=True, key="variant_editor", num_rows="fixed")

# --- Divider & Cross-Store App Link ---
st.markdown("---")
st.markdown("## 🌍 Cross-Store Sync (alternative UI)")
run_update_app()
