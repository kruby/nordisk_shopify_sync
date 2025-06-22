import streamlit as st
import shopify
import pandas as pd
import json
import time
from pyactiveresource.connection import ClientError
from update_app import run_update_app
from update_app import sync_product_fields



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

# --- Streamlit App ---
st.title("🔧 Shopify Product + Variant Metafield Sync Tool")
connect_to_store()

# Load all products
if "products" not in st.session_state:
    with st.spinner("Loading products..."):
        st.session_state.products = get_all_products()

products = st.session_state.products
if not products:
    st.warning("No products found.")
    st.stop()

# Select category
product_types = sorted(set(p.product_type for p in products if p.product_type))
selected_type = st.selectbox("Select a Product Category", product_types)
filtered_products = [p for p in products if p.product_type == selected_type]

# Select product
selected_product = st.selectbox(
    "Select a Product",
    filtered_products,
    format_func=lambda p: f"{p.title} (ID: {p.id})"
)

# Toggle to show only sync fields
show_only_sync = st.checkbox("🔁 Show only synced metafields", value=False)

# --- Product Metafields ---
product_fields = []
sync_keys = get_sync_keys(selected_product)
existing_fields = {m.key: m for m in selected_product.metafields()}

for key, m in existing_fields.items():
    if show_only_sync and key not in sync_keys:
        continue
    product_fields.append({
        "key": key,
        "value": m.value,
        "product_id": selected_product.id,
        "sync": key in sync_keys,
        "metafield_obj": m
    })

edited_df = None
if product_fields:
    st.markdown("### 🔍 Product Metafields")
    st.caption(f"Currently synced product fields: {', '.join(sync_keys) if sync_keys else 'None'}")
    df = pd.DataFrame(product_fields).drop(columns=["metafield_obj"])
    edited_df = st.data_editor(df, num_rows="fixed", use_container_width=True, key="product_editor")

# --- Variant Metafields ---
st.markdown("## 🔍 Variant Metafields")
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
                "variant_id": variant.id,
                "variant_title": variant.title,
                "key": key,
                "value": m.value,
                "sync": key in variant_sync_keys,
                "metafield_obj": m
            })
    except ClientError as e:
        if '429' in str(e):
            st.warning(f"Rate limit hit while fetching variant {variant.id} — retrying...")
            time.sleep(2)

edited_df_v = None
if variant_rows:
    variant_sync_keys_combined = set()
    for keys in variant_sync_map.values():
        variant_sync_keys_combined.update(keys)
    st.caption(f"Currently synced variant fields: {', '.join(sorted(variant_sync_keys_combined)) if variant_sync_keys_combined else 'None'}")
    df_v = pd.DataFrame(variant_rows).drop(columns=["metafield_obj"])
    edited_df_v = st.data_editor(df_v, num_rows="fixed", use_container_width=True, key="variant_editor")

# --- Unified Save Button ---
updated_product_sync_keys = []
updated_variant_sync_keys_combined = set()

if st.button("✅ Save All Changes"):
    success_count = 0

    if edited_df is not None:
        for i, row in edited_df.iterrows():
            original = product_fields[i]["metafield_obj"]
            if original and str(original.value) != str(row["value"]):
                original.value = row["value"]
                try:
                    original.save()
                    time.sleep(0.6)
                except ClientError as e:
                    if '429' in str(e):
                        st.warning("Rate limit hit — retrying...")
                        time.sleep(2)
                        original.save()
            if row["sync"]:
                updated_product_sync_keys.append(row["key"])
        if save_sync_keys(selected_product, updated_product_sync_keys):
            st.success("✅ Product metafields and sync fields saved.")

    if edited_df_v is not None:
        row_lookup = {
            (row["variant_id"], row["key"]): row["metafield_obj"]
            for row in variant_rows
        }
        grouped = edited_df_v.groupby("variant_id")
        for variant_id, rows in grouped:
            variant = variant_map[variant_id]
            keys_to_sync = []
            for _, row in rows.iterrows():
                key = row["key"]
                original = row_lookup.get((variant_id, key))
                if original and str(original.value) != str(row["value"]):
                    try:
                        original.value = row["value"]
                        if original.save():
                            success_count += 1
                            time.sleep(0.6)
                    except ClientError as e:
                        if '429' in str(e):
                            st.warning("Rate limited — retrying after short delay...")
                            time.sleep(2)
                            original.save()
                if row["sync"]:
                    keys_to_sync.append(key)
            save_sync_keys(variant, keys_to_sync)
            updated_variant_sync_keys_combined.update(keys_to_sync)

        st.success(f"✅ Updated {success_count} variant metafields and sync settings.")

# --- Apply Sync to Category Button ---
if st.button("📦 Apply Sync Settings to All in Category"):
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
    st.success("✅ Sync settings applied to all products and variants in this category.")

st.markdown("---")
st.markdown("## 🌍 Cross-Store Sync")

if st.button("📡 Sync This Product to Shop B & C (via EAN)"):
    results = sync_product_fields(selected_product)
    if results:
        st.subheader("Cross-Store Sync Results")
        for shop, result in results.items():
            st.markdown(f"**{shop}**")
            if "error" in result:
                st.error(result["error"])
            else:
                for key, status in result.items():
                    st.write(f"{key}: {status}")


