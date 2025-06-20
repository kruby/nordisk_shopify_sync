import streamlit as st
import shopify
import pandas as pd
import json

# --- Shopify Setup ---
SHOP_URL = st.secrets["STORE_A_URL"]
TOKEN = st.secrets["TOKEN_A"]
API_VERSION = "2023-10"

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

def get_or_create_metafield(resource, namespace, key):
    for m in resource.metafields():
        if m.namespace == namespace and m.key == key:
            return m
    m = shopify.Metafield()
    m.namespace = namespace
    m.key = key
    m.value = ""
    m.type = "single_line_text_field"
    m.owner_resource = "product" if isinstance(resource, shopify.Product) else "variant"
    m.owner_id = resource.id
    return m

def get_sync_keys(resource):
    for m in resource.metafields():
        if m.namespace == "sync" and m.key == "fields":
            try:
                return json.loads(m.value)
            except:
                return []
    return []

def save_sync_keys(resource, keys):
    meta = next((m for m in resource.metafields() if m.namespace == "sync" and m.key == "fields"), None)
    if not meta:
        meta = shopify.Metafield()
        meta.namespace = "sync"
        meta.key = "fields"
        meta.owner_id = resource.id
        meta.owner_resource = "product" if isinstance(resource, shopify.Product) else "variant"
        meta.type = "json"
    meta.value = json.dumps(keys)
    return meta.save()

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

# --- Product Metafields ---
product_fields = []
sync_keys = get_sync_keys(selected_product)

for m in selected_product.metafields():
    product_fields.append({
        "key": m.key,
        "value": m.value,
        "type": m.type,
        "sync": m.key in sync_keys,
        "metafield_obj": m
    })

if product_fields:
    df = pd.DataFrame(product_fields).drop(columns=["metafield_obj"])
    edited_df = st.data_editor(df, num_rows="fixed", use_container_width=True, key="product_editor")

    if st.button("✅ Save Product Changes"):
        keys_to_sync = []
        for i, row in edited_df.iterrows():
            original = product_fields[i]["metafield_obj"]
            if str(original.value) != str(row["value"]):
                original.value = row["value"]
                original.type = row["type"]
                original.save()
            if row["sync"]:
                keys_to_sync.append(row["key"])
        if save_sync_keys(selected_product, keys_to_sync):
            st.success("✅ Product metafields and sync fields saved.")

# --- Variant Metafields ---
st.markdown("## 🔍 Variant Metafields")
variant_rows = []
variant_map = {}
for variant in selected_product.variants:
    variant_map[variant.id] = variant
    sync_keys = get_sync_keys(variant)
    for m in variant.metafields():
        variant_rows.append({
            "variant_id": variant.id,
            "variant_title": variant.title,
            "key": m.key,
            "value": m.value,
            "type": m.type,
            "sync": m.key in sync_keys,
            "metafield_obj": m
        })

if variant_rows:
    df_v = pd.DataFrame(variant_rows).drop(columns=["metafield_obj"])
    edited_df_v = st.data_editor(df_v, num_rows="fixed", use_container_width=True, key="variant_editor")

    if st.button("✅ Save Variant Changes"):
        success_count = 0
        grouped = edited_df_v.groupby("variant_id")
        for variant_id, rows in grouped:
            variant = variant_map[variant_id]
            keys_to_sync = []
            metafields = variant.metafields()
            for _, row in rows.iterrows():
                original = next((m for m in metafields if m.key == row["key"]), None)
                if original and str(original.value) != str(row["value"]):
                    original.value = row["value"]
                    original.type = row["type"]
                    if original.save():
                        success_count += 1
                if row["sync"]:
                    keys_to_sync.append(row["key"])
            save_sync_keys(variant, keys_to_sync)
        st.success(f"✅ Updated {success_count} variant metafields and sync settings.")
