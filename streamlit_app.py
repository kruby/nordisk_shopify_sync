import streamlit as st
import shopify
import pandas as pd
import json
import time

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

for m in selected_product.metafields():
    if not show_only_sync or m.key in sync_keys:
        product_fields.append({
            "key": m.key,
            "value": m.value,
            "product_id": selected_product.id,
            "sync": m.key in sync_keys,
            "metafield_obj": m
        })

edited_df = None
if product_fields:
    st.markdown("### 🔍 Product Metafields")
    st.caption(f"Currently synced fields: {', '.join(sync_keys) if sync_keys else 'None'}")
    df = pd.DataFrame(product_fields).drop(columns=["metafield_obj"])
    edited_df = st.data_editor(df, num_rows="fixed", use_container_width=True, key="product_editor")

# --- Variant Metafields ---
st.markdown("## 🔍 Variant Metafields")
variant_rows = []
variant_map = {}
variant_sync_map = {}

for variant in selected_product.variants:
    time.sleep(0.6)  # throttle reads too
    try:
        variant_map[variant.id] = variant
        variant_sync_keys = get_sync_keys(variant)
        variant_sync_map[variant.id] = variant_sync_keys
        for m in variant.metafields():
            if not show_only_sync or m.key in variant_sync_keys:
                variant_rows.append({
                    "product_id": selected_product.id,
                    "variant_id": variant.id,
                    "variant_title": variant.title,
                    "key": m.key,
                    "value": m.value,
                    "sync": m.key in variant_sync_keys,
                    "metafield_obj": m
                })
    except shopify.ShopifyResource.ClientError as e:
        if '429' in str(e):
            st.warning(f"Rate limit hit while fetching variant {variant.id} — retrying...")
            time.sleep(2)
            for m in variant.metafields():
                if not show_only_sync or m.key in variant_sync_keys:
                    variant_rows.append({
                        "product_id": selected_product.id,
                        "variant_id": variant.id,
                        "variant_title": variant.title,
                        "key": m.key,
                        "value": m.value,
                        "sync": m.key in variant_sync_keys,
                        "metafield_obj": m
                    })

edited_df_v = None
if variant_rows:
    st.caption("Currently synced variant fields: (click checkbox to toggle)")
    df_v = pd.DataFrame(variant_rows).drop(columns=["metafield_obj"])
    edited_df_v = st.data_editor(df_v, num_rows="fixed", use_container_width=True, key="variant_editor")

# --- Unified Save Button ---
if st.button("✅ Save All Changes"):
    success_count = 0
    # --- Save product metafields ---
    if edited_df is not None:
        updated_sync_keys = []
        for i, row in edited_df.iterrows():
            original = product_fields[i]["metafield_obj"]
            if str(original.value) != str(row["value"]):
                original.value = row["value"]
                try:
                    original.save()
                    time.sleep(0.6)
                except shopify.ShopifyResource.ClientError as e:
                    if '429' in str(e):
                        st.warning("Rate limit hit — retrying...")
                        time.sleep(2)
                        original.save()
            if row["sync"]:
                updated_sync_keys.append(row["key"])
        if save_sync_keys(selected_product, updated_sync_keys):
            st.success("✅ Product metafields and sync fields saved.")

    # --- Save variant metafields ---
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
                    except shopify.ShopifyResource.ClientError as e:
                        if '429' in str(e):
                            st.warning("Rate limited — retrying after short delay...")
                            time.sleep(2)
                            original.save()
                if row["sync"]:
                    keys_to_sync.append(key)
            save_sync_keys(variant, keys_to_sync)

        st.success(f"✅ Updated {success_count} variant metafields and sync settings.")
