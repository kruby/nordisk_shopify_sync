import streamlit as st
import shopify
import json
import time
from pyactiveresource.connection import ClientError

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

SUCCESS_ICON = "✅"
FAILURE_ICON = "❌"

def connect_to_store(shop_url, token):
    session = shopify.Session(f"https://{shop_url}", API_VERSION, token)
    shopify.ShopifyResource.activate_session(session)

def test_shop_access(label, shop_url, token):
    try:
        connect_to_store(shop_url, token)
        shop = shopify.Shop.current()
        return f"✅ {label}: Connected to {shop.name} ({shop.myshopify_domain})"
    except Exception as e:
        return f"❌ {label}: {str(e)}"

def get_variant_barcode(resource):
    for variant in resource.variants:
        if variant.barcode:
            return variant.barcode.strip()
    return None

def get_sync_keys(resource):
    for m in resource.metafields():
        if m.namespace == SYNC_NAMESPACE and m.key == SYNC_KEY:
            try:
                return json.loads(m.value)
            except:
                return []
    return []

def find_product_by_variant_barcode(barcode):
    page = shopify.Product.find(limit=250)
    while page:
        for product in page:
            for variant in product.variants:
                if variant.barcode and variant.barcode.strip() == barcode:
                    return product
        try:
            page = page.next_page()
        except Exception:
            break
    return None

def sync_product_fields(primary_product):
    product_barcode = get_variant_barcode(primary_product)
    if not product_barcode:
        st.warning("Primary product has no variant barcode set.")
        return

    sync_keys = get_sync_keys(primary_product)

    all_metafields = shopify.Metafield.find(
        resource_id=primary_product.id,
        resource_type="product"
    )
    primary_metafields = [m for m in all_metafields if m.key in sync_keys]

    st.write("🔍 Metafields to sync:")
    for m in primary_metafields:
        st.write(f"- {m.namespace}.{m.key} = {m.value} (type: {getattr(m, 'type', 'N/A')})")

    results = {}

    for store_url, token, label in [
        (STORE_B_URL, TOKEN_B, "Shop B"),
        (STORE_C_URL, TOKEN_C, "Shop C")
    ]:
        try:
            connect_to_store(store_url, token)

            target_product = find_product_by_variant_barcode(product_barcode)
            if not target_product or target_product.status != "active":
                results[label] = {"error": "Inactive or product not found via variant barcode"}
                continue

            target_metafields = shopify.Metafield.find(
                resource_id=target_product.id, resource_type="product"
            )

            field_results = {}

            for m in primary_metafields:
                try:
                    existing = [
                        mf for mf in target_metafields
                        if mf.key == m.key and mf.namespace == m.namespace
                    ]

                    if existing:
                        existing[0].value = m.value
                        existing[0].save()
                    else:
                        new_m = shopify.Metafield()
                        new_m.namespace = m.namespace
                        new_m.key = m.key
                        new_m.owner_id = target_product.id
                        new_m.owner_resource = "product"

                        # Infer type
                        value = m.value
                        if hasattr(m, "type") and m.type:
                            metafield_type = m.type
                        else:
                            # Auto-detect type based on value
                            try:
                                float_val = float(value)
                                metafield_type = "number_decimal"
                            except:
                                try:
                                    int_val = int(value)
                                    metafield_type = "number_integer"
                                except:
                                    if isinstance(value, dict):
                                        value = json.dumps(value)
                                        metafield_type = "json"
                                    elif isinstance(value, list):
                                        value = json.dumps(value)
                                        metafield_type = "json"
                                    else:
                                        metafield_type = "single_line_text_field"

                        new_m.type = metafield_type
                        new_m.value = str(value)

                        # Debug info
                        st.write(f"📝 Creating metafield: {new_m.namespace}.{new_m.key} = {new_m.value} (type: {new_m.type})")

                        # Save and report
                        success = new_m.save()
                        if not success:
                            error_messages = new_m.errors.full_messages()
                            st.error(f"❌ Failed to save {new_m.key}: {error_messages}")
                            field_results[m.key] = f"{FAILURE_ICON} ({error_messages})"
                            continue

                    field_results[m.key] = SUCCESS_ICON

                except Exception as e:
                    field_results[m.key] = f"{FAILURE_ICON} ({str(e)})"

            results[label] = field_results

        except Exception as e:
            results[label] = {"error": str(e)}

    return results




# ✅ Wrap Streamlit UI inside a callable function
def run_update_app():
    st.title("📱 Sync Product Fields to Other Stores")

    st.markdown("### 🔐 Shopify Store Access Check")
    st.write(test_shop_access("Store A", SHOP_URL, TOKEN))
    st.write(test_shop_access("Store B", STORE_B_URL, TOKEN_B))
    st.write(test_shop_access("Store C", STORE_C_URL, TOKEN_C))

    connect_to_store(SHOP_URL, TOKEN)

    product_id = st.text_input("Enter Product ID to Sync")
    if product_id:
        product = shopify.Product.find(product_id)
        if product:
            if st.button("🔄 Sync Product Fields"):
                sync_results = sync_product_fields(product)
                if sync_results:
                    st.subheader("Results")
                    for shop, result in sync_results.items():
                        st.markdown(f"**{shop}**")
                        if "error" in result:
                            st.error(result["error"])
                        else:
                            for key, status in result.items():
                                st.write(f"{key}: {status}")
        else:
            st.error("Product not found with given ID")
