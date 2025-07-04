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
API_VERSION = "2023-10"

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

def coerce_value_by_type(value, metafield_type):
    try:
        if metafield_type == "integer":
            return str(int(value))
        elif metafield_type == "boolean":
            return "true" if str(value).lower() in ["true", "1", "yes"] else "false"
        elif metafield_type == "json":
            return json.dumps(json.loads(value)) if isinstance(value, str) else json.dumps(value)
        elif metafield_type in ["float", "decimal"]:
            return str(float(value))
        else:
            return str(value)
    except:
        return str(value)

def sync_product_fields(primary_product):
    product_barcode = get_variant_barcode(primary_product)
    if not product_barcode:
        st.warning("Primary product has no variant barcode set.")
        return

    sync_keys = get_sync_keys(primary_product)
    primary_metafields = [
        m for m in primary_product.metafields() if m.key in sync_keys
    ]

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

            field_results = {}
            for m in primary_metafields:
                try:
                    value = coerce_value_by_type(m.value, m.type)
                    existing = [
                        mf for mf in target_product.metafields()
                        if mf.key == m.key and mf.namespace == m.namespace
                    ]
                    if existing:
                        mf = existing[0]
                        mf.value = value
                        mf.type = m.type
                        mf.save()
                    else:
                        new_m = shopify.Metafield()
                        new_m.namespace = m.namespace
                        new_m.key = m.key
                        new_m.value = value
                        new_m.type = m.type
                        new_m.owner_id = target_product.id
                        new_m.owner_resource = "product"
                        new_m.save()
                    field_results[m.key] = SUCCESS_ICON
                except Exception as e:
                    field_results[m.key] = f"❌ {str(e)}"

            results[label] = field_results
        except Exception as e:
            results[label] = {"error": str(e)}

    return results

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
