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
                    return shopify.Product.find(product.id)
        try:
            page = page.next_page()
        except Exception:
            break
    return None


def find_variant_by_barcode_in_product(product, barcode):
    for variant in product.variants:
        if variant.barcode and variant.barcode.strip() == barcode:
            return variant
    return None


def normalize_type(metafield_type):
    if metafield_type == "integer":
        return "number_integer"
    elif metafield_type == "float":
        return "number_decimal"
    return metafield_type


def convert_value_for_type(value, metafield_type):
    if metafield_type == "integer":
        return int(value)
    elif metafield_type == "float":
        return float(value)
    elif metafield_type == "boolean":
        return True if str(value).lower() in ["true", "1", "yes"] else False
    elif metafield_type == "json":
        return json.loads(value) if isinstance(value, str) else value
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
        log_lines = []
        try:
            connect_to_store(store_url, token)
            log_lines.append(f"🔌 Connected to {label}")

            target_product = find_product_by_variant_barcode(product_barcode)
            if not target_product or target_product.status != "active":
                log_lines.append("❌ Target product not found or inactive")
                results[label] = {"error": "Inactive or product not found via variant barcode"}
                continue

            field_results = {}
            for m in primary_metafields:
                try:
                    value = convert_value_for_type(m.value, m.type)
                    m_type = normalize_type(m.type)

                    existing = [
                        mf for mf in target_product.metafields()
                        if mf.key == m.key and mf.namespace == m.namespace
                    ]

                    if existing:
                        mf = existing[0]
                        log_lines.append(f"🔄 Updating product metafield '{m.key}' with value '{value}' and type '{m_type}'")
                        mf.value = value
                        mf.type = m_type
                        if m_type == "number_integer":
                            mf.value_type = "integer"
                        mf.save()
                    else:
                        log_lines.append(f"➕ Creating product metafield '{m.key}' with value '{value}' and type '{m_type}'")
                        new_m = shopify.Metafield()
                        new_m.namespace = m.namespace
                        new_m.key = m.key
                        new_m.value = value
                        new_m.type = m_type
                        new_m.owner_id = target_product.id
                        new_m.owner_resource = "product"
                        if m_type == "number_integer":
                            new_m.value_type = "integer"
                        new_m.save()
                    field_results[m.key] = SUCCESS_ICON
                except Exception as e:
                    log_lines.append(f"❌ Error syncing product metafield '{m.key}': {e}")
                    field_results[m.key] = f"❌ {str(e)}"

            for primary_variant in primary_product.variants:
                if not primary_variant.barcode:
                    continue
                sync_keys_variant = get_sync_keys(primary_variant)
                target_variant = find_variant_by_barcode_in_product(target_product, primary_variant.barcode.strip())
                if not target_variant:
                    log_lines.append(f"❌ No matching variant for barcode {primary_variant.barcode.strip()} in {label}")
                    field_results[f"{primary_variant.id}"] = "❌ No matching variant in target store"
                    continue
                for m in primary_variant.metafields():
                    if m.key not in sync_keys_variant:
                        continue
                    try:
                        value = convert_value_for_type(m.value, m.type)
                        m_type = normalize_type(m.type)
                        existing = [
                            mf for mf in target_variant.metafields()
                            if mf.key == m.key and mf.namespace == m.namespace
                        ]
                        if existing:
                            mf = existing[0]
                            log_lines.append(f"🔄 Updating variant {target_variant.id} metafield '{m.key}' with value '{value}' and type '{m_type}'")
                            mf.value = value
                            mf.type = m_type
                            if m_type == "number_integer":
                                mf.value_type = "integer"
                            mf.save()
                        else:
                            log_lines.append(f"➕ Creating variant {target_variant.id} metafield '{m.key}' with value '{value}' and type '{m_type}'")
                            new_m = shopify.Metafield()
                            new_m.namespace = m.namespace
                            new_m.key = m.key
                            new_m.value = value
                            new_m.type = m_type
                            new_m.owner_id = target_variant.id
                            new_m.owner_resource = "variant"
                            if m_type == "number_integer":
                                new_m.value_type = "integer"
                            new_m.save()
                        field_results[f"{target_variant.id}:{m.key}"] = SUCCESS_ICON
                    except Exception as e:
                        log_lines.append(f"❌ Error syncing variant metafield '{m.key}' for variant {target_variant.id}: {e}")
                        field_results[f"{target_variant.id}:{m.key}"] = f"❌ {str(e)}"

            log_lines.append("✅ Sync complete")
            results[label] = {"log": log_lines, **field_results}

        except Exception as e:
            log_lines.append(f"❌ {label} sync failed: {e}")
            results[label] = {"error": str(e), "log": log_lines}

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
                        if "log" in result:
                            with st.expander("📜 Logs"):
                                for line in result["log"]:
                                    st.write(line)
                        for key, status in result.items():
                            if key not in ["log", "error"]:
                                st.write(f"{key}: {status}")
        else:
            st.error("Product not found with given ID")
