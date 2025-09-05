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

# ---------------------------
# Shopify API helpers
# ---------------------------

def shopify_get(url, token):
    """GET request with retry on 429 (rate limit)."""
    headers = {"X-Shopify-Access-Token": token}
    while True:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            time.sleep(2)
            continue
        resp.raise_for_status()
        return resp.json()

def shopify_post(url, token, payload):
    """POST request with retry on 429 (rate limit)."""
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    while True:
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 429:
            time.sleep(2)
            continue
        if resp.status_code in (200, 201):
            return resp.json()
        else:
            resp.raise_for_status()

def shopify_put(url, token, payload):
    """PUT request with retry on 429 (rate limit)."""
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    while True:
        resp = requests.put(url, headers=headers, json=payload)
        if resp.status_code == 429:
            time.sleep(2)
            continue
        if resp.status_code in (200, 201):
            return resp.json()
        else:
            resp.raise_for_status()

# ---------------------------
# Logic
# ---------------------------

def norm_title(s: str) -> str:
    """Normalize product title for grouping."""
    return (s or "").strip().lower()

def get_all_products(shop, token):
    products = []
    page_info = None
    base = f"https://{shop}/admin/api/2025-01/products.json?limit=250"
    while True:
        url = base if not page_info else f"{base}&page_info={page_info}"
        resp = shopify_get(url, token)
        products.extend(resp.get("products", []))
        if "link" not in resp:
            break
        # TODO: handle pagination via Link headers if needed
        break
    return products

def get_metafields(shop, token, product_id):
    url = f"https://{shop}/admin/api/2025-01/products/{product_id}/metafields.json"
    resp = shopify_get(url, token)
    return resp.get("metafields", [])

def upsert_metafield(shop, token, product_id, mf):
    # Try update if id exists, else create
    if "id" in mf:
        url = f"https://{shop}/admin/api/2025-01/metafields/{mf['id']}.json"
        return shopify_put(url, token, {"metafield": mf})
    else:
        url = f"https://{shop}/admin/api/2025-01/products/{product_id}/metafields.json"
        return shopify_post(url, token, {"metafield": mf})

# ---------------------------
# Streamlit UI
# ---------------------------

def run():
    st.write("This tool copies product-level metafields to all products with the same title (within Shop A).")

    shop = st.secrets["SHOP_A_DOMAIN"]
    token = st.secrets["SHOP_A_TOKEN"]

    allowed_namespaces = st.text_area(
        "Allowed namespaces (comma-separated, leave blank for all):",
        value="",
    ).split(",")

    dry_run = st.checkbox("Dry run (don’t write changes)", value=True)

    if st.button("Start duplicate metafield sync"):
        st.write("Fetching products...")
        products = get_all_products(shop, token)

        groups = {}
        for p in products:
            groups.setdefault(norm_title(p["title"]), []).append(p)

        results = []
        for title, prods in groups.items():
            if len(prods) < 2:
                continue

            # Pick donor = product with most metafields
            mf_counts = [(len(get_metafields(shop, token, p["id"])), p) for p in prods]
            donor = max(mf_counts, key=lambda x: x[0])[1]
            donor_mfs = get_metafields(shop, token, donor["id"])

            for p in prods:
                if p["id"] == donor["id"]:
                    continue
                target_mfs = get_metafields(shop, token, p["id"])
                target_map = {(mf["namespace"], mf["key"]): mf for mf in target_mfs}

                for mf in donor_mfs:
                    if allowed_namespaces != [""] and mf["namespace"] not in allowed_namespaces:
                        continue
                    key = (mf["namespace"], mf["key"])
                    new_mf = {
                        "namespace": mf["namespace"],
                        "key": mf["key"],
                        "type": mf["type"],
                        "value": mf["value"],
                        "owner_resource": "product",
                        "owner_id": p["id"],
                    }
                    if key in target_map:
                        new_mf["id"] = target_map[key]["id"]

                    if not dry_run:
                        upsert_metafield(shop, token, p["id"], new_mf)

                results.append(f"Copied metafields from {donor['title']} ({donor['id']}) → {p['id']}")

        st.success("Done")
        st.write(results)
