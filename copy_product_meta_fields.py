import time
import requests
import streamlit as st
from collections import defaultdict

# =========================
# Settings / Secrets
# =========================
API_VERSION = st.secrets.get("SHOPIFY_API_VERSION", "2024-10")  # keep configurable
SHOP_DOMAIN = st.secrets["SHOP_A_DOMAIN"]                        # e.g. "yourstore.myshopify.com"
ADMIN_TOKEN = st.secrets["SHOP_A_TOKEN"]                         # Admin API access token

# Optional allow-list to restrict what gets copied
DEFAULT_ALLOWED = [
    # Examples:
    # ("namespace", None),           # copy all keys in namespace
    # ("namespace", "key_name"),     # copy only specific key
]
# Tip: leave empty to copy ALL product-level metafields

# =========================
# HTTP helpers
# =========================
def _headers():
    return {
        "X-Shopify-Access-Token": ADMIN_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _rest(method, path, params=None, json=None, max_retries=5):
    url = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}{path}"
    for attempt in range(max_retries):
        resp = requests.request(method, url, headers=_headers(), params=params, json=json)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "2"))
            time.sleep(retry_after)
            continue
        if 500 <= resp.status_code < 600:
            time.sleep(1.5 * (attempt + 1))
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp  # not reached

def _paginate_get(path, params=None, key=None):
    params = params or {}
    params["limit"] = 250
    out = []
    next_page_info = None
    while True:
        if next_page_info:
            params["page_info"] = next_page_info
        resp = _rest("GET", path, params=params)
        data = resp.json()
        if key:
            out.extend(data.get(key, []))
        else:
            out.append(data)
        # parse Link header for page_info
        link = resp.headers.get("Link", "")
        next_page_info = None
        if 'rel="next"' in link:
            # extract page_info
            try:
                part = [p for p in link.split(",") if 'rel="next"' in p][0]
                # page_info=...> format
                pi = part.split("page_info=")[1].split(">")[0]
                next_page_info = pi
            except Exception:
                next_page_info = None
        if not next_page_info:
            break
    return out

# =========================
# Product & metafield ops
# =========================
def normalize_title(s: str) -> str:
    return (s or "").strip().lower()

def fetch_all_products():
    # Only fetch fields we need
    return _paginate_get(f"/products.json", params={"fields": "id,title,handle"}, key="products")

def fetch_product_metafields(product_id: int):
    return _paginate_get(f"/products/{product_id}/metafields.json", key="metafields")

def upsert_metafield(product_id: int, ns: str, key: str, value, type_str: str, existing_by_ns_key):
    # If it exists -> PUT, else -> POST
    mf = existing_by_ns_key.get((ns, key))
    payload = {"metafield": {"namespace": ns, "key": key, "type": type_str, "value": value}}
    if mf:
        mf_id = mf["id"]
        _rest("PUT", f"/metafields/{mf_id}.json", json=payload)
        return "updated"
    else:
        payload["metafield"]["owner_resource"] = "product"
        payload["metafield"]["owner_id"] = product_id
        _rest("POST", f"/metafields.json", json=payload)
        return "created"

def build_allow_predicate(allowed):
    if not allowed:
        return lambda ns, key: True
    allowed_set = set(allowed)
    whole_ns = {ns for ns, k in allowed if k is None}
    def _ok(ns, key):
        return (ns, key) in allowed_set or ns in whole_ns
    return _ok

# =========================
# Streamlit UI
# =========================
st.subheader("Copy product metafields to products with the same name (within Shop A)")
st.caption("Find duplicate product titles and copy product-level metafields from a chosen donor to the others.")

with st.expander("Options", expanded=True):
    allowed_input = st.text_area(
        "Allowed namespaces/keys (optional)",
        help="One per line as 'namespace' or 'namespace.key'. Leave empty to copy ALL product-level metafields.",
        value=""
    )
    DRY_RUN = st.checkbox("Dry run (preview only)", value=True)
    donor_strategy = st.selectbox(
        "Donor selection strategy per title",
        ["Most metafields", "Oldest product ID", "Newest product ID"],
    )

def parse_allowed(text):
    if not text.strip():
        return []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    parsed = []
    for ln in lines:
        if "." in ln:
            ns, key = ln.split(".", 1)
            parsed.append((ns.strip(), key.strip()))
        else:
            parsed.append((ln.strip(), None))
    return parsed

if st.button("Scan & Copy"):
    with st.spinner("Fetching products…"):
        products = fetch_all_products()

    # Group by normalized title
    groups = defaultdict(list)
    for p in products:
        groups[normalize_title(p["title"])].append(p)

    # Keep only groups with duplicates
    dup_groups = {t: lst for t, lst in groups.items() if len(lst) > 1}

    if not dup_groups:
        st.success("No duplicate product titles found. Nothing to copy.")
        st.stop()

    st.write(f"Found **{len(dup_groups)}** duplicate title group(s).")

    allowed = parse_allowed(allowed_input)
    can_copy = build_allow_predicate(allowed)

    results = []
    for title_norm, plist in dup_groups.items():
        # Choose donor
        if donor_strategy == "Most metafields":
            mf_counts = []
            for p in plist:
                mfs = fetch_product_metafields(p["id"])
                mf_counts.append((p, mfs, len(mfs)))
            # pick product with most metafields
            donor_p, donor_mfs, _ = sorted(mf_counts, key=lambda t: t[2], reverse=True)[0]
        elif donor_strategy == "Oldest product ID":
            donor_p = sorted(plist, key=lambda p: p["id"])[0]
            donor_mfs = fetch_product_metafields(donor_p["id"])
        else:
            donor_p = sorted(plist, key=lambda p: p["id"], reverse=True)[0]
            donor_mfs = fetch_product_metafields(donor_p["id"])

        # Index donor metafields (product-level only)
        donor_mfs = [m for m in donor_mfs if m.get("owner_resource") == "product" or True]  # product endpoint already filters
        if allowed:
            donor_mfs = [m for m in donor_mfs if can_copy(m["namespace"], m["key"])]

        # Prepare map for quick lookup by (ns,key)
        donor_by_ns_key = {(m["namespace"], m["key"]): m for m in donor_mfs}

        # Copy to others in group
        for tgt in plist:
            if tgt["id"] == donor_p["id"]:
                continue
            tgt_mfs = fetch_product_metafields(tgt["id"])
            tgt_by_ns_key = {(m["namespace"], m["key"]): m for m in tgt_mfs}

            actions = []
            for (ns, key), mf in donor_by_ns_key.items():
                val = mf["value"]
                typ = mf["type"]
                exists = (ns, key) in tgt_by_ns_key
                action = "update" if exists else "create"
                actions.append((ns, key, action, typ, val))

            # Execute
            changed = []
            if not DRY_RUN:
                for ns, key, action, typ, val in actions:
                    status = upsert_metafield(tgt["id"], ns, key, val, typ, tgt_by_ns_key)
                    changed.append((ns, key, status))
                    # Simple, polite pacing
                    time.sleep(0.05)
            else:
                changed = [(ns, key, "would_" + action) for ns, key, action, typ, val in actions]

            results.append({
                "title": plist[0]["title"],
                "donor_id": donor_p["id"],
                "target_id": tgt["id"],
                "changes": changed,
            })

    # Summarize
    total_ops = sum(len(r["changes"]) for r in results)
    if DRY_RUN:
        st.info(f"Dry run complete. {len(results)} target products affected; {total_ops} metafield operations would be performed.")
    else:
        st.success(f"Done. {len(results)} target products updated; {total_ops} metafields created/updated.")

    # Optional: show a compact log
    with st.expander("Details"):
        for r in results:
            st.write(f'**Title:** {r["title"]} | Donor: {r["donor_id"]} → Target: {r["target_id"]}')
            for ns, key, status in r["changes"]:
                st.write(f"- `{ns}.{key}`: {status}")
