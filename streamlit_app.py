from io import BytesIO
import datetime as dt

def _is_effectively_empty(v):
    # None or blank strings count as empty; numbers 0, False, "0" are NOT empty.
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False

def _drop_all_empty_columns(df, keep_always=None):
    """
    Remove columns where every row is empty (None or blank string).
    keep_always: set of column names to always keep (e.g., ids/titles)
    """
    if df is None or df.empty:
        return df
    keep_always = keep_always or set()
    cols_to_keep = []
    for col in df.columns:
        if col in keep_always:
            cols_to_keep.append(col)
            continue
        series = df[col]
        # Keep column if any row has non-empty value
        if not all(_is_effectively_empty(v) for v in series):
            cols_to_keep.append(col)
    return df[cols_to_keep]

def metafields_dict(resource, only_synced=False):
    """
    Returns a flat dict of metafields for a product or variant:
    keys look like 'namespace.key' -> value (None/str)
    """
    allowed_keys = set(get_sync_keys(resource)) if only_synced else None
    out = {}
    try:
        for m in resource.metafields():
            if only_synced and (m.key not in allowed_keys):
                continue
            ns = getattr(m, "namespace", "mf")
            key = f"{ns}.{m.key}"
            # Preserve None; turn empty strings into None so all-empty cols get dropped.
            val = m.value
            if isinstance(val, str) and val.strip() == "":
                val = None
            out[key] = val
    except Exception:
        pass
    return out

def build_category_export(products_in_type, only_synced=False, include_variants=True):
    """
    Build two DataFrames:
      - products_df: one row per product (standard fields + metafields)
      - variants_df: one row per variant (standard fields + metafields)
    Drops any columns that are completely empty across the sheet,
    while always keeping key identifiers.
    """
    product_rows, variant_rows = [], []

    for idx, p in enumerate(products_in_type, 1):
        # --- Product row (standard fields)
        base = {
            "product_id": p.id,
            "title": getattr(p, "title", None) or None,
            "handle": getattr(p, "handle", None) or None,
            "vendor": getattr(p, "vendor", None) or None,
            "product_type": getattr(p, "product_type", None) or None,
            "status": getattr(p, "status", None) or None,
            "tags": (
                ", ".join(p.tags) if isinstance(getattr(p, "tags", ""), list)
                else (getattr(p, "tags", None) or None)
            ),
            "created_at": getattr(p, "created_at", None) or None,
            "updated_at": getattr(p, "updated_at", None) or None,
        }
        base.update(metafields_dict(p, only_synced=only_synced))
        product_rows.append(base)

        # --- Variant rows (optional)
        if include_variants:
            for v in getattr(p, "variants", []):
                vbase = {
                    "product_id": p.id,
                    "product_title": getattr(p, "title", None) or None,
                    "variant_id": getattr(v, "id", None) or None,
                    "variant_title": getattr(v, "title", None) or None,
                    "sku": getattr(v, "sku", None) or None,
                    "barcode": getattr(v, "barcode", None) or None,
                    "price": getattr(v, "price", None) or None,
                    "compare_at_price": getattr(v, "compare_at_price", None) or None,
                    "position": getattr(v, "position", None) or None,
                    "option1": getattr(v, "option1", None) or None,
                    "option2": getattr(v, "option2", None) or None,
                    "option3": getattr(v, "option3", None) or None,
                }
                vbase.update(metafields_dict(v, only_synced=only_synced))
                variant_rows.append(vbase)
                time.sleep(0.4)  # gentle on rate limits

        if idx % 10 == 0:
            time.sleep(0.2)

    products_df = pd.DataFrame(product_rows) if product_rows else pd.DataFrame()
    variants_df = pd.DataFrame(variant_rows) if variant_rows else pd.DataFrame()

    # Drop columns that are entirely empty, but always keep identifiers
    products_df = _drop_all_empty_columns(
        products_df,
        keep_always={"product_id", "title", "handle", "product_type"}
    )
    if not variants_df.empty:
        variants_df = _drop_all_empty_columns(
            variants_df,
            keep_always={"product_id", "product_title", "variant_id", "variant_title", "sku", "barcode"}
        )

    return products_df, variants_df

def make_xlsx_download(products_df, variants_df, store_key, category_label):
    """
    Write the two DataFrames into an in-memory XLSX with 2 sheets.
    Only columns with any data remain, thanks to build_category_export.
    """
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        (products_df or pd.DataFrame()).to_excel(writer, index=False, sheet_name="Products")
        (variants_df or pd.DataFrame()).to_excel(writer, index=False, sheet_name="Variants")
    buf.seek(0)
    safe_cat = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in category_label)[:60]
    fname = f"export_{store_key}_{safe_cat}_{dt.date.today().isoformat()}.xlsx"
    return fname, buf
