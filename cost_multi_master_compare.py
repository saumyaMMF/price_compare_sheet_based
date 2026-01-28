import numpy as np
import pandas as pd
from pathlib import Path

"""Compare the Lot 1A sheet against Cost, Medline master, and MedPart master pricing.

The script preserves every original column from the Lot 1A sheet and appends the
matching price columns pulled from the other sources. It also reports match
statistics for each source.
"""

# =========================
# CONFIGURATION
# =========================
SOURCE_WORKBOOK = Path("New Price Master Worksheet 1-23-2026 - Copy.xlsx")
LOT1A_SHEET = "Lot 1A"

OUTPUT_WORKBOOK = Path("Lot1A_vs_All_Masters.xlsx")
OUTPUT_SHEET_ALL_KEYS = "Lot1A_vs_Masters"
OUTPUT_SHEET_NO_UOM = "Lot1A_vs_Masters_NoUOM"

JOIN_KEYS_FULL = [
    "supplier_name",
    "manufacturer_part_number",
    # "vendor_part_number",
    "uom",
]

JOIN_KEYS_NO_UOM = [key for key in JOIN_KEYS_FULL if key != "uom"]

UOM_NORMALIZATION_MAP = {
    "ea": "each",
    "each": "each",
    "1 ea": "each",
    "cs": "case",
    "case": "case",
    "box": "box",
    "box of 10": "box",
    "bottle": "bottle",
}

LOT1A_RENAME_MAP = {
    "MANUFACTURER/SUPPLIER": "supplier_name",
    "Manufacturer / Supplier": "supplier_name",
    "MANUFACTURER PART NUMBER": "manufacturer_part_number",
    "Manufacturer Part Number": "manufacturer_part_number",
    "VENDOR PART NUMBER": "vendor_part_number",
    "Vendor Part Number": "vendor_part_number",
    "UNIT OF MEASURE (UOM)": "uom",
    "UNIT OF MEASURE": "uom",
    "UOM": "uom",
    "NET PRICE": "lot1a_price",
    "Net Price": "lot1a_price",
}

PRICE_LIST_RENAME_MAP = {
    "Supplier Name": "supplier_name",
    "Supplier Item Code": "manufacturer_part_number",
    "NDC Item Code": "vendor_part_number",
    "Package": "uom",
    "Price": "cost_price",
}

MEDLINE_RENAME_MAP = {
    "MANUFACTURER/SUPPLIER": "supplier_name",
    "Manufacturer / Supplier": "supplier_name",
    "MANUFACTURER PART NUMBER": "manufacturer_part_number",
    "Manufacturer Part Number": "manufacturer_part_number",
    "VENDOR PART NUMBER": "vendor_part_number",
    "Vendor Part Number": "vendor_part_number",
    "UNIT OF MEASURE": "uom",
    "UNIT OF MEASURE (UOM)": "uom",
    "UOM": "uom",
    "NYS NET PRICE": "medline_price",
}

MEDPART_RENAME_MAP = {
    "MANUFACTURER/SUPPLIER": "supplier_name",
    "Manufacturer / Supplier": "supplier_name",
    "MANUFACTURER PART NUMBER": "manufacturer_part_number",
    "Manufacturer Part Number": "manufacturer_part_number",
    "VENDOR PART NUMBER": "vendor_part_number",
    "Vendor Part Number": "vendor_part_number",
    "UNIT OF MEASURE": "uom",
    "UNIT OF MEASURE (UOM)": "uom",
    "UOM": "uom",
    "NYS NET PRICE": "medpart_price",
}


def preprocess_medpart(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading three digits and dash from MedPart manufacturer numbers."""

    pattern = r"^[0-9]{3}-"
    if "manufacturer_part_number" in df.columns:
        df["manufacturer_part_number"] = (
            df["manufacturer_part_number"].astype("string").str.replace(pattern, "", regex=True)
        )
    return df

PRICE_SOURCES = [
    {
        "name": "Cost",
        "workbook": SOURCE_WORKBOOK,
        "sheet": "Cost",
        "rename_map": PRICE_LIST_RENAME_MAP,
        "price_field": "cost_price",
        "alias": "Cost Price",
        "additional_columns": {
            "Product Name": "Cost Product Name",
            "Item Name": "Cost Item Name",
            "NDC Item Code": "Cost NDC Item Code",
        },
    },
    {
        "name": "Medline Master",
        "workbook": Path("1200023154PL_Medline_master.xlsx"),
        "sheet": "Master",
        "rename_map": MEDLINE_RENAME_MAP,
        "price_field": "medline_price",
        "alias": "Medline Master Price",
    },
    {
        "name": "MedPart Master",
        "workbook": Path("1200023154PL_MedPart_master.xlsx"),
        "sheet": "Master",
        "rename_map": MEDPART_RENAME_MAP,
        "price_field": "medpart_price",
        "alias": "MedPart Master Price",
    },
]


# =========================
# HELPERS
# =========================
def rename_available(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    available = {col: mapping[col] for col in mapping if col in df.columns}
    return df.rename(columns=available)


def clean_price(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.replace(r"[,$]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


def normalize_key(series: pd.Series, key: str) -> pd.Series:
    normalized = series.astype("string").str.strip().str.lower()
    if key == "uom":
        normalized = normalized.map(lambda x: UOM_NORMALIZATION_MAP.get(x, x))
    return normalized


def normalize_join_keys(df: pd.DataFrame, join_keys: list[str]) -> pd.DataFrame:
    for key in join_keys:
        if key in df.columns:
            df[key] = normalize_key(df[key], key)
    return df


def load_price_source(cfg: dict, join_keys: list[str]) -> tuple[pd.DataFrame, str, list[str]]:
    workbook = cfg["workbook"]
    sheet_name = cfg["sheet"]
    rename_map = cfg["rename_map"]
    price_field = cfg["price_field"]

    df = pd.read_excel(workbook, sheet_name=sheet_name)
    df = rename_available(df, rename_map)

    if cfg["name"] == "MedPart Master":
        df = preprocess_medpart(df)

    required_columns = set(JOIN_KEYS_FULL + [price_field])
    missing = required_columns.difference(df.columns)
    if missing:
        raise ValueError(
            f"Missing columns {missing} in sheet '{sheet_name}' of {Path(workbook).name}"
        )

    df[price_field] = clean_price(df[price_field])
    df = normalize_join_keys(df, JOIN_KEYS_FULL)

    price_alias = cfg["alias"]
    if price_alias != price_field:
        if price_alias in df.columns:
            raise ValueError(f"Alias '{price_alias}' already exists in {cfg['name']} data")
        df = df.rename(columns={price_field: price_alias})
    else:
        price_alias = price_field

    additional_aliases: list[str] = []
    for original_col, alias in cfg.get("additional_columns", {}).items():
        if original_col not in df.columns:
            continue
        if alias in df.columns:
            raise ValueError(f"Alias '{alias}' already exists in {cfg['name']} data")
        df = df.rename(columns={original_col: alias})
        additional_aliases.append(alias)

    selected_columns = join_keys + [price_alias] + additional_aliases
    return df[selected_columns], price_alias, additional_aliases


# =========================
# LOAD LOT 1A SHEET
# =========================
lot1a_raw = pd.read_excel(SOURCE_WORKBOOK, sheet_name=LOT1A_SHEET)
lot1a_raw = lot1a_raw.copy()
lot1a_raw["__row_id"] = np.arange(len(lot1a_raw))
original_lot1a_columns = [col for col in lot1a_raw.columns if col != "__row_id"]

lot1a_proc = rename_available(lot1a_raw, LOT1A_RENAME_MAP)
required_lot1a_columns = set(JOIN_KEYS_FULL + ["lot1a_price"])
missing_lot1a_columns = required_lot1a_columns.difference(lot1a_proc.columns)
if missing_lot1a_columns:
    raise ValueError(
        f"Missing columns {missing_lot1a_columns} in sheet '{LOT1A_SHEET}' of {SOURCE_WORKBOOK.name}"
    )

lot1a_proc["lot1a_price"] = clean_price(lot1a_proc["lot1a_price"])
lot1a_proc["__row_id"] = lot1a_raw["__row_id"]

lot1a_proc = normalize_join_keys(lot1a_proc, JOIN_KEYS_FULL)


def build_comparison(join_keys: list[str]) -> tuple[pd.DataFrame, list[str], list[dict[str, int | str]]]:
    comparison_df = lot1a_proc[["__row_id", *join_keys]].copy()
    appended_aliases: list[str] = []
    stats: list[dict[str, int | str]] = []

    for source in PRICE_SOURCES:
        source_df, price_alias, extra_aliases = load_price_source(source, join_keys)
        comparison_df = comparison_df.merge(source_df, on=join_keys, how="left")
        appended_aliases.extend([price_alias, *extra_aliases])

        matched_count = int(comparison_df[price_alias].notna().sum())
        total_rows = int(len(comparison_df))
        stats.append(
            {
                "source": source["name"],
                "matched": matched_count,
                "unmatched": total_rows - matched_count,
                "total": total_rows,
                "join_keys": ", ".join(join_keys),
            }
        )

    subset_columns = ["__row_id", *appended_aliases]
    return comparison_df[subset_columns], appended_aliases, stats


comparison_full, aliases_full, stats_full = build_comparison(JOIN_KEYS_FULL)
comparison_no_uom, aliases_no_uom, stats_no_uom = build_comparison(JOIN_KEYS_NO_UOM)


def compose_final_dataframe(base_df: pd.DataFrame, comparison_subset: pd.DataFrame, price_aliases: list[str]) -> pd.DataFrame:
    merged = base_df.merge(comparison_subset, on="__row_id", how="left")
    merged = merged.drop(columns=["__row_id"])

    price_columns = [col for col in merged.columns if "price" in col.lower()]
    non_price_columns = [col for col in merged.columns if col not in price_columns]
    return merged[non_price_columns + price_columns]


final_df_full = compose_final_dataframe(lot1a_raw.copy(), comparison_full, aliases_full)
final_df_no_uom = compose_final_dataframe(lot1a_raw.copy(), comparison_no_uom, aliases_no_uom)

# =========================
# WRITE OUTPUT
# =========================
with pd.ExcelWriter(OUTPUT_WORKBOOK, engine="openpyxl") as writer:
    final_df_full.to_excel(writer, sheet_name=OUTPUT_SHEET_ALL_KEYS, index=False)
    final_df_no_uom.to_excel(writer, sheet_name=OUTPUT_SHEET_NO_UOM, index=False)

print(
    "Comparison complete. "
    f"Output sheets '{OUTPUT_SHEET_ALL_KEYS}' and '{OUTPUT_SHEET_NO_UOM}' written to {OUTPUT_WORKBOOK.name}."
)

print("\nMatch statistics:")
for stat in stats_full + stats_no_uom:
    print(
        f"  - {stat['source']} (keys: {stat['join_keys']}): matched={stat['matched']}, "
        f"unmatched={stat['unmatched']} (total={stat['total']})"
    )
