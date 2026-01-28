"""
this script is used to compare the Lot 1A and Cost sheets
"""

import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================
INPUT_FILE = r"New Price Master Worksheet 1-23-2026 - Copy.xlsx"
OUTPUT_FILE_WITH_UOM = r"Cost_vs_Price_With_UOM.xlsx"
OUTPUT_FILE_NO_UOM = r"Cost_vs_Price_No_UOM.xlsx"

LOT1A_SHEET = "Lot 1A"
COST_SHEET = "Cost"
OUTPUT_SHEET_WITH_UOM = "Lot1A_Cost"
OUTPUT_SHEET_NO_UOM = "Lot1A_Cost_NoUOM"

JOIN_KEYS_FULL = [
    "supplier_name",
    "manufacturer_part_number",
    "vendor_part_number",
    "uom",
]

JOIN_KEYS_NO_UOM = [key for key in JOIN_KEYS_FULL if key != "uom"]

UOM_MAP = {
    "ea": "each",
    "each": "each",
    "1 ea": "each",
    "cs": "case",
    "case": "case",
    "box": "box",
    "box of 10": "box",
    "bottle": "bottle",
}

# =========================
# LOAD DATA
# =========================
lot1a_raw = pd.read_excel(INPUT_FILE, sheet_name=LOT1A_SHEET)
cost_raw = pd.read_excel(INPUT_FILE, sheet_name=COST_SHEET)

# Preserve original Lot 1A order and add row id for later alignment
lot1a_columns = list(lot1a_raw.columns)
lot1a_raw["__row_id"] = np.arange(len(lot1a_raw))

# =========================
# PREPARE CANONICAL VIEWS
# =========================
lot1a_proc = lot1a_raw.rename(columns={
    "MANUFACTURER/SUPPLIER": "supplier_name",
    "MANUFACTURER PART NUMBER": "manufacturer_part_number",
    "VENDOR PART NUMBER": "vendor_part_number",
    "UNIT OF MEASURE (UOM)": "uom",
    "NET PRICE": "net_price",
}).copy()

cost_proc = cost_raw.rename(columns={
    "Supplier Name": "supplier_name",
    "Supplier Item Code": "manufacturer_part_number",
    "NDC Item Code": "vendor_part_number",
    "Package": "uom",
    "Price": "cost_price",
}).copy()

lot1a_proc["__row_id"] = lot1a_raw["__row_id"]


def normalize_uom(series: pd.Series) -> pd.Series:
    series = series.astype("string").str.strip().str.lower()
    return series.map(lambda x: UOM_MAP.get(x, x))


def clean_price(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string").str.replace(r"[,$]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


lot1a_proc["uom"] = normalize_uom(lot1a_proc["uom"])
cost_proc["uom"] = normalize_uom(cost_proc["uom"])

lot1a_proc["net_price"] = clean_price(lot1a_proc["net_price"])
cost_proc["cost_price"] = clean_price(cost_proc["cost_price"])

for df in (lot1a_proc, cost_proc):
    for key in JOIN_KEYS_FULL:
        df[key] = df[key].astype("string").str.strip().str.lower()


def build_comparison(join_keys: list[str]) -> tuple[pd.DataFrame, list[str], dict[str, int]]:
    merged = lot1a_proc.merge(
        cost_proc[join_keys + ["cost_price"]],
        on=join_keys,
        how="left",
    )

    merged["Profit Margin ($)"] = merged["net_price"] - merged["cost_price"]

    net_price_denominator = merged["net_price"].replace({0: pd.NA})
    merged["Profit Margin (%)"] = merged["Profit Margin ($)"].divide(net_price_denominator)
    merged["Profit Margin (%)"] = merged["Profit Margin (%)"].round(4)

    status = pd.Series("OK", index=merged.index, dtype="string")

    missing_mask = merged["cost_price"].isna()
    status.loc[missing_mask] = "Missing in Cost"

    cost_higher_mask = (~missing_mask) & (merged["Profit Margin ($)"] < 0)
    status.loc[cost_higher_mask] = "Cost Higher Than Price"

    merged["Status"] = status

    stats = {
        "matched": int(merged["cost_price"].notna().sum()),
        "unmatched": int(merged["cost_price"].isna().sum()),
        "total": int(len(merged)),
    }

    cost_columns = merged[[
        "__row_id",
        "cost_price",
        "Profit Margin ($)",
        "Profit Margin (%)",
        "Status",
    ]].rename(columns={
        "cost_price": "Cost Price",
    })

    return cost_columns, stats


def compose_final_output(cost_columns: pd.DataFrame) -> pd.DataFrame:
    final_df = lot1a_raw.merge(cost_columns, on="__row_id", how="left")
    final_df = final_df.drop(columns=["__row_id"])

    additional_cols = [col for col in final_df.columns if col not in lot1a_columns]
    final_df = final_df[lot1a_columns + additional_cols]
    return final_df


cost_columns_full, stats_full = build_comparison(JOIN_KEYS_FULL)
cost_columns_no_uom, stats_no_uom = build_comparison(JOIN_KEYS_NO_UOM)

final_df_full = compose_final_output(cost_columns_full)
final_df_no_uom = compose_final_output(cost_columns_no_uom)

with pd.ExcelWriter(OUTPUT_FILE_WITH_UOM, engine="openpyxl") as writer:
    final_df_full.to_excel(writer, sheet_name=OUTPUT_SHEET_WITH_UOM, index=False)

with pd.ExcelWriter(OUTPUT_FILE_NO_UOM, engine="openpyxl") as writer:
    final_df_no_uom.to_excel(writer, sheet_name=OUTPUT_SHEET_NO_UOM, index=False)

print(
    "Comparison complete. "
    f"Output sheets '{OUTPUT_SHEET_WITH_UOM}' and '{OUTPUT_SHEET_NO_UOM}' written to {OUTPUT_FILE_WITH_UOM} and {OUTPUT_FILE_NO_UOM}."
)

print("\nMatch statistics:")
print(
    f"  - With UOM: matched={stats_full['matched']}, "
    f"unmatched={stats_full['unmatched']} (total={stats_full['total']})"
)
print(
    f"  - Without UOM: matched={stats_no_uom['matched']}, "
    f"unmatched={stats_no_uom['unmatched']} (total={stats_no_uom['total']})"
)