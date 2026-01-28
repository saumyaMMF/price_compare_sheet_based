import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================
INPUT_FILE = r"New Price Master Worksheet 1-23-2026 - Copy.xlsx"
OUTPUT_FILE = r"Cost_vs_Price_Comparison.xlsx"

LOT1A_SHEET = "Lot 1A"
COST_SHEET  = "Cost"

# =========================
# LOAD DATA
# =========================
lot1a_df = pd.read_excel(INPUT_FILE, sheet_name=LOT1A_SHEET)
cost_df  = pd.read_excel(INPUT_FILE, sheet_name=COST_SHEET)

# =========================
# RENAME JOIN KEYS (CANONICAL)
# =========================
lot1a_df = lot1a_df.rename(columns={
    "MANUFACTURER/SUPPLIER": "supplier_name",
    "MANUFACTURER PART NUMBER": "manufacturer_part_number",
    "VENDOR PART NUMBER": "vendor_part_number",
    "UNIT OF MEASURE (UOM)": "uom",
    "NET PRICE": "net_price"
})

cost_df = cost_df.rename(columns={
    "Supplier Name": "supplier_name",
    "Supplier Item Code": "manufacturer_part_number",
    "NDC Item Code": "vendor_part_number",
    "Package": "uom",
    "Price": "cost_price"
})

JOIN_KEYS = [
    "supplier_name",
    "manufacturer_part_number",
    "vendor_part_number",
    "uom"
]

# =========================
# NORMALIZE JOIN KEYS (FAST)
# =========================
for df in (lot1a_df, cost_df):
    for c in JOIN_KEYS:
        df[c] = df[c].astype("string").str.strip().str.lower()

# =========================
# MERGE (STRICT BUSINESS JOIN)
# =========================
merged = lot1a_df.merge(
    cost_df[JOIN_KEYS + ["cost_price"]],
    on=JOIN_KEYS,
    how="inner"
)



# =========================
# CALCULATIONS
# =========================
merged["Price Difference ($)"] = merged["net_price"] - merged["cost_price"]
merged["Price Difference (%)"] = (
    merged["Price Difference ($)"] / merged["net_price"]
)

# =========================
# STATUS (VECTORIZED & GUARANTEED)
# =========================
merged["Status"] = np.where(
    merged["cost_price"].isna(),
    "Missing in Cost",
    np.where(
        merged["Price Difference ($)"] < 0,
        "Cost Higher Than Price",
        "OK"
    )
)

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

def normalize_uom(s):
    return s.map(lambda x: UOM_MAP.get(x, x))

lot1a_df["uom"] = normalize_uom(lot1a_df["uom"])
cost_df["uom"]  = normalize_uom(cost_df["uom"])


# =========================
# FINAL COLUMN SELECTION
# (SINGLE SOURCE PER COLUMN)
# =========================
final_df = merged[[
    "Source Lot",
    "supplier_name",
    "manufacturer_part_number",
    "vendor_part_number",
    "DESCRIPTION",
    "uom",
    "net_price",
    "cost_price",
    "Price Difference ($)",
    "Price Difference (%)",
    "Status"
]].rename(columns={
    "supplier_name": "Manufacturer / Supplier",
    "manufacturer_part_number": "Manufacturer Part Number",
    "vendor_part_number": "Vendor Part Number",
    "uom": "UOM",
    "net_price": "Net Price",
    "cost_price": "Cost Price"
})

# =========================
# WRITE TO NEW EXCEL FILE
# =========================
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    final_df.to_excel(writer, sheet_name="Cost_vs_Price_Comparison", index=False)

print(f"✅ Comparison complete. Output written to: {OUTPUT_FILE}")
