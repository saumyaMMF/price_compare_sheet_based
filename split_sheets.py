"""
this script is used to split the Cost_vs_Price_Comparison.xlsx file into two files:
1. Matched
2. Unmatched
"""

import pandas as pd
from pathlib import Path


# =========================
# CONFIGURATION
# =========================
# INPUT_WORKBOOK = Path("Cost_vs_Price_Comparison.xlsx")
INPUT_WORKBOOK = Path("Cost_vs_All_Masters.xlsx")
SOURCE_SHEET = "Lot1A_Cost_Comparison"

OUTPUT_WORKBOOK = Path("Cost_vs_Price_Comparison_split.xlsx")
MATCHED_SHEET = "Matched"
UNMATCHED_SHEET = "Unmatched"


# =========================
# LOAD SOURCE DATA
# =========================
df = pd.read_excel(INPUT_WORKBOOK, sheet_name=SOURCE_SHEET)

# =========================
# SPLIT DATA
# =========================
matched_mask = df["Cost Price"].notna()

matched_df = df.loc[matched_mask].copy()
unmatched_df = df.loc[~matched_mask].copy()


# =========================
# WRITE OUTPUT
# =========================
with pd.ExcelWriter(OUTPUT_WORKBOOK, engine="openpyxl") as writer:
    matched_df.to_excel(writer, sheet_name=MATCHED_SHEET, index=False)
    unmatched_df.to_excel(writer, sheet_name=UNMATCHED_SHEET, index=False)


print(
    "Split complete. "
    f"Matched rows: {len(matched_df)}, "
    f"Unmatched rows: {len(unmatched_df)}. "
    f"Output file: {OUTPUT_WORKBOOK.name}"
)
