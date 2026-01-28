"""
this script is used to merge multiple sheets into one
"""

import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
INPUT_FILES = [
    # r"1200023154PL_MedPart.xlsx",
    r"1200023154PL_Medline.xlsx",
]

SHEETS_TO_APPEND = []  # empty = all sheets
HEADER_KEYWORD = "MANUFACTURER PART NUMBER"   # 👈 must exist in real header

OUTPUT_DIR = Path(".")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MASTER_SHEET_NAME = "Master"

# =========================
# HELPERS
# =========================
def find_header_row(file_path, sheet_name, keyword):
    preview = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    for i, row in preview.iterrows():
        if keyword in row.astype(str).values:
            return i
    raise ValueError("Header row not found")

def normalize_columns(df):
    """Normalize column names by stripping whitespace and handling duplicates"""
    # Strip whitespace from column names
    df.columns = df.columns.astype(str).str.strip()
    
    # Find and handle duplicate columns (case-insensitive)
    seen_columns = {}
    new_columns = []
    
    for col in df.columns:
        col_clean = col.strip().upper()
        
        if col_clean in seen_columns:
            # If we've seen this column before, append suffix
            seen_columns[col_clean] += 1
            new_col = f"{col}_{seen_columns[col_clean]}"
            print(f"    🔄 Renamed duplicate column: '{col}' -> '{new_col}'")
        else:
            seen_columns[col_clean] = 0
            new_col = col
        
        new_columns.append(new_col)
    
    df.columns = new_columns
    return df

# =========================
# PROCESS FILES
# =========================
for input_file in INPUT_FILES:
    input_path = Path(input_file)
    print(f"\nProcessing: {input_path.name}")

    dfs = []

    # Load all sheet names if not provided
    if not SHEETS_TO_APPEND:
        with pd.ExcelFile(input_path) as excel_file:
            sheets_to_process = excel_file.sheet_names
        print(f"  📋 Found {len(sheets_to_process)} sheets: {', '.join(sheets_to_process)}")
    else:
        sheets_to_process = SHEETS_TO_APPEND

    for sheet in sheets_to_process:
        try:
            header_row = find_header_row(input_path, sheet, HEADER_KEYWORD)

            df = pd.read_excel(
                input_path,
                sheet_name=sheet,
                skiprows=header_row,
                header=0
            )
            
            # Normalize columns to handle duplicates like "UNIT OF MEASURE" vs "UNIT OF MEASURE "
            df = normalize_columns(df)

            df["source_sheet"] = sheet
            df["source_file"] = input_path.name

            dfs.append(df)
            print(f"  {sheet}: {len(df)} rows (header at row {header_row})")

        except Exception as e:
            print(f"  ⚠️ Skipping sheet {sheet}: {e}")

    if not dfs:
        print("  ❌ No valid sheets found. Skipping file.")
        continue

    master_df = pd.concat(dfs, ignore_index=True)

    output_file = OUTPUT_DIR / f"{input_path.stem}_master.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        master_df.to_excel(writer, sheet_name=MASTER_SHEET_NAME, index=False)

    print(f"  ✅ Saved: {output_file}")
