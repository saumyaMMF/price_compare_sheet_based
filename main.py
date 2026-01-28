import pandas as pd

# =========================
# CONFIG
# =========================
INPUT_FILES = [
    r"1200023154PL_MedPart.xlsx",
    r"1200023154PL_Medline.xlsx",
]
OUTPUT_FILE = r"Master_Merged_Sheets.xlsx"
OUTPUT_SHEET = "Master"


def load_all_sheets(file_path: str) -> list[pd.DataFrame]:
    sheets = pd.read_excel(file_path, sheet_name=None)
    frames = []
    for sheet_name, df in sheets.items():
        if df.empty:
            continue
        df = df.copy()
        df.insert(0, "Source File", file_path)
        df.insert(1, "Source Sheet", sheet_name)
        frames.append(df)
    return frames


def build_master_dataframe(files: list[str]) -> pd.DataFrame:
    all_frames: list[pd.DataFrame] = []
    for file_path in files:
        all_frames.extend(load_all_sheets(file_path))
    if not all_frames:
        return pd.DataFrame()
    return pd.concat(all_frames, ignore_index=True, sort=False)


def main() -> None:
    master_df = build_master_dataframe(INPUT_FILES)
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        master_df.to_excel(writer, sheet_name=OUTPUT_SHEET, index=False)
    print(f"✅ Master sheet created: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
