import pandas as pd
import os
from datetime import datetime, timedelta

PIDROZDIL_TO_RU_MAPPING = {
    "ДОРД": "ЧЦП",
    "Ізмаїльського": "ПдРУ",
    "Бердянського": "ПдРУ",
    "Білгород-Дністровського": "ПдРУ",
    "Житомирського": "ЧЦП",
    "ЗхРУ": "ЗхРУ",
    "Краматорськ": "СхРУ",
    "Волинського": "ЗхРУ",
    "Карпатського": "ЗхРУ",
    "Могилів-Подільського": "ПдРУ",
    "Мукачівського": "ЗхРУ",
    'ОКПП "Київ"': "ЧЦП",
    "Одеського": "ПдРУ",
    "ПдРУ": "ПдРУ",
    "Подільського": "ПдРУ",
    "РУМО": "РУМО",
    "РУМО Ізмаїл": "РУМО",
    "РУМО Маріуполь": "РУМО",
    "РУМО Одеса": "РУМО",
    "Сумського": "СхРУ",
    "СхРУ": "СхРУ",
    "Харківського": "СхРУ",
    "Херсонського": "ПдРУ",
    "Чернівецького": "ЗхРУ",
    "Чернігівського": "ЧЦП",
    "Чопського": "ЗхРУ",
}

def week_start_end_dates(year, week):
    start = datetime.fromisocalendar(year, int(week), 1)
    end = start + timedelta(days=6)
    return start.date(), end.date()

def process_single_file(file_path, year):
    excel = pd.ExcelFile(file_path)
    sheets_to_skip = ["звіт", "звіти по місяцям", "звіт по місяцям", "ДОРД"]

    all_data = []
    for sheet in excel.sheet_names:
        if sheet.lower() in sheets_to_skip:
            continue
        df = excel.parse(sheet)
        if "Unnamed: 66" not in df.columns:
            continue
        df = df[df["Unnamed: 66"].notna()]

        week_cols = [col for col in df.columns if isinstance(col, (int, float)) and 1 <= int(col) <= 52]
        if not week_cols:
            continue
        df_reduced = df[[df.columns[0]] + week_cols].copy()
        df_reduced[df.columns[0]] = df["Unnamed: 66"]
        df_reduced.rename(columns={df.columns[0]: "Показник"}, inplace=True)
        df_reduced["Підрозділ"] = sheet.strip()
        all_data.append(df_reduced)

    if not all_data:
        return pd.DataFrame()

    full_df = pd.concat(all_data, ignore_index=True)
    week_columns = [col for col in full_df.columns if isinstance(col, (int, float)) and 1 <= int(col) <= 52]
    for col in week_columns:
        full_df[col] = (
            full_df[col]
            .astype(str)
            .str.replace("\xa0", "", regex=False)  # remove non-breaking spaces
            .str.replace(" ", "", regex=False)  # remove normal spaces
            .str.replace(",", ".", regex=False)  # optional: replace commas with dots (if decimals)
        )
        full_df[col] = pd.to_numeric(full_df[col], errors="coerce").fillna(0).astype(int)

    # Melt and pivot
    melted = full_df.melt(
        id_vars=["Підрозділ", "Показник"],
        value_vars=week_columns,
        var_name="Тиждень",
        value_name="Значення"
    )

    pivoted = melted.pivot_table(
        index=["Підрозділ", "Тиждень"],
        columns="Показник",
        values="Значення",
        aggfunc="first"
    ).reset_index()

    # Convert all numeric metric columns to integers
    numeric_columns = pivoted.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns
    metric_columns = [col for col in numeric_columns if col not in ["Тиждень"]]
    pivoted[metric_columns] = pivoted[metric_columns].fillna(0).astype(int)

    # Add week date range columns
    pivoted["Початок тижня"], pivoted["Кінець тижня"] = zip(
        *pivoted["Тиждень"].apply(lambda w: week_start_end_dates(year, w))
    )

    # Add РУ column based on Підрозділ mapping
    pivoted["РУ"] = pivoted["Підрозділ"].map(PIDROZDIL_TO_RU_MAPPING)

    # Fill missing РУ values with a default or leave as NaN
    # Uncomment the next line if you want to fill missing values with a default:
    # pivoted["РУ"] = pivoted["РУ"].fillna("Невідомо")

    # Reorder columns - РУ first, then existing order
    cols = pivoted.columns.tolist()
    week_idx = cols.index("Тиждень")
    cols.remove("Початок тижня")
    cols.remove("Кінець тижня")
    cols.remove("РУ")
    cols[week_idx + 1:week_idx + 1] = ["Початок тижня", "Кінець тижня"]

    # Put РУ as the first column
    cols = ["РУ"] + cols
    pivoted = pivoted[cols]

    return pivoted

def process_single_file_with_year(file_path, year):
    df = process_single_file(file_path, year)
    if not df.empty:
        df["Рік"] = year
    return df

def process_folder_combined_years(base_input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    year_folders = {
        "2024": os.path.join(base_input_folder, "2024"),
        "2025": os.path.join(base_input_folder, "2025")
    }

    # Collect all files grouped by their base name (without year)
    file_groups = {}
    for year, folder in year_folders.items():
        for filename in os.listdir(folder):
            if not filename.endswith(".xlsx"):
                continue
            base_name = os.path.splitext(filename)[0]
            base_name = base_name.replace(year, "").strip(" _-")
            file_groups.setdefault(base_name, {})[year] = os.path.join(folder, filename)

    for base_name, files_by_year in file_groups.items():
        all_years_data = []
        for year, file_path in files_by_year.items():
            print(f"Processing: {file_path}")
            df = process_single_file_with_year(file_path, int(year))
            if not df.empty:
                all_years_data.append(df)

        if all_years_data:
            combined_df = pd.concat(all_years_data, ignore_index=True)
            output_filename = f"{base_name}.csv"
            output_path = os.path.join(output_folder, output_filename)
            combined_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"✅ Saved: {output_path}")
        else:
            print(f"⚠️ Skipped (no data): {base_name}")

prefix = "C:/Users/avoleksiuk/Desktop/Power BI/Керівництво/ДОРД/Sharepoint"
process_folder_combined_years(prefix + "/Звіти з Шарепоінта", prefix + "/csv")
