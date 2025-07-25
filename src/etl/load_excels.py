import pandas as pd


def find_header_row(file_path, sheet_name, search_term="Country"):
    # Read the Excel file without skipping rows
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    # Iterate through rows to find "Country"
    for idx, row in df.iterrows():
        if search_term in row.values:
            return idx  # Return the row index where "Country" is found
    return None  # If not found


excel_path = "src/data/SIPRI-Milex-data-2023-2023.xlsx"
sheet_names = [
    "Constant (2023) US$",
    "Current US$",
    "Share of GDP",
    "Per capita",
    "Share of Govt. spending",
]

clean_dfs = []

for sheet_name in sheet_names:
    header_row = find_header_row(excel_path, sheet_name)

    if header_row is None:
        raise ValueError("'Country' header not found in the sheet.")

    df = pd.read_excel(
        excel_path,
        sheet_name=sheet_name,
        skiprows=header_row,  # Skip rows before the header
    )
    df = df.rename(columns={df.columns[0]: "Country", df.columns[-1]: sheet_name})
    df = df.filter(["Country", sheet_name])  # Solo estas columnas
    # Quitar NaN, en nuestro caso son los valores de regiones o subregiones, no países
    df = df.dropna()
    clean_dfs.append(df)

# Seleccionar primer df
merged_df = clean_dfs[0]

# Mergeando a partir del segundo
for df in clean_dfs[1:]:
    merged_df = pd.merge(merged_df, df, on="Country", how="outer")

merged_df.to_csv("src/data/sipri_clean.csv", index=False)
