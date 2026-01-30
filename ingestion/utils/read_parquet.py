import pandas as pd
from tkinter import Tk, filedialog

# Hide the root Tk window
root = Tk()
root.withdraw()

# Open file picker
file_path = filedialog.askopenfilename(
    title="Select a Parquet file",
    filetypes=[("Parquet files", "*.parquet")]
)

if not file_path:
    print("No file selected.")
    exit()

# Read parquet
df = pd.read_parquet(file_path)

# Quick inspection
print("File:", file_path)
print("\nColumns:")
print(df.columns.tolist())

print("\nPreview:")
print(df.head())

print("\nDtypes:")
print(df.dtypes)

csv_path = file_path.replace(".parquet", ".csv")
df.to_csv(csv_path, index=False)

print(f"\nCSV written to: {csv_path}")
