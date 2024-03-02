# Upload CSV file using Colab's uploader
uploaded = files.upload()

# Get the uploaded filename (assuming a single file)
filename = list(uploaded.keys())[0]

# Read the CSV data from the bytes content
df = pd.read_csv(io.BytesIO(uploaded[filename]))

# Print memory usage before conversion
print(f"Memory usage before conversion: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")

# Convert to PyArrow Table
table = pa.Table.from_pandas(df)

# Write to Parquet with Snappy compression
# Assuming you meant to use the 'parquet' module from 'pyarrow'
pa.parquet.write_table(table, "output.parquet", compression="snappy")

# Print memory usage after conversion
print(f"Memory usage after conversion: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")