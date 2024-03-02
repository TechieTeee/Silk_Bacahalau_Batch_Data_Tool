import pandas as pd
import pyarrow as pa
from google.colab import files
import io
from ceramic import Client, DID
from ceramic.stream import StreamType

# Connect to Ceramic (replace with your credentials)
ceramic = Client("http://localhost:7507", DID.from_seed("YOUR_SEED"))

# Function to upload Parquet file to ComposeDB
def upload_to_compose_db(file_data, stream_id):
    stream = ceramic.create_stream(StreamType.BLOB, content=file_data)
    ceramic.update_stream(stream_id, content=stream.commit_id)

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
parquet_buffer = io.BytesIO()
pa.parquet.write_table(table, parquet_buffer, compression="snappy")

# Get the compressed Parquet file content
parquet_data = parquet_buffer.getvalue()

# Replace "your-stream-id" with the actual stream ID
stream_id = "your-stream-id"

# Upload Parquet file to ComposeDB
upload_to_compose_db(parquet_data, stream_id)

# Print memory usage after conversion
print(f"Memory usage after conversion: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")
print("Parquet file uploaded to ComposeDB!")