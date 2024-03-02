# -*- coding: utf-8 -*-
import pandas as pd
import pyarrow as pa
from pyarrow import parquet
from flask import Flask, render_template, request
import psutil
from google.colab import files
import psutil
import io

# Commented out IPython magic to ensure Python compatibility.
!command -v bacalhau >/dev/null 2>&1 || (export BACALHAU_INSTALL_DIR=.; curl -sL https://get.bacalhau.org/install.sh | bash)
path=!echo $PATH
# %env PATH=./:{path[0]}

!curl -sL https://get.bacalhau.org/install.sh | bash

!pip install fastavro

!pip install numpy

!pip install pyarrow

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