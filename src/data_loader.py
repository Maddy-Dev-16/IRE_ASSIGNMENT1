# src/data_loader.py

import os
import zipfile
import json
import pandas as pd
import pyarrow.parquet as pq
from typing import Iterable, Dict

def load_news_data(folder_path: str) -> Iterable[Dict]:
    """Loads news data from a folder of zip files and yields a dictionary."""
    doc_id_counter = 0
    print(f"Loading News data from: {folder_path}")
    for filename in os.listdir(folder_path):
        if filename.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(folder_path, filename), 'r') as zip_ref:
                for json_file in zip_ref.namelist():
                    with zip_ref.open(json_file) as file:
                        try:
                            data = json.load(file)
                            if data.get('language', '').lower() == "english":
                                yield {
                                    "doc_id": f"news_{doc_id_counter}",
                                    "content": data.get('text', ''),
                                    "author": data.get('author', ''),
                                    "published_date": data.get('published', None),
                                    "source": "news"
                                }
                                doc_id_counter += 1
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass 

def load_wiki_data(folder_path: str) -> Iterable[Dict]:
    """
    Loads Wikipedia data from a folder of Parquet files.
    Now limited to processing only the first 20 files it finds.
    """
    print(f"Loading Wikipedia data from: {folder_path}")
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Error: Directory not found at {folder_path}")

    parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]

    # We'll use a counter to process only the first 20 files
    files_processed = 0
    for file_path in parquet_files:
        if files_processed >= 20:
            print("\nReached the limit of 20 Parquet files.")
            break # Stop the loop

        print(f"Processing file {files_processed + 1} of 20: {os.path.basename(file_path)}")
        table = pq.read_table(file_path)
        df = table.to_pandas()
        
        for index, row in df.iterrows():
            yield {
                "doc_id": f"wiki_{row['id']}",
                "content": row['text'],
                "title": row['title'],
                "url": row['url'],
                "source": "wikipedia"
            }
        
        files_processed += 1