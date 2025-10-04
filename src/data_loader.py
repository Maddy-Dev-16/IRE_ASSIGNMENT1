import os
import zipfile
import json
import pandas as pd
import pyarrow.parquet as pq
from typing import Iterable, Dict, Optional

def load_news_data(folder_path: str, limit: Optional[int] = None) -> Iterable[Dict]:
    doc_id_counter = 0
    print(f"Loading News data from: {folder_path}")
    if not os.path.isdir(folder_path):
        print(f"Warning: News data directory not found at {folder_path}")
        return

    for filename in os.listdir(folder_path):
        if filename.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(folder_path, filename), 'r') as zf:
                for json_file in zf.namelist():
                    with zf.open(json_file) as f:
                        try:
                            data = json.load(f)
                            if data.get('language', '').lower() == 'english' and data.get('text'):
                                yield {
                                    "doc_id": f"news_{doc_id_counter}",
                                    "original_id": data.get('uuid'),
                                    "content": data.get('text', ''),
                                    "title": data.get('title', ''),
                                    "author": data.get('author', ''),
                                    "published_date": data.get('published', None),
                                    "source": "news"
                                }
                                doc_id_counter += 1
                                if limit and doc_id_counter >= limit:
                                    print(f"\nReached news document limit of {limit}.")
                                    return
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass
        if limit and doc_id_counter >= limit:
            break

def load_wiki_data(folder_path: str, limit: Optional[int] = None) -> Iterable[Dict]:
    doc_id_counter = 0
    print(f"Loading Wikipedia data from: {folder_path}")
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Error: Directory not found at {folder_path}")
        
    parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
    for file_path in parquet_files:
        df = pq.read_table(file_path).to_pandas()
        for _, row in df.iterrows():
            yield {
                "doc_id": f"wiki_{row['id']}",
                "original_id": row['id'],
                "content": row['text'],
                "title": row['title'],
                "url": row['url'],
                "source": "wikipedia"
            }
            doc_id_counter += 1
            if limit and doc_id_counter >= limit:
                print(f"\nReached wiki document limit of {limit}.")
                return
        if limit and doc_id_counter >= limit:
            break