from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from .index_base import IndexBase
from typing import Iterable, Dict

class ESIndexer(IndexBase):
    def __init__(self, es_client: Elasticsearch):
        self.es = es_client
        super().__init__(core='ESIndex', info='TFIDF', dstore='DB2', qproc='TERMatat', compr='NONE', optim='Null')

    def create_index(self, index_id: str, documents: Iterable[Dict]):
        index_body = {
            "mappings": {
                "properties": {
                    "original_id": {"type": "keyword"},
                    "content": {"type": "text", "analyzer": "my_english_analyzer"},
                    "source": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "my_english_analyzer"},
                    "url": {"type": "keyword"},
                    "author": {"type": "text"},
                    "published_date": {"type": "date"}
                }
            },
            "settings": { "analysis": { "analyzer": { "my_english_analyzer": { "type": "custom", "tokenizer": "standard", "filter": ["lowercase", "english_stop", "english_stemmer"] } }, "filter": { "english_stop": {"type": "stop", "stopwords": "_english_"}, "english_stemmer": {"type": "stemmer", "language": "english"} } } }
        }

        if self.es.indices.exists(index=index_id):
            self.es.indices.delete(index=index_id)
            print(f"Deleted existing index: {index_id}")

        self.es.indices.create(index=index_id, body=index_body)
        print(f"Created final index '{index_id}' with correct mapping.")

        def doc_generator():
            for doc in documents:
                doc_id = doc.pop("doc_id")
                yield {"_index": index_id, "_id": doc_id, "_source": doc}

        print("Starting final bulk indexing...")
        success, failed = bulk(self.es, doc_generator(), chunk_size=500, request_timeout=120)
        print(f"Successfully indexed {success} documents.")
        if failed: print(f"Failed to index {len(failed)} documents.")

    def query(self, index_id: str, query_text: str, fields: list = ["content", "title"]):
        """Performs a multi-field keyword search and returns the results."""
        if not self.es.indices.exists(index=index_id):
            print(f"Index '{index_id}' does not exist.")
            return [] 

        query_body = { "query": { "multi_match": { "query": query_text, "fields": fields } } }
        
        try:
            response = self.es.search(index=index_id, body=query_body)
            return response['hits']['hits']
        except Exception as e:
            print(f"An error occurred during search: {e}")
            return []

    def get_memory_footprint(self, index_id: str) -> str:
        """Gets the disk usage for a specific index."""
        try:
            stats = self.es.cat.indices(index=index_id, format="json", h="store.size")
            if stats:
                return stats[0]['store.size']
            return "N/A"
        except Exception as e:
            print(f"Could not retrieve memory footprint: {e}")
            return "Error"

    # --- Placeholder methods from index_base.py ---
    def load_index(self, serialized_index_dump: str): pass
    def update_index(self, index_id: str, remove_files: Iterable[Dict], add_files: Iterable[Dict]): pass
    def delete_index(self, index_id: str): pass
    def list_indices(self) -> Iterable[str]: pass
    def list_indexed_files(self, index_id: str) -> Iterable[str]: pass