# src/es_indexer.py
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from .index_base import IndexBase
from typing import Iterable, Dict

class ESIndexer(IndexBase):
    def __init__(self, es_client: Elasticsearch):
        self.es = es_client
        super().__init__(core='ESIndex', info='TFIDF', dstore='DB2', qproc='TERMatat', compr='NONE', optim='Null')

    def create_index(self, index_id: str):
        """Creates the ES index with the mapping, but ONLY if it doesn't already exist."""
        index_body = {
            "settings": { "analysis": { "analyzer": { "my_english_analyzer": { "type": "custom", "tokenizer": "standard", "filter": ["lowercase", "english_stop", "english_stemmer"] } }, "filter": { "english_stop": {"type": "stop", "stopwords": "_english_"}, "english_stemmer": {"type": "stemmer", "language": "english"} } } },
            "mappings": { "properties": { "content": {"type": "text", "analyzer": "my_english_analyzer"}, "source": {"type": "keyword"}, "title": {"type": "text", "analyzer": "my_english_analyzer"}, "url": {"type": "keyword"}, "author": {"type": "text"}, "published_date": {"type": "date"} } }
        }
        
        if not self.es.indices.exists(index=index_id):
            self.es.indices.create(index=index_id, body=index_body)
            print(f"Created new index '{index_id}' with combined mapping.")
        else:
            print(f"Index '{index_id}' already exists.")

    def add_documents(self, index_id: str, documents: Iterable[Dict]):
        """Adds a batch of documents to an existing index without deleting it."""
        def doc_generator():
            for doc in documents:
                doc_id = doc.pop("doc_id") 
                yield {"_index": index_id, "_id": doc_id, "_source": doc}
        
        print(f"Starting bulk indexing for index '{index_id}'...")
        success, failed = bulk(self.es, doc_generator(), chunk_size=500, request_timeout=120)
        print(f"Successfully indexed {success} documents.")
        if failed: print(f"Failed to index {len(failed)} documents.")
            
    # --- Placeholder methods required by the base class ---
    def load_index(self, serialized_index_dump: str): pass
    def update_index(self, index_id: str, remove_files: Iterable[Dict], add_files: Iterable[Dict]): pass
    def query(self, query: str) -> str: pass
    def delete_index(self, index_id: str): pass
    def list_indices(self) -> Iterable[str]: pass
    def list_indexed_files(self, index_id: str) -> Iterable[str]: pass