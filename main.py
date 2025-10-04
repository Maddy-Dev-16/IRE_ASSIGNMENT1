import itertools
from elasticsearch import Elasticsearch
from src.data_loader import load_wiki_data, load_news_data
from src.es_indexer import ESIndexer

def run_final_indexing():
    """
    Creates the final ESIndex-v1.0 by indexing a large sample
    of both Wikipedia and News data.
    """
    try:
        es_client = Elasticsearch("http://localhost:9200")
        if not es_client.ping():
            raise ConnectionError("Could not connect to Elasticsearch.")
        print("Successfully connected to local Elasticsearch instance!")
    except Exception as e:
        print(f"Connection failed: {e}")
        print("Please ensure your local Elasticsearch instance is running.")
        return
    
    es_indexer = ESIndexer(es_client)
    index_name = "esindex-v1.0"
    
    # --- Load 100,000 documents from EACH dataset ---
    print("\n--- Loading All Datasets (Sample) ---")
    wiki_documents = load_wiki_data("data/wiki/", limit=100000)
    news_documents = load_news_data("data/News_Datasets/", limit=100000)
    
    # Combine them into a single stream
    all_documents = itertools.chain(wiki_documents, news_documents)
    
    # This will delete any old index and create the final, correct one
    es_indexer.create_index(index_id=index_name, documents=all_documents)
    
    print("\n--- Final ESIndex-v1.0 Build Complete (with sample data) ---")

if __name__ == "__main__":
    run_final_indexing()