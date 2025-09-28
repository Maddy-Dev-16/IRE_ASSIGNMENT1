# main.py
from elasticsearch import Elasticsearch
from src.data_loader import load_wiki_data, load_news_data
from src.es_indexer import ESIndexer

# This function definition must start at the far left of the file (no indent)
def run_indexing_phase():
    """Runs the entire process for indexing data into Elasticsearch."""
    # Code inside the function is indented
    print("--- Starting Elasticsearch Indexing Phase ---")

    # 1. Connect to Elasticsearch
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
    
    # --- Step 1: Create the index blueprint (will only happen once) ---
    es_indexer.create_index(index_id=index_name)
    
    # --- Step 2: Load and index Wikipedia data ---
    # NOTE: Since you already indexed this, you can comment out these two lines for now
    # print("\n--- Indexing Wikipedia Data ---")
    # wiki_documents = load_wiki_data("data/wiki/")
    # es_indexer.add_documents(index_id=index_name, documents=wiki_documents)
    
    # --- Step 3: Load and index News data ---
    print("\n--- Indexing News Data ---")
    news_documents = load_news_data("data/News_Datasets/") # Make sure your news zips are in data/news/
    es_indexer.add_documents(index_id=index_name, documents=news_documents)
    
    print("\n--- All Indexing Complete ---")

# This "if" statement must also start at the far left of the file (no indent)
if __name__ == "__main__":
    # The call to the function is indented inside the "if" block
    run_indexing_phase()