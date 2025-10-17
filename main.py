# main.py
import argparse
import itertools
from elasticsearch import Elasticsearch

# Import all our modules
from src.data_loader import load_wiki_data, load_news_data
from src.es_indexer import ESIndexer
from src.self_indexer import SelfIndexer

def run_es_indexing(limit_docs=None):
    """Builds the ESIndex using a sample of data."""
    print("--- Starting Elasticsearch Indexing Phase ---")
    
    # --- This function builds the Elasticsearch index ---
    es_client = Elasticsearch("http://localhost:9200")
    if not es_client.ping():
        raise ConnectionError("Could not connect to Elasticsearch.")
        
    es_indexer = ESIndexer(es_client)
    
    print(f"\n--- Loading datasets (limit: {limit_docs} per source) ---")
    wiki_docs = load_wiki_data("data/wiki/", limit=limit_docs)
    news_docs = load_news_data("data/News_Datasets/", limit=limit_docs) # <-- CORRECTED PATH
    all_documents = itertools.chain(wiki_docs, news_docs)
    
    es_indexer.create_index(index_id="esindex-v1.0", documents=all_documents)
    print("\n--- ESIndex Build Complete ---")

def run_self_indexing(limit_docs=None):
    """Builds the SelfIndex using a sample of data."""
    print("--- Starting Self-Indexing Phase ---")
    
    self_indexer = SelfIndexer()
    
    print(f"\n--- Loading datasets (limit: {limit_docs} per source) ---")
    wiki_docs = load_wiki_data("data/wiki/", limit=limit_docs)
    news_docs = load_news_data("data/News_Datasets/", limit=limit_docs) # Correct Path
    all_documents = itertools.chain(wiki_docs, news_docs)
    
    self_indexer.create_index(index_id=self_indexer.identifier_short, documents=all_documents)
    print("\n--- SelfIndex Build Complete ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build an index.")
    parser.add_argument("--indexer", type=str, choices=['es', 'self'], required=True,
                        help="Specify which indexer to run: 'es' or 'self'.")
    args = parser.parse_args()

    DOC_LIMIT = 100000

    if args.indexer == 'self':
        run_self_indexing(limit_docs=DOC_LIMIT)
    elif args.indexer == 'es':
        # run_es_indexing(limit_docs=DOC_LIMIT) # We can add this back later
        print("ES indexing is disabled in this run. Use --indexer self.")