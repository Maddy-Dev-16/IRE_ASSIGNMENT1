import json
from collections import defaultdict
from .index_base import IndexBase
from .preprocessor import preprocess_text
from typing import Iterable, Dict

class SelfIndexer(IndexBase):
    def __init__(self):
        super().__init__(core='SelfIndex', info='BOOLEAN', dstore='CUSTOM', 
                         qproc='TERMatat', compr='NONE', optim='Null')
        self.inverted_index = defaultdict(list)
        self.documents = {}

    def create_index(self, index_id: str, documents: Iterable[Dict]):
        print(f"--- Building '{self.identifier_short}' from scratch ---")
        
        doc_count = 0
        for doc in documents:
            doc_id = doc['doc_id']
            content = doc.get('content', '')
            self.documents[doc_id] = {'title': doc.get('title', 'No Title')}
            
            tokens = preprocess_text(content)
            
            term_positions = defaultdict(list)
            for i, token in enumerate(tokens):
                term_positions[token].append(i)
            
            for term, positions in term_positions.items():
                self.inverted_index[term].append([doc_id, positions])
            doc_count += 1

        print(f"Index building complete. Processed {doc_count} documents.")
        print(f"Total unique terms: {len(self.inverted_index)}")
        self._save_index(index_id)

    def _save_index(self, index_id: str):
        filename = f"{self.identifier_short}.json"
        index_data = {
            "identifier": self.identifier_short,
            "inverted_index": self.inverted_index,
            "documents": self.documents
        }
        with open(filename, 'w') as f:
            json.dump(index_data, f)
        print(f"Index saved to disk as {filename}")

    def load_index(self, index_id: str):
        filename = f"{self.identifier_short}.json"
        print(f"--- Loading index from {filename} ---")
        try:
            with open(filename, 'r') as f:
                index_data = json.load(f)
            self.inverted_index = defaultdict(list, index_data["inverted_index"])
            self.documents = index_data["documents"]
            print(f"Index loaded successfully. Found {len(self.inverted_index)} terms.")
        except FileNotFoundError:
            print(f"Error: Index file {filename} not found. Please create it first.")

    # --- Placeholder for the next step ---
    def query(self, query: str) -> str: pass
    
    # --- Other required placeholder methods ---
    def update_index(self, index_id: str, remove_docs: Iterable[Dict], add_docs: Iterable[Dict]): pass
    def delete_index(self, index_id: str): pass
    def list_indices(self) -> Iterable[str]: pass
    def list_indexed_files(self, index_id: str) -> Iterable[str]: pass