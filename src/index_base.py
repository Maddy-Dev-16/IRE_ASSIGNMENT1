# src/index_base.py

from abc import ABC, abstractmethod
from typing import Iterable, Dict
from enum import Enum

# Identifier enums for variants for index
# These are defined in your README.md
class IndexInfo(Enum):
    BOOLEAN = 1
    WORDCOUNT = 2
    TFIDF = 3

class DataStore(Enum):
    CUSTOM = 1
    DB1 = 2
    DB2 = 3

class Compression(Enum):
    NONE = 1
    CODE = 2
    CLIB = 3

class QueryProc(Enum):
    TERMatat = 'T'
    DOCatat = 'D'

class Optimizations(Enum):
    Null = '0'
    Skipping = 'sp'
    Thresholding = 'th'
    EarlyStopping = 'es'
  
class IndexBase(ABC):
    """
    Base index class with abstract methods to inherit for specific implementations.
    """
    def __init__(self, core, info, dstore, qproc, compr, optim):
      assert core in ('ESIndex', 'SelfIndex')
      long = [ IndexInfo[info], DataStore[dstore], Compression[compr], QueryProc[qproc], Optimizations[optim] ]
      short = [k.value for k in long]
      self.identifier_long = "core={}|index={}|datastore={}|compressor={}|qproc={}|optim={}".format(*[core]+[e.name for e in long])
      self.identifier_short = "{}_i{}d{}c{}q{}o{}".format(*[core]+short)
        
    def __repr__(self):
        return f"{self.identifier_short}: {self.identifier_long}"
      
    @abstractmethod
    def create_index(self, index_id: str, documents: Iterable[Dict]) -> None: 
        """Creates an index for the given documents."""
        pass
            
    @abstractmethod
    def load_index(self, serialized_index_dump: str) -> None:
        """Loads an already created index into memory from disk."""
        pass
        
    @abstractmethod
    def update_index(self, index_id: str, remove_docs: Iterable[Dict], add_docs: Iterable[Dict]) -> None:
        """Updates an index."""
        pass

    @abstractmethod
    def query(self, query: str) -> str:
        """Queries the already loaded index."""
        pass
  
    @abstractmethod
    def delete_index(self, index_id: str) -> None:
        """Deletes the index."""
        pass
  
    @abstractmethod
    def list_indices(self) -> Iterable[str]:
        """Lists all indices."""
        pass
  
    @abstractmethod
    def list_indexed_files(self, index_id: str) -> Iterable[str]:
        """Lists all files indexed in the given index."""
        pass