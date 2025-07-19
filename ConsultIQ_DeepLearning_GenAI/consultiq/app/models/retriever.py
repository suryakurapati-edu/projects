import faiss
import os
import numpy as np
import pickle

class Retriever:
    def __init__(self, index_path='vectorstore/faiss_index.bin'):
        self.index_path = index_path
        self.index = None
        self.docs = []

    def build_index(self, embeddings, documents):
        dim = embeddings[0].shape[0]
        self.index = faiss.IndexFlatL2(dim)
        self.index.add(np.array(embeddings))
        self.docs = documents
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        with open(self.index_path.replace(".bin", "_docs.pkl"), "wb") as f:
            pickle.dump(self.docs, f)
        faiss.write_index(self.index, self.index_path)

    def load_index(self):
        if os.path.exists(self.index_path):
            try:
                self.index = faiss.read_index(self.index_path)
                with open(self.index_path.replace(".bin", "_docs.pkl"), "rb") as f:
                    self.docs = pickle.load(f)
                print("FAISS index and documents loaded.")
            except Exception as e:
                print(f"Failed to load FAISS index: {e}")
                self.index = None
                self.docs = []
        else:
            print("FAISS index path does not exist.")
            self.index = None
            self.docs = []


    def search(self, query_embedding, top_k=1):
        if self.index is None:
            raise ValueError("FAISS index not loaded. Please build or load the index first.")
        D, I = self.index.search(np.array([query_embedding]), top_k)
        return [self.docs[i] for i in I[0]]
