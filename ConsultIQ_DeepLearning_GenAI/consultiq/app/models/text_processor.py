from typing import List
from langchain.text_splitter import CharacterTextSplitter

class TextProcessor:
    def __init__(self, chunk_size=1000, chunk_overlap=200):
        self.splitter = CharacterTextSplitter(separator="\n", chunk_size=chunk_size,
                                              chunk_overlap=chunk_overlap, length_function=len)

    def split_text(self, text: str) -> List[str]:
        return self.splitter.split_text(text)
