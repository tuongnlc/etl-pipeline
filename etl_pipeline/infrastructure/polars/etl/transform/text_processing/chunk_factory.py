from langchain_text_splitters import RecursiveCharacterTextSplitter
from typing import Any



class RecursiveCharacterChunker:
    def __init__(
        self,
        chunk_size: int,
        chunk_overlap: int,
        length_function: Any,
    ) -> None:
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.length_function = length_function
        

    def text_splitter(self):
        return RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,       
            chunk_overlap=self.chunk_overlap,     
            length_function=self.length_function, 
        )


MAPPING_SPLITTER = {
    "recursive_character": RecursiveCharacterChunker,
}


def text_splitter_factory(
    spliiter_type: str,
    chunk_size: int = None,
    chunk_overlap: int = None,
) -> Any:
    if spliiter_type == "recursive_character":
        return RecursiveCharacterChunker(
            chunk_size=chunk_size,       
            chunk_overlap=chunk_overlap,     
            length_function=len, 
        ).text_splitter()
    else:
        raise ValueError(f"Unknown splitter type: {spliiter_type}")