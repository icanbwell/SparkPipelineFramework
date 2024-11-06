from dataclasses import dataclass
from typing import List


@dataclass
class ChunkContainer[TInputColumnDataType]:
    """
    This class is used to store the chunk of input values that will be passed to the Pandas UDF
    """

    """ The input values for the chunk"""
    chunk_input_values: List[TInputColumnDataType]

    """ The index of the chunk"""
    chunk_index: int

    """ The index of the first input value in the chunk"""
    begin_chunk_input_values_index: int

    """ The index of the last input value in the chunk"""
    end_chunk_input_values_index: int
