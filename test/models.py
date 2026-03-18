from typing import List, Optional, Any, Dict


from enum import Enum


class DataType(str, Enum):
    IntegerU8 = "IntegerU8"
    IntegerU16 = "IntegerU16"
    IntegerU32 = "IntegerU32"
    IntegerU64 = "IntegerU64"
    IntegerU128 = "IntegerU128"

    IntegerI8 = "IntegerI8"
    IntegerI16 = "IntegerI16"
    IntegerI32 = "IntegerI32"
    IntegerI64 = "IntegerI64"
    IntegerI128 = "IntegerI128"

    FloatF32 = "FloatF32"
    FloatF64 = "FloatF64"

    String = "String"
    Boolean = "Boolean"
    Bytes = "Bytes"

class Cell:
    @staticmethod
    def string(column_id: int, value: str):
        return {
            "column_id": column_id,
            "data": {"String": value}
        }

    @staticmethod
    def int32(column_id: int, value: int):
        return {
            "column_id": column_id,
            "data": {"IntegerI32": value}
        }

    @staticmethod
    def int64(column_id: int, value: int):
        return {
            "column_id": column_id,
            "data": {"IntegerI64": value}
        }

    @staticmethod
    def float64(column_id: int, value: float):
        return {
            "column_id": column_id,
            "data": {"FloatF64": value}
        }

    @staticmethod
    def boolean(column_id: int, value: bool):
        return {
            "column_id": column_id,
            "data": {"Boolean": value}
        }
def create_table_payload(name: str) -> Dict:
    return {"name": name}


def create_column_payload(column_name: str, data_type: DataType, constraints=None):
    return {
        "column_name": column_name,
        "data_type": data_type.value,
        "constraints": constraints or []
    }

def insert_rows_payload(rows: List[List[Any]]):
    return {"rows": rows}


def delete_rows_payload(row_ids: List[int]):
    return {"row_ids": row_ids}


def update_rows_payload(row_ids: List[int], new_values: List[Any]):
    return {
        "row_ids": row_ids,
        "new_values": new_values
    }

def get_rows_payload(row_ids: List[int]):
    return {"row_ids": row_ids}


def search_payload(criteria, projection=None, sort_by=None):
    return {
        "criteria": criteria,
        "projection": projection,
        "sort_by": sort_by
    }
