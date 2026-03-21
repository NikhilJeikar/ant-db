from exceptions import APIError


def validate_status(resp):
    if not resp.ok:
        raise APIError(resp.text, resp.status_code)


def validate_keys(data, keys):
    for key in keys:
        if key not in data:
            raise APIError(f"Missing key: {key}")


def validate_create_table(data):
    validate_keys(data, ["table_id", "name", "message"])


def validate_create_column(data):
    validate_keys(data, ["column_id", "column_name", "table_id"])


def validate_insert(data):
    validate_keys(data, ["row_count", "table_id"])


def validate_search(data):
    validate_keys(data, ["table_id", "row_count", "rows"])


def validate_schema(data):
    validate_keys(data, ["table_id", "table_name", "columns"])