import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from client import DatabaseClient
from models import (
    Cell,
    DataType,
    create_column_payload,
    delete_rows_payload,
    insert_rows_payload,
    search_payload,
    update_rows_payload,
)


MIN_ROWS_FLOOR = 1_000_000
MIN_BENCHMARK_COLUMNS = 6


@dataclass(frozen=True)
class Config:
    base_url: str
    min_table_rows: int
    test_insert_batch_size: int
    test_progress_every: int
    parallel_single_workers: int
    parallel_single_rows_per_worker: int
    parallel_multi_tables: int
    parallel_multi_rows_per_table: int
    run_benchmark: bool
    run_full_operation_benchmark: bool
    benchmark_total_rows: int
    benchmark_batch_size: int
    benchmark_single_workers: int
    benchmark_multi_tables: int
    benchmark_multi_rows_per_table: int
    benchmark_search_iterations: int
    benchmark_columns: int
    benchmark_index_columns: list[int]
    benchmark_search_unique_column: int
    benchmark_search_group_column: int


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return parsed


def parse_column_list(raw_value):
    try:
        columns = [int(part.strip()) for part in raw_value.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("comma-separated integers expected") from exc
    if not columns:
        raise argparse.ArgumentTypeError("at least one column id is required")
    return list(dict.fromkeys(columns))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Integration and benchmark test harness for the database API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--min-table-rows", type=positive_int, default=MIN_ROWS_FLOOR)
    parser.add_argument("--test-insert-batch-size", type=positive_int, default=200_000)
    parser.add_argument("--test-progress-every", type=positive_int, default=200_000)
    parser.add_argument("--parallel-single-workers", type=positive_int, default=4)
    parser.add_argument("--parallel-single-rows-per-worker", type=positive_int, default=100)
    parser.add_argument("--parallel-multi-tables", type=positive_int, default=2)
    parser.add_argument("--parallel-multi-rows-per-table", type=positive_int, default=100)

    parser.add_argument("--run-benchmark", action="store_true")
    parser.add_argument("--run-full-operation-benchmark", action="store_true")
    parser.add_argument("--benchmark-total-rows", type=positive_int, default=MIN_ROWS_FLOOR)
    parser.add_argument("--benchmark-batch-size", type=positive_int, default=200_000)
    parser.add_argument("--benchmark-single-workers", type=positive_int, default=4)
    parser.add_argument("--benchmark-multi-tables", type=positive_int, default=2)
    parser.add_argument(
        "--benchmark-multi-rows-per-table",
        type=positive_int,
        default=MIN_ROWS_FLOOR,
    )
    parser.add_argument("--benchmark-search-iterations", type=positive_int, default=20)
    parser.add_argument("--benchmark-columns", type=positive_int, default=MIN_BENCHMARK_COLUMNS)
    parser.add_argument("--benchmark-index-columns", default="1,2,5")
    parser.add_argument("--benchmark-search-unique-column", type=positive_int, default=1)
    parser.add_argument("--benchmark-search-group-column", type=positive_int, default=2)

    args = parser.parse_args()

    benchmark_columns = max(MIN_BENCHMARK_COLUMNS, args.benchmark_columns)
    benchmark_index_columns = parse_column_list(args.benchmark_index_columns)

    for column_id in benchmark_index_columns:
        if column_id < 0 or column_id >= benchmark_columns:
            parser.error(
                f"--benchmark-index-columns contains {column_id}, "
                f"but benchmark columns are 0..{benchmark_columns - 1}"
            )

    for option_name, column_id in [
        ("--benchmark-search-unique-column", args.benchmark_search_unique_column),
        ("--benchmark-search-group-column", args.benchmark_search_group_column),
    ]:
        if column_id < 0 or column_id >= benchmark_columns:
            parser.error(
                f"{option_name} must be between 0 and {benchmark_columns - 1}"
            )

    return Config(
        base_url=args.base_url,
        min_table_rows=max(MIN_ROWS_FLOOR, args.min_table_rows),
        test_insert_batch_size=args.test_insert_batch_size,
        test_progress_every=args.test_progress_every,
        parallel_single_workers=args.parallel_single_workers,
        parallel_single_rows_per_worker=args.parallel_single_rows_per_worker,
        parallel_multi_tables=args.parallel_multi_tables,
        parallel_multi_rows_per_table=args.parallel_multi_rows_per_table,
        run_benchmark=args.run_benchmark,
        run_full_operation_benchmark=args.run_full_operation_benchmark,
        benchmark_total_rows=max(MIN_ROWS_FLOOR, args.benchmark_total_rows),
        benchmark_batch_size=args.benchmark_batch_size,
        benchmark_single_workers=args.benchmark_single_workers,
        benchmark_multi_tables=args.benchmark_multi_tables,
        benchmark_multi_rows_per_table=max(
            MIN_ROWS_FLOOR, args.benchmark_multi_rows_per_table
        ),
        benchmark_search_iterations=args.benchmark_search_iterations,
        benchmark_columns=benchmark_columns,
        benchmark_index_columns=benchmark_index_columns,
        benchmark_search_unique_column=args.benchmark_search_unique_column,
        benchmark_search_group_column=args.benchmark_search_group_column,
    )


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def cell_signature(cell):
    data = cell["data"]
    assert_true(isinstance(data, dict) and len(data) == 1, f"Invalid cell payload: {cell!r}")
    data_type, value = next(iter(data.items()))
    return (cell["column_id"], data_type, json.dumps(value, sort_keys=True))


def row_signature(cells):
    return tuple(
        cell_signature(cell)
        for cell in sorted(cells, key=lambda item: item["column_id"])
    )


def cell_value(cell):
    _, value = next(iter(cell["data"].items()))
    return value


def cells_by_column(cells):
    return {cell["column_id"]: cell_value(cell) for cell in cells}


def assert_get_rows(response, expected_rows_by_id, message):
    assert_equal(response["row_count"], len(expected_rows_by_id), f"{message} row count")
    actual_rows = {row["row_id"]: row_signature(row["cells"]) for row in response["rows"]}
    expected_rows = {
        row_id: row_signature(cells)
        for row_id, cells in expected_rows_by_id.items()
    }
    assert_equal(actual_rows, expected_rows, message)


def assert_search_rows(response, expected_rows, message, ordered=False):
    assert_equal(response["row_count"], len(expected_rows), f"{message} row count")
    actual_rows = [row_signature(row) for row in response["rows"]]
    expected_rows = [row_signature(row) for row in expected_rows]

    if not ordered:
        actual_rows = sorted(actual_rows)
        expected_rows = sorted(expected_rows)

    assert_equal(actual_rows, expected_rows, message)


def string_value(value):
    return {"String": value}


def int32_value(value):
    return {"IntegerI32": value}


def criterion(column_id, operator, value):
    return {
        "column_id": column_id,
        "operator": operator,
        "value": value,
    }


def sort_by(column_id, order_by):
    return {
        "column_id": column_id,
        "order_by": order_by,
    }


def make_small_row(name, age):
    return [Cell.string(0, name), Cell.int32(1, age)]


def make_wide_row(row_index, prefix, column_count):
    row = [Cell.string(0, f"{prefix}_key_{row_index}")]

    for column_id in range(1, column_count):
        if column_id == 1:
            value = row_index
        elif column_id == 2:
            value = row_index % 1000
        else:
            value = (row_index * 97 + column_id * 13) % 1_000_000_000
        row.append(Cell.int32(column_id, value))

    return row


def unique_table_name(prefix):
    return f"{prefix}_{time.time_ns()}"


def print_progress(label, inserted, total_rows, progress_every):
    if inserted == total_rows or inserted % progress_every == 0:
        print(f"{label}: inserted {inserted}/{total_rows}")


def format_throughput(unit_count, duration_secs):
    return int(unit_count / max(duration_secs, 1e-9))


def run_parallel(label, items, worker_fn, max_workers):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_fn, item): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                raise AssertionError(f"{label} failed for {item!r}: {exc}") from exc
    return results


def basic_schema_specs():
    return [
        ("name", DataType.String),
        ("age", DataType.IntegerI32),
    ]


def wide_schema_specs(column_count):
    specs = [("key", DataType.String)]
    for column_id in range(1, column_count):
        specs.append((f"metric_{column_id:02d}", DataType.IntegerI32))
    return specs


def create_schema(client, table_name, column_specs):
    table_id = client.create_table(table_name)["table_id"]
    for column_name, data_type in column_specs:
        client.create_column(table_id, create_column_payload(column_name, data_type))
    return table_id


def create_indexes(client, table_id, column_ids):
    for column_id in column_ids:
        client.create_index(table_id, column_id)


def drop_indexes(client, table_id, column_ids):
    for column_id in column_ids:
        client.drop_index(table_id, column_id)


def bulk_insert_rows(client, table_id, rows, label):
    if not rows:
        return 0.0
    start = time.time()
    response = client.insert_rows(table_id, insert_rows_payload(rows))
    assert_equal(response["row_count"], len(rows), f"{label} insert row_count")
    return time.time() - start


def bulk_insert_generated_rows(
    client,
    table_id,
    total_rows,
    row_factory,
    label,
    batch_size,
    progress_every,
    show_progress=True,
):
    inserted = 0
    start_time = time.time()

    while inserted < total_rows:
        current_batch_size = min(batch_size, total_rows - inserted)
        rows = [row_factory(inserted + i) for i in range(current_batch_size)]
        response = client.insert_rows(table_id, insert_rows_payload(rows))
        assert_equal(
            response["row_count"],
            current_batch_size,
            f"{label} batch row_count at offset {inserted}",
        )
        inserted += current_batch_size
        if show_progress:
            print_progress(label, inserted, total_rows, progress_every)

    return time.time() - start_time


def expected_mod_count(total_rows, modulus, target_value):
    whole = total_rows // modulus
    remainder = total_rows % modulus
    return whole + (1 if target_value < remainder else 0)


def measure_index_creation(client, table_id, column_id):
    start = time.time()
    client.create_index(table_id, column_id)
    return time.time() - start


def measure_index_drop(client, table_id, column_id):
    start = time.time()
    client.drop_index(table_id, column_id)
    return time.time() - start


def measure_search(client, table_id, payload, iterations, expected_row_count, label):
    first_result = client.search(table_id, payload)
    assert_equal(first_result["row_count"], expected_row_count, f"{label} warmup count")

    start = time.time()
    last_result = first_result
    for _ in range(iterations):
        last_result = client.search(table_id, payload)
    duration = time.time() - start

    assert_equal(last_result["row_count"], expected_row_count, f"{label} count")
    return {
        "duration_secs": duration,
        "avg_ms": (duration * 1000.0) / iterations,
        "queries_per_sec": format_throughput(iterations, duration),
    }


def print_load_benchmark_result(name, row_count, duration_secs):
    print(
        f"{name}: {row_count} rows in {round(duration_secs, 2)}s "
        f"({format_throughput(row_count, duration_secs)} rows/sec)"
    )


def benchmark_serial_load(config):
    client = DatabaseClient(base_url=config.base_url)
    table_name = unique_table_name("serial_load_bench")
    table_id = None

    try:
        table_id = create_schema(client, table_name, wide_schema_specs(config.benchmark_columns))
        print(
            f"\nSerial load benchmark | table={table_name} rows={config.benchmark_total_rows} "
            f"batch={config.benchmark_batch_size} columns={config.benchmark_columns}"
        )
        duration = bulk_insert_generated_rows(
            client=client,
            table_id=table_id,
            total_rows=config.benchmark_total_rows,
            row_factory=lambda i: make_wide_row(i, table_name, config.benchmark_columns),
            label="serial load",
            batch_size=config.benchmark_batch_size,
            progress_every=config.test_progress_every,
            show_progress=True,
        )
        size = client.get_size(table_id)
        assert_equal(size["row_count"], config.benchmark_total_rows, "Serial load final size")
        print_load_benchmark_result(
            "Serial single-table load", config.benchmark_total_rows, duration
        )
        return {
            "name": "serial_single_table",
            "row_count": config.benchmark_total_rows,
            "duration_secs": duration,
            "throughput_rows_per_sec": format_throughput(
                config.benchmark_total_rows, duration
            ),
        }
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def benchmark_parallel_single_table_load(config):
    client = DatabaseClient(base_url=config.base_url)
    table_name = unique_table_name("parallel_single_load_bench")
    table_id = None
    worker_count = max(1, min(config.benchmark_single_workers, config.benchmark_total_rows))

    try:
        table_id = create_schema(client, table_name, wide_schema_specs(config.benchmark_columns))
        print(
            f"\nParallel single-table load benchmark | table={table_name} "
            f"rows={config.benchmark_total_rows} batch={config.benchmark_batch_size} "
            f"workers={worker_count} columns={config.benchmark_columns}"
        )

        base_rows = config.benchmark_total_rows // worker_count
        remainder = config.benchmark_total_rows % worker_count
        worker_specs = []
        offset = 0

        for worker in range(worker_count):
            worker_rows = base_rows + (1 if worker < remainder else 0)
            worker_specs.append((worker, offset, worker_rows))
            offset += worker_rows

        def worker_fn(spec):
            worker, start_offset, worker_rows = spec
            local_client = DatabaseClient(base_url=config.base_url)
            duration = bulk_insert_generated_rows(
                client=local_client,
                table_id=table_id,
                total_rows=worker_rows,
                row_factory=lambda i: make_wide_row(
                    start_offset + i, table_name, config.benchmark_columns
                ),
                label=f"parallel single worker {worker}",
                batch_size=config.benchmark_batch_size,
                progress_every=config.test_progress_every,
                show_progress=False,
            )
            return {
                "worker": worker,
                "rows": worker_rows,
                "duration_secs": duration,
            }

        wall_start = time.time()
        worker_results = run_parallel(
            "parallel single-table load",
            worker_specs,
            worker_fn,
            max_workers=worker_count,
        )
        wall_duration = time.time() - wall_start

        size = client.get_size(table_id)
        assert_equal(
            size["row_count"],
            config.benchmark_total_rows,
            "Parallel single-table final size",
        )

        print_load_benchmark_result(
            "Parallel single-table load", config.benchmark_total_rows, wall_duration
        )
        for result in sorted(worker_results, key=lambda item: item["worker"]):
            print(
                f"  worker {result['worker']}: {result['rows']} rows in "
                f"{round(result['duration_secs'], 2)}s"
            )

        return {
            "name": "parallel_single_table",
            "row_count": config.benchmark_total_rows,
            "duration_secs": wall_duration,
            "throughput_rows_per_sec": format_throughput(
                config.benchmark_total_rows, wall_duration
            ),
        }
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def benchmark_parallel_multi_table_load(config):
    client = DatabaseClient(base_url=config.base_url)
    table_specs = []

    try:
        for table_index in range(config.benchmark_multi_tables):
            table_name = unique_table_name(f"parallel_multi_load_{table_index}")
            table_id = create_schema(
                client,
                table_name,
                wide_schema_specs(config.benchmark_columns),
            )
            table_specs.append((table_index, table_id, table_name))

        print(
            f"\nParallel multi-table load benchmark | tables={config.benchmark_multi_tables} "
            f"rows_per_table={config.benchmark_multi_rows_per_table} "
            f"batch={config.benchmark_batch_size} columns={config.benchmark_columns}"
        )

        def worker_fn(spec):
            worker, table_id, table_name = spec
            local_client = DatabaseClient(base_url=config.base_url)
            duration = bulk_insert_generated_rows(
                client=local_client,
                table_id=table_id,
                total_rows=config.benchmark_multi_rows_per_table,
                row_factory=lambda i: make_wide_row(i, table_name, config.benchmark_columns),
                label=f"parallel multi table {worker}",
                batch_size=config.benchmark_batch_size,
                progress_every=config.test_progress_every,
                show_progress=False,
            )
            size = local_client.get_size(table_id)
            assert_equal(
                size["row_count"],
                config.benchmark_multi_rows_per_table,
                f"Parallel multi-table final size for {table_name}",
            )
            return {
                "worker": worker,
                "table_name": table_name,
                "rows": config.benchmark_multi_rows_per_table,
                "duration_secs": duration,
            }

        wall_start = time.time()
        worker_results = run_parallel(
            "parallel multi-table load",
            table_specs,
            worker_fn,
            max_workers=config.benchmark_multi_tables,
        )
        wall_duration = time.time() - wall_start

        aggregate_rows = (
            config.benchmark_multi_rows_per_table * config.benchmark_multi_tables
        )
        print_load_benchmark_result(
            "Parallel multi-table aggregate load", aggregate_rows, wall_duration
        )
        for result in sorted(worker_results, key=lambda item: item["worker"]):
            print(
                f"  table {result['table_name']}: {result['rows']} rows in "
                f"{round(result['duration_secs'], 2)}s"
            )

        return {
            "name": "parallel_multi_table",
            "row_count": aggregate_rows,
            "duration_secs": wall_duration,
            "throughput_rows_per_sec": format_throughput(aggregate_rows, wall_duration),
        }
    finally:
        for _, table_id, _ in table_specs:
            client.drop_table(table_id)


def benchmark_index_lifecycle_states(config):
    print(
        f"\nIndex create/drop benchmark | rows(empty/partial/full)=0/"
        f"{config.benchmark_total_rows // 2}/{config.benchmark_total_rows} "
        f"columns={config.benchmark_columns} indexed_columns={config.benchmark_index_columns}"
    )

    states = [
        ("empty", 0),
        ("partial", config.benchmark_total_rows // 2),
        ("full", config.benchmark_total_rows),
    ]
    results = []

    for state_name, row_count in states:
        client = DatabaseClient(base_url=config.base_url)
        table_name = unique_table_name(f"index_lifecycle_{state_name}")
        table_id = None

        try:
            table_id = create_schema(
                client,
                table_name,
                wide_schema_specs(config.benchmark_columns),
            )
            if row_count > 0:
                bulk_insert_generated_rows(
                    client=client,
                    table_id=table_id,
                    total_rows=row_count,
                    row_factory=lambda i: make_wide_row(i, table_name, config.benchmark_columns),
                    label=f"{state_name} index preload",
                    batch_size=config.benchmark_batch_size,
                    progress_every=config.test_progress_every,
                    show_progress=False,
                )

            create_times = {}
            drop_times = {}

            for column_id in config.benchmark_index_columns:
                create_times[column_id] = measure_index_creation(client, table_id, column_id)
            for column_id in config.benchmark_index_columns:
                drop_times[column_id] = measure_index_drop(client, table_id, column_id)

            total_create = sum(create_times.values())
            total_drop = sum(drop_times.values())

            print(
                f"  {state_name}: create={round(total_create, 2)}s "
                f"drop={round(total_drop, 2)}s rows={row_count}"
            )
            for column_id in config.benchmark_index_columns:
                print(
                    f"    column {column_id}: create={round(create_times[column_id], 3)}s "
                    f"drop={round(drop_times[column_id], 3)}s"
                )

            results.append(
                {
                    "state": state_name,
                    "row_count": row_count,
                    "create_secs": total_create,
                    "drop_secs": total_drop,
                }
            )
        finally:
            if table_id is not None:
                client.drop_table(table_id)

    return results


def benchmark_search_with_and_without_indexes(config):
    client = DatabaseClient(base_url=config.base_url)
    table_name = unique_table_name("search_index_bench")
    table_id = None

    try:
        table_id = create_schema(
            client,
            table_name,
            wide_schema_specs(config.benchmark_columns),
        )
        print(
            f"\nSearch benchmark | rows={config.benchmark_total_rows} "
            f"iterations={config.benchmark_search_iterations} "
            f"columns={config.benchmark_columns}"
        )

        bulk_insert_generated_rows(
            client=client,
            table_id=table_id,
            total_rows=config.benchmark_total_rows,
            row_factory=lambda i: make_wide_row(i, table_name, config.benchmark_columns),
            label="search benchmark preload",
            batch_size=config.benchmark_batch_size,
            progress_every=config.test_progress_every,
            show_progress=False,
        )

        target_row = config.benchmark_total_rows // 2
        unique_payload = search_payload(
            criteria=[
                criterion(
                    config.benchmark_search_unique_column,
                    "Equal",
                    int32_value(target_row),
                )
            ]
        )

        group_value = target_row % 1000
        group_payload = search_payload(
            criteria=[
                criterion(
                    config.benchmark_search_group_column,
                    "Equal",
                    int32_value(group_value),
                )
            ]
        )

        group_expected = expected_mod_count(
            config.benchmark_total_rows,
            1000,
            group_value,
        )

        no_index_unique = measure_search(
            client,
            table_id,
            unique_payload,
            config.benchmark_search_iterations,
            1,
            "unique search without index",
        )
        no_index_group = measure_search(
            client,
            table_id,
            group_payload,
            config.benchmark_search_iterations,
            group_expected,
            "group search without index",
        )

        create_unique_secs = measure_index_creation(
            client, table_id, config.benchmark_search_unique_column
        )
        if config.benchmark_search_group_column == config.benchmark_search_unique_column:
            create_group_secs = 0.0
        else:
            create_group_secs = measure_index_creation(
                client, table_id, config.benchmark_search_group_column
            )

        with_index_unique = measure_search(
            client,
            table_id,
            unique_payload,
            config.benchmark_search_iterations,
            1,
            "unique search with index",
        )
        with_index_group = measure_search(
            client,
            table_id,
            group_payload,
            config.benchmark_search_iterations,
            group_expected,
            "group search with index",
        )

        if config.benchmark_search_group_column != config.benchmark_search_unique_column:
            measure_index_drop(client, table_id, config.benchmark_search_group_column)
        measure_index_drop(client, table_id, config.benchmark_search_unique_column)

        unique_speedup = no_index_unique["duration_secs"] / max(
            with_index_unique["duration_secs"], 1e-9
        )
        group_speedup = no_index_group["duration_secs"] / max(
            with_index_group["duration_secs"], 1e-9
        )

        print(
            f"  create index unique/group: {round(create_unique_secs, 3)}s / "
            f"{round(create_group_secs, 3)}s"
        )
        print(
            f"  unique search no index: avg={round(no_index_unique['avg_ms'], 3)}ms "
            f"qps={no_index_unique['queries_per_sec']}"
        )
        print(
            f"  unique search with index: avg={round(with_index_unique['avg_ms'], 3)}ms "
            f"qps={with_index_unique['queries_per_sec']} "
            f"speedup={round(unique_speedup, 2)}x"
        )
        print(
            f"  group search no index: avg={round(no_index_group['avg_ms'], 3)}ms "
            f"qps={no_index_group['queries_per_sec']}"
        )
        print(
            f"  group search with index: avg={round(with_index_group['avg_ms'], 3)}ms "
            f"qps={with_index_group['queries_per_sec']} "
            f"speedup={round(group_speedup, 2)}x"
        )

        return {
            "unique_speedup": unique_speedup,
            "group_speedup": group_speedup,
            "unique_without_index": no_index_unique,
            "unique_with_index": with_index_unique,
            "group_without_index": no_index_group,
            "group_with_index": with_index_group,
        }
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def benchmark_load_profiles(config):
    serial_result = benchmark_serial_load(config)
    parallel_single_result = benchmark_parallel_single_table_load(config)
    parallel_multi_result = benchmark_parallel_multi_table_load(config)

    print("\nLoad Benchmark Summary:")
    for result in [serial_result, parallel_single_result, parallel_multi_result]:
        print(
            f"  {result['name']}: {result['row_count']} rows in "
            f"{round(result['duration_secs'], 2)}s "
            f"({result['throughput_rows_per_sec']} rows/sec)"
        )

    print(
        f"  speedup serial->parallel single-table: "
        f"{round(serial_result['duration_secs'] / max(parallel_single_result['duration_secs'], 1e-9), 2)}x"
    )
    print(
        f"  throughput speedup parallel multi-table vs serial: "
        f"{round(parallel_multi_result['throughput_rows_per_sec'] / max(serial_result['throughput_rows_per_sec'], 1e-9), 2)}x"
    )


def benchmark_all_operations(config):
    client = DatabaseClient(base_url=config.base_url)
    table_name = unique_table_name("full_operation_bench")
    table_id = None

    try:
        table_id = create_schema(
            client,
            table_name,
            wide_schema_specs(config.benchmark_columns),
        )
        total_rows = config.benchmark_total_rows
        batch_size = config.benchmark_batch_size

        print(
            f"\nFull operation benchmark | table={table_name} rows={total_rows} "
            f"batch={batch_size} columns={config.benchmark_columns}"
        )

        print("Bulk Insert")
        insert_time = bulk_insert_generated_rows(
            client=client,
            table_id=table_id,
            total_rows=total_rows,
            row_factory=lambda i: make_wide_row(i, table_name, config.benchmark_columns),
            label="full operation insert",
            batch_size=batch_size,
            progress_every=config.test_progress_every,
            show_progress=True,
        )
        print_load_benchmark_result("Full benchmark insert", total_rows, insert_time)

        search_payload_unique = search_payload(
            criteria=[
                criterion(
                    config.benchmark_search_unique_column,
                    "Equal",
                    int32_value(total_rows // 2),
                )
            ]
        )
        search_metrics = measure_search(
            client,
            table_id,
            search_payload_unique,
            config.benchmark_search_iterations,
            1,
            "full benchmark search",
        )
        print(
            f"Search: avg={round(search_metrics['avg_ms'], 3)}ms "
            f"qps={search_metrics['queries_per_sec']}"
        )

        print("Bulk Read")
        read_start = time.time()
        rows_read = 0
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            response = client.get_rows(table_id, list(range(start, end)))
            rows_read += response["row_count"]
        read_time = time.time() - read_start
        print_load_benchmark_result("Full benchmark read", rows_read, read_time)

        print("Bulk Update")
        update_start = time.time()
        updated = 0
        while updated < total_rows:
            batch_ids = list(range(updated, min(updated + batch_size, total_rows)))
            new_values = make_wide_row(updated, f"{table_name}_updated", config.benchmark_columns)
            client.update_rows(table_id, update_rows_payload(batch_ids, new_values))
            updated += len(batch_ids)
        update_time = time.time() - update_start
        print_load_benchmark_result("Full benchmark update", total_rows, update_time)

        print("Bulk Delete")
        delete_start = time.time()
        deleted = 0
        while deleted < total_rows:
            batch_ids = list(range(deleted, min(deleted + batch_size, total_rows)))
            client.delete_rows(table_id, delete_rows_payload(batch_ids))
            deleted += len(batch_ids)
        delete_time = time.time() - delete_start
        print_load_benchmark_result("Full benchmark delete", total_rows, delete_time)
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def run_functional_tests(client, config):
    health = client.health()
    assert_equal(health["status"], "healthy", "Health endpoint status")

    table_name = unique_table_name("users_test")
    table_id = None
    seed_rows = [
        make_small_row("Alice", 25),
        make_small_row("Bob", 30),
        make_small_row("Alice", 28),
        make_small_row("Charlie", 35),
    ]
    initial_target_rows = config.min_table_rows + 1

    try:
        table_id = create_schema(client, table_name, basic_schema_specs())
        bulk_insert_rows(client, table_id, seed_rows, "functional seed rows")

        filler_rows = initial_target_rows - len(seed_rows)
        bulk_insert_generated_rows(
            client=client,
            table_id=table_id,
            total_rows=filler_rows,
            row_factory=lambda i: make_small_row(f"{table_name}_bulk_{i}", 10_000 + i),
            label="functional preload",
            batch_size=config.test_insert_batch_size,
            progress_every=config.test_progress_every,
            show_progress=True,
        )

        create_indexes(client, table_id, [0, 1])

        size = client.get_size(table_id)
        assert_equal(size["row_count"], initial_target_rows, "Table size after preload")

        sample_index = min(123_456, filler_rows - 1)
        sample_row_id = len(seed_rows) + sample_index
        sample_row = make_small_row(f"{table_name}_bulk_{sample_index}", 10_000 + sample_index)

        fetched_rows = client.get_rows(table_id, [0, 2, sample_row_id])
        assert_get_rows(
            fetched_rows,
            {0: seed_rows[0], 2: seed_rows[2], sample_row_id: sample_row},
            "Initial get_rows validation",
        )

        alices = client.search(
            table_id,
            search_payload(
                criteria=[criterion(0, "Equal", string_value("Alice"))],
                sort_by=sort_by(1, "ASC"),
            ),
        )
        assert_search_rows(
            alices,
            [seed_rows[0], seed_rows[2]],
            "Search by indexed name",
            ordered=True,
        )

        projected = client.search(
            table_id,
            search_payload(
                criteria=[criterion(1, "LessThan", int32_value(40))],
                projection=[0],
                sort_by=sort_by(1, "DESC"),
            ),
        )
        assert_search_rows(
            projected,
            [
                [Cell.string(0, "Charlie")],
                [Cell.string(0, "Bob")],
                [Cell.string(0, "Alice")],
                [Cell.string(0, "Alice")],
            ],
            "Projected and sorted search on seed rows",
            ordered=True,
        )

        bulk_row_search = client.search(
            table_id,
            search_payload(
                criteria=[criterion(0, "Equal", string_value(f"{table_name}_bulk_{sample_index}"))]
            ),
        )
        assert_search_rows(bulk_row_search, [sample_row], "Search for sampled bulk row")

        updated_row_zero = make_small_row("Alice", 26)
        client.update_rows(table_id, update_rows_payload([0], updated_row_zero))
        updated_rows = client.get_rows(table_id, [0])
        assert_get_rows(updated_rows, {0: updated_row_zero}, "Updated row validation")

        updated_search = client.search(
            table_id,
            search_payload(criteria=[criterion(1, "Equal", int32_value(26))]),
        )
        assert_search_rows(updated_search, [updated_row_zero], "Search after update")

        client.delete_rows(table_id, delete_rows_payload([1]))
        size_after_delete = client.get_size(table_id)
        assert_equal(size_after_delete["row_count"], config.min_table_rows, "Table size after delete")

        deleted_search = client.search(
            table_id,
            search_payload(criteria=[criterion(0, "Equal", string_value("Bob"))]),
        )
        assert_search_rows(deleted_search, [], "Deleted row should not be searchable")

        remaining_seed_rows = client.search(
            table_id,
            search_payload(
                criteria=[criterion(1, "LessThan", int32_value(40))],
                sort_by=sort_by(1, "ASC"),
            ),
        )
        assert_search_rows(
            remaining_seed_rows,
            [updated_row_zero, seed_rows[2], seed_rows[3]],
            "Remaining small rows after update/delete",
            ordered=True,
        )

        schema = client.get_schema(table_id)
        assert_equal(schema["table_name"], table_name, "Schema table name")
        assert_equal(len(schema["columns"]), 2, "Schema column count")
        assert_equal(schema["columns"][0]["name"], "name", "Schema first column name")
        assert_equal(schema["columns"][1]["name"], "age", "Schema second column name")
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def run_parallel_single_table_tests(client, config):
    table_name = unique_table_name("parallel_single")
    table_id = None

    update_targets = [
        make_small_row(f"{table_name}_update_target_{worker}", 20_000 + worker)
        for worker in range(config.parallel_single_workers)
    ]
    delete_targets = [
        make_small_row(f"{table_name}_delete_target_{worker}", 21_000 + worker)
        for worker in range(config.parallel_single_workers)
    ]
    reserved_rows = update_targets + delete_targets
    inserted_total = (
        config.parallel_single_workers * config.parallel_single_rows_per_worker
    )

    worker_batches = []
    for worker in range(config.parallel_single_workers):
        batch = [
            make_small_row(
                f"{table_name}_parallel_insert_{worker}_{row_index}",
                40_000 + worker * config.parallel_single_rows_per_worker + row_index,
            )
            for row_index in range(config.parallel_single_rows_per_worker)
        ]
        worker_batches.append((worker, batch))

    try:
        table_id = create_schema(client, table_name, basic_schema_specs())
        bulk_insert_rows(client, table_id, reserved_rows, "single-table reserved rows")

        filler_rows = config.min_table_rows - len(reserved_rows)
        bulk_insert_generated_rows(
            client=client,
            table_id=table_id,
            total_rows=filler_rows,
            row_factory=lambda i: make_small_row(f"{table_name}_bulk_{i}", 100_000 + i),
            label="single-table preload",
            batch_size=config.test_insert_batch_size,
            progress_every=config.test_progress_every,
            show_progress=True,
        )

        create_indexes(client, table_id, [0, 1])
        initial_size = client.get_size(table_id)
        assert_equal(initial_size["row_count"], config.min_table_rows, "Single-table initial size")

        def insert_batch(item):
            worker, batch = item
            response = client.insert_rows(table_id, insert_rows_payload(batch))
            assert_equal(
                response["row_count"],
                len(batch),
                f"Parallel single-table insert row count for worker {worker}",
            )
            return worker

        run_parallel(
            "parallel single-table inserts",
            worker_batches,
            insert_batch,
            max_workers=config.parallel_single_workers,
        )

        size_after_insert = client.get_size(table_id)
        assert_equal(
            size_after_insert["row_count"],
            config.min_table_rows + inserted_total,
            "Single-table size after parallel insert",
        )

        def search_inserted_row(row):
            response = client.search(
                table_id,
                search_payload(
                    criteria=[criterion(0, "Equal", string_value(cells_by_column(row)[0]))]
                ),
            )
            assert_search_rows(
                response,
                [row],
                f"Single-table search for inserted row {cells_by_column(row)[0]}",
            )
            return True

        run_parallel(
            "parallel single-table searches",
            [batch[0] for _, batch in worker_batches],
            search_inserted_row,
            max_workers=config.parallel_single_workers,
        )

        update_specs = []
        for worker in range(config.parallel_single_workers):
            row_id = worker
            updated_row = make_small_row(f"{table_name}_updated_{worker}", 300_000 + worker)
            update_specs.append((worker, row_id, updated_row))

        def update_worker(spec):
            worker, row_id, updated_row = spec
            client.update_rows(table_id, update_rows_payload([row_id], updated_row))
            response = client.get_rows(table_id, [row_id])
            assert_get_rows(
                response,
                {row_id: updated_row},
                f"Single-table updated row fetch for worker {worker}",
            )
            return worker

        run_parallel(
            "parallel single-table updates",
            update_specs,
            update_worker,
            max_workers=config.parallel_single_workers,
        )

        delete_specs = []
        for worker in range(config.parallel_single_workers):
            row_id = config.parallel_single_workers + worker
            deleted_row = delete_targets[worker]
            delete_specs.append((worker, row_id, deleted_row))

        def delete_worker(spec):
            worker, row_id, deleted_row = spec
            client.delete_rows(table_id, delete_rows_payload([row_id]))
            response = client.search(
                table_id,
                search_payload(
                    criteria=[criterion(0, "Equal", string_value(cells_by_column(deleted_row)[0]))]
                ),
            )
            assert_search_rows(
                response,
                [],
                f"Single-table delete validation for worker {worker}",
            )
            return worker

        run_parallel(
            "parallel single-table deletes",
            delete_specs,
            delete_worker,
            max_workers=config.parallel_single_workers,
        )

        final_size = client.get_size(table_id)
        assert_equal(
            final_size["row_count"],
            config.min_table_rows + inserted_total - config.parallel_single_workers,
            "Single-table final size",
        )

        sample_bulk_index = min(123_456, filler_rows - 1)
        sample_bulk_row = make_small_row(
            f"{table_name}_bulk_{sample_bulk_index}",
            100_000 + sample_bulk_index,
        )
        sample_bulk_search = client.search(
            table_id,
            search_payload(
                criteria=[criterion(0, "Equal", string_value(cells_by_column(sample_bulk_row)[0]))]
            ),
        )
        assert_search_rows(
            sample_bulk_search,
            [sample_bulk_row],
            "Single-table sampled bulk row still present",
        )
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def run_parallel_multi_table_tests(client, config):
    table_specs = []

    try:
        for table_index in range(config.parallel_multi_tables):
            table_name = unique_table_name(f"parallel_multi_{table_index}")
            table_id = create_schema(client, table_name, basic_schema_specs())

            control_rows = [
                make_small_row(f"{table_name}_update_target", 50_000),
                make_small_row(f"{table_name}_delete_target", 50_001),
                make_small_row(f"{table_name}_search_target", 50_002),
            ]
            bulk_insert_rows(client, table_id, control_rows, f"{table_name} control rows")

            filler_rows = config.min_table_rows - len(control_rows)
            bulk_insert_generated_rows(
                client=client,
                table_id=table_id,
                total_rows=filler_rows,
                row_factory=lambda i: make_small_row(f"{table_name}_bulk_{i}", 200_000 + i),
                label=f"{table_name} preload",
                batch_size=config.test_insert_batch_size,
                progress_every=config.test_progress_every,
                show_progress=True,
            )

            create_indexes(client, table_id, [0, 1])
            size = client.get_size(table_id)
            assert_equal(size["row_count"], config.min_table_rows, f"{table_name} initial size")

            inserted_rows = [
                make_small_row(f"{table_name}_parallel_insert_{i}", 900_000 + i)
                for i in range(config.parallel_multi_rows_per_table)
            ]
            updated_row = make_small_row(f"{table_name}_updated", 950_000 + table_index)
            search_target = control_rows[2]

            table_specs.append(
                {
                    "table_id": table_id,
                    "table_name": table_name,
                    "inserted_rows": inserted_rows,
                    "updated_row": updated_row,
                    "search_target": search_target,
                }
            )

        def table_worker(spec):
            table_id = spec["table_id"]
            table_name = spec["table_name"]
            inserted_rows = spec["inserted_rows"]
            updated_row = spec["updated_row"]
            search_target = spec["search_target"]

            insert_result = client.insert_rows(table_id, insert_rows_payload(inserted_rows))
            assert_equal(
                insert_result["row_count"],
                len(inserted_rows),
                f"Multi-table insert count for {table_name}",
            )

            search_result = client.search(
                table_id,
                search_payload(
                    criteria=[criterion(0, "Equal", string_value(cells_by_column(search_target)[0]))]
                ),
            )
            assert_search_rows(
                search_result,
                [search_target],
                f"Multi-table control-row search for {table_name}",
            )

            inserted_sample = inserted_rows[-1]
            inserted_search = client.search(
                table_id,
                search_payload(
                    criteria=[criterion(0, "Equal", string_value(cells_by_column(inserted_sample)[0]))]
                ),
            )
            assert_search_rows(
                inserted_search,
                [inserted_sample],
                f"Multi-table inserted-row search for {table_name}",
            )

            client.update_rows(table_id, update_rows_payload([0], updated_row))
            client.delete_rows(table_id, delete_rows_payload([1]))

            updated_search = client.search(
                table_id,
                search_payload(
                    criteria=[criterion(0, "Equal", string_value(cells_by_column(updated_row)[0]))]
                ),
            )
            assert_search_rows(
                updated_search,
                [updated_row],
                f"Multi-table updated-row search for {table_name}",
            )

            deleted_search = client.search(
                table_id,
                search_payload(
                    criteria=[criterion(0, "Equal", string_value(f"{table_name}_delete_target"))]
                ),
            )
            assert_search_rows(
                deleted_search,
                [],
                f"Multi-table deleted-row absence for {table_name}",
            )

            final_size = client.get_size(table_id)
            assert_equal(
                final_size["row_count"],
                config.min_table_rows + len(inserted_rows) - 1,
                f"Multi-table final size for {table_name}",
            )
            return table_name

        run_parallel(
            "parallel multi-table workflows",
            table_specs,
            table_worker,
            max_workers=config.parallel_multi_tables,
        )
    finally:
        for spec in table_specs:
            client.drop_table(spec["table_id"])


def run_benchmarks_if_requested(config):
    if not config.run_benchmark:
        return

    benchmark_load_profiles(config)
    benchmark_index_lifecycle_states(config)
    benchmark_search_with_and_without_indexes(config)

    if config.run_full_operation_benchmark:
        benchmark_all_operations(config)


def run_all_tests(config):
    print(
        f"Running tests against {config.base_url} with at least "
        f"{config.min_table_rows} rows per test table"
    )
    client = DatabaseClient(base_url=config.base_url)

    run_functional_tests(client, config)
    run_parallel_single_table_tests(client, config)
    run_parallel_multi_table_tests(client, config)
    run_benchmarks_if_requested(config)

    print("All tests passed!")


if __name__ == "__main__":
    run_all_tests(parse_args())
