import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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


MIN_TABLE_ROWS = max(1_000_000, int(os.getenv("TEST_TABLE_ROWS", "1000000")))
TEST_INSERT_BATCH_SIZE = int(os.getenv("TEST_INSERT_BATCH_SIZE", "200000"))
TEST_PROGRESS_EVERY = int(os.getenv("TEST_PROGRESS_EVERY", "200000"))
LOAD_BENCH_TOTAL_ROWS = max(1_000_000, int(os.getenv("LOAD_BENCH_TOTAL_ROWS", "1000000")))
LOAD_BENCH_BATCH_SIZE = int(os.getenv("LOAD_BENCH_BATCH_SIZE", str(TEST_INSERT_BATCH_SIZE)))
LOAD_BENCH_SINGLE_WORKERS = int(os.getenv("LOAD_BENCH_SINGLE_WORKERS", "4"))
LOAD_BENCH_MULTI_TABLES = int(os.getenv("LOAD_BENCH_MULTI_TABLES", "2"))
LOAD_BENCH_MULTI_ROWS_PER_TABLE = max(
    1_000_000, int(os.getenv("LOAD_BENCH_MULTI_ROWS_PER_TABLE", "1000000"))
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
        row_id: row_signature(cells) for row_id, cells in expected_rows_by_id.items()
    }
    assert_equal(actual_rows, expected_rows, message)


def assert_search_rows(response, expected_rows, message, ordered=False):
    assert_equal(response["row_count"], len(expected_rows), f"{message} row count")
    actual_rows = [row_signature(row) for row in response["rows"]]
    expected_signatures = [row_signature(row) for row in expected_rows]

    if not ordered:
        actual_rows = sorted(actual_rows)
        expected_signatures = sorted(expected_signatures)

    assert_equal(actual_rows, expected_signatures, message)


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


def make_user_row(name, age):
    return [Cell.string(0, name), Cell.int32(1, age)]


def unique_table_name(prefix):
    return f"{prefix}_{time.time_ns()}"


def print_progress(label, inserted, total_rows):
    if inserted == total_rows or inserted % TEST_PROGRESS_EVERY == 0:
        print(f"{label}: inserted {inserted}/{total_rows}")


def format_throughput(row_count, duration_secs):
    return int(row_count / max(duration_secs, 1e-9))


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


def create_table_with_schema(client, table_name):
    table = client.create_table(table_name)
    table_id = table["table_id"]
    client.create_column(table_id, create_column_payload("name", DataType.String))
    client.create_column(table_id, create_column_payload("age", DataType.IntegerI32))
    return table_id


def create_indexes(client, table_id):
    client.create_index(table_id, 0)
    client.create_index(table_id, 1)


def bulk_insert_rows(client, table_id, rows, label):
    if not rows:
        return
    response = client.insert_rows(table_id, insert_rows_payload(rows))
    assert_equal(response["row_count"], len(rows), f"{label} insert row_count")


def bulk_insert_generated_rows(
    client,
    table_id,
    total_rows,
    row_factory,
    label,
    batch_size=None,
    show_progress=True,
):
    batch_size = batch_size or TEST_INSERT_BATCH_SIZE
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
            print_progress(label, inserted, total_rows)
    return time.time() - start_time


def print_load_benchmark_result(name, row_count, duration_secs):
    print(
        f"{name}: {row_count} rows in {round(duration_secs, 2)}s "
        f"({format_throughput(row_count, duration_secs)} rows/sec)"
    )


def benchmark_serial_load(base_url, total_rows, batch_size):
    client = DatabaseClient(base_url=base_url)
    table_name = unique_table_name("serial_load_bench")
    table_id = None

    try:
        table_id = create_table_with_schema(client, table_name)
        print(f"\nSerial load benchmark | table={table_name} rows={total_rows} batch={batch_size}")
        duration = bulk_insert_generated_rows(
            client,
            table_id,
            total_rows,
            lambda i: make_user_row(f"{table_name}_serial_{i}", i),
            "serial load",
            batch_size=batch_size,
            show_progress=True,
        )
        size = client.get_size(table_id)
        assert_equal(size["row_count"], total_rows, "Serial load final size")
        print_load_benchmark_result("Serial single-table load", total_rows, duration)
        return {
            "name": "serial_single_table",
            "row_count": total_rows,
            "duration_secs": duration,
            "throughput_rows_per_sec": format_throughput(total_rows, duration),
        }
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def benchmark_parallel_single_table_load(base_url, total_rows, batch_size, worker_count):
    client = DatabaseClient(base_url=base_url)
    table_name = unique_table_name("parallel_single_load_bench")
    table_id = None
    worker_count = max(1, min(worker_count, total_rows))

    try:
        table_id = create_table_with_schema(client, table_name)
        print(
            f"\nParallel single-table load benchmark | table={table_name} "
            f"rows={total_rows} batch={batch_size} workers={worker_count}"
        )

        rows_per_worker = total_rows // worker_count
        remainder = total_rows % worker_count
        worker_specs = []
        start_offset = 0

        for worker in range(worker_count):
            worker_rows = rows_per_worker + (1 if worker < remainder else 0)
            worker_specs.append((worker, start_offset, worker_rows))
            start_offset += worker_rows

        def worker_fn(spec):
            worker, offset, worker_rows = spec
            local_client = DatabaseClient(base_url=base_url)
            duration = bulk_insert_generated_rows(
                local_client,
                table_id,
                worker_rows,
                lambda i: make_user_row(
                    f"{table_name}_parallel_single_{offset + i}",
                    offset + i,
                ),
                f"parallel single worker {worker}",
                batch_size=batch_size,
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
        assert_equal(size["row_count"], total_rows, "Parallel single-table final size")

        print_load_benchmark_result("Parallel single-table load", total_rows, wall_duration)
        for result in sorted(worker_results, key=lambda item: item["worker"]):
            print(
                f"  worker {result['worker']}: "
                f"{result['rows']} rows in {round(result['duration_secs'], 2)}s"
            )

        return {
            "name": "parallel_single_table",
            "row_count": total_rows,
            "duration_secs": wall_duration,
            "throughput_rows_per_sec": format_throughput(total_rows, wall_duration),
            "workers": worker_count,
        }
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def benchmark_parallel_multi_table_load(base_url, rows_per_table, batch_size, table_count):
    client = DatabaseClient(base_url=base_url)
    table_count = max(1, table_count)
    table_specs = []

    try:
        for table_idx in range(table_count):
            table_name = unique_table_name(f"parallel_multi_load_bench_{table_idx}")
            table_id = create_table_with_schema(client, table_name)
            table_specs.append((table_idx, table_id, table_name))

        print(
            f"\nParallel multi-table load benchmark | "
            f"tables={table_count} rows_per_table={rows_per_table} batch={batch_size}"
        )

        def worker_fn(spec):
            worker, table_id, table_name = spec
            local_client = DatabaseClient(base_url=base_url)
            duration = bulk_insert_generated_rows(
                local_client,
                table_id,
                rows_per_table,
                lambda i: make_user_row(f"{table_name}_parallel_multi_{i}", i),
                f"parallel multi table {worker}",
                batch_size=batch_size,
                show_progress=False,
            )
            size = local_client.get_size(table_id)
            assert_equal(
                size["row_count"],
                rows_per_table,
                f"Parallel multi-table final size for {table_name}",
            )
            return {
                "worker": worker,
                "table_name": table_name,
                "rows": rows_per_table,
                "duration_secs": duration,
            }

        wall_start = time.time()
        worker_results = run_parallel(
            "parallel multi-table load",
            table_specs,
            worker_fn,
            max_workers=table_count,
        )
        wall_duration = time.time() - wall_start

        aggregate_rows = rows_per_table * table_count
        print_load_benchmark_result(
            "Parallel multi-table aggregate load", aggregate_rows, wall_duration
        )
        for result in sorted(worker_results, key=lambda item: item["worker"]):
            print(
                f"  table {result['table_name']}: "
                f"{result['rows']} rows in {round(result['duration_secs'], 2)}s"
            )

        return {
            "name": "parallel_multi_table",
            "row_count": aggregate_rows,
            "duration_secs": wall_duration,
            "throughput_rows_per_sec": format_throughput(aggregate_rows, wall_duration),
            "tables": table_count,
            "rows_per_table": rows_per_table,
        }
    finally:
        for _, table_id, _ in table_specs:
            client.drop_table(table_id)


def benchmark_load_profiles(base_url):
    serial_result = benchmark_serial_load(
        base_url=base_url,
        total_rows=LOAD_BENCH_TOTAL_ROWS,
        batch_size=LOAD_BENCH_BATCH_SIZE,
    )
    parallel_single_result = benchmark_parallel_single_table_load(
        base_url=base_url,
        total_rows=LOAD_BENCH_TOTAL_ROWS,
        batch_size=LOAD_BENCH_BATCH_SIZE,
        worker_count=LOAD_BENCH_SINGLE_WORKERS,
    )
    parallel_multi_result = benchmark_parallel_multi_table_load(
        base_url=base_url,
        rows_per_table=LOAD_BENCH_MULTI_ROWS_PER_TABLE,
        batch_size=LOAD_BENCH_BATCH_SIZE,
        table_count=LOAD_BENCH_MULTI_TABLES,
    )

    print("\nLoad Benchmark Summary:")
    for result in [serial_result, parallel_single_result, parallel_multi_result]:
        print(
            f"  {result['name']}: {result['row_count']} rows in "
            f"{round(result['duration_secs'], 2)}s "
            f"({result['throughput_rows_per_sec']} rows/sec)"
        )

    serial_vs_parallel_single = (
        serial_result["duration_secs"] / max(parallel_single_result["duration_secs"], 1e-9)
    )
    parallel_multi_vs_serial = (
        parallel_multi_result["throughput_rows_per_sec"]
        / max(serial_result["throughput_rows_per_sec"], 1e-9)
    )
    print(
        f"  speedup serial->parallel single-table: {round(serial_vs_parallel_single, 2)}x"
    )
    print(
        "  throughput speedup parallel multi-table vs serial: "
        f"{round(parallel_multi_vs_serial, 2)}x"
    )


def benchmark_all_operations(
    client: DatabaseClient,
    table_id: int,
    total_rows=1_000_000,
    batch_size=400_000,
):
    print(
        f"\nStarting benchmark for table {table_id} | "
        f"Total rows: {total_rows} | Batch size: {batch_size}\n"
    )

    print("Bulk Insert")
    start_time = time.time()
    inserted = 0

    while inserted < total_rows:
        current_batch_size = min(batch_size, total_rows - inserted)
        batch = [
            make_user_row(f"user_{inserted + i}", inserted + i)
            for i in range(current_batch_size)
        ]

        t1 = time.time()
        client.insert_rows(table_id, insert_rows_payload(batch))
        t2 = time.time()

        inserted += current_batch_size
        batch_time = max(t2 - t1, 1e-9)
        print(
            f"Inserted: {inserted}/{total_rows} | "
            f"Batch time: {round(batch_time, 3)}s | "
            f"Throughput: {int(current_batch_size / batch_time)} rows/sec"
        )

    total_insert_time = time.time() - start_time
    print(
        f"\nInsert complete | Total time: {round(total_insert_time, 2)}s | "
        f"Avg throughput: {int(total_rows / max(total_insert_time, 1e-9))} rows/sec\n"
    )

    print("Bulk Read")
    start_time = time.time()
    all_rows = []
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch_rows = client.get_rows(table_id, list(range(start, end)))
        all_rows.extend(batch_rows["rows"])
        elapsed = max(time.time() - start_time, 1e-9)
        print(
            f"Read: {len(all_rows)}/{total_rows} rows | "
            f"Throughput: {int(len(all_rows) / elapsed)} rows/sec"
        )
    read_time = time.time() - start_time
    print(
        f"Read {len(all_rows)} rows | Time: {round(read_time, 2)}s | "
        f"Avg throughput: {int(len(all_rows) / max(read_time, 1e-9))} rows/sec\n"
    )

    print("Bulk Update")
    start_time = time.time()
    updated = 0

    while updated < total_rows:
        batch_ids = list(range(updated, min(updated + batch_size, total_rows)))
        new_values = make_user_row(f"user_{updated}_updated", updated + 1000)

        t1 = time.time()
        client.update_rows(table_id, update_rows_payload(batch_ids, new_values))
        t2 = time.time()

        updated += len(batch_ids)
        batch_time = max(t2 - t1, 1e-9)
        print(
            f"Updated: {updated}/{total_rows} | "
            f"Batch time: {round(batch_time, 3)}s | "
            f"Throughput: {int(len(batch_ids) / batch_time)} rows/sec"
        )

    update_time = time.time() - start_time
    print(
        f"Updated {total_rows} rows | Time: {round(update_time, 2)}s | "
        f"Avg throughput: {int(total_rows / max(update_time, 1e-9))} rows/sec\n"
    )

    print("Bulk Delete")
    start_time = time.time()
    deleted = 0
    while deleted < total_rows:
        batch_ids = list(range(deleted, min(deleted + batch_size, total_rows)))
        client.delete_rows(table_id, delete_rows_payload(batch_ids))
        deleted += len(batch_ids)
    delete_time = time.time() - start_time
    print(
        f"Deleted {total_rows} rows | Time: {round(delete_time, 2)}s | "
        f"Avg throughput: {int(total_rows / max(delete_time, 1e-9))} rows/sec\n"
    )

    print("Benchmark Summary:")
    print(
        f"Insert: {round(total_insert_time, 2)}s | "
        f"Read: {round(read_time, 2)}s | "
        f"Update: {round(update_time, 2)}s | "
        f"Delete: {round(delete_time, 2)}s"
    )


def run_functional_tests(client: DatabaseClient):
    health = client.health()
    assert_equal(health["status"], "healthy", "Health endpoint status")

    table_name = unique_table_name("users_test")
    table_id = None

    seed_rows = [
        make_user_row("Alice", 25),
        make_user_row("Bob", 30),
        make_user_row("Alice", 28),
        make_user_row("Charlie", 35),
    ]
    initial_target_rows = MIN_TABLE_ROWS + 1

    try:
        table_id = create_table_with_schema(client, table_name)
        bulk_insert_rows(client, table_id, seed_rows, "functional seed rows")

        filler_rows = initial_target_rows - len(seed_rows)
        bulk_insert_generated_rows(
            client,
            table_id,
            filler_rows,
            lambda i: make_user_row(f"{table_name}_bulk_{i}", 10_000 + i),
            "functional preload",
        )

        create_indexes(client, table_id)

        size = client.get_size(table_id)
        assert_equal(size["row_count"], initial_target_rows, "Table size after preload")

        sample_index = min(123_456, filler_rows - 1)
        sample_row_id = len(seed_rows) + sample_index
        sample_row = make_user_row(f"{table_name}_bulk_{sample_index}", 10_000 + sample_index)

        fetched_rows = client.get_rows(table_id, [0, 2, sample_row_id])
        assert_get_rows(
            fetched_rows,
            {
                0: seed_rows[0],
                2: seed_rows[2],
                sample_row_id: sample_row,
            },
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
        assert_search_rows(
            bulk_row_search,
            [sample_row],
            "Search for sampled bulk row",
        )

        updated_row_zero = make_user_row("Alice", 26)
        client.update_rows(table_id, update_rows_payload([0], updated_row_zero))

        updated_rows = client.get_rows(table_id, [0])
        assert_get_rows(
            updated_rows,
            {0: updated_row_zero},
            "Updated row validation",
        )

        updated_search = client.search(
            table_id,
            search_payload(criteria=[criterion(1, "Equal", int32_value(26))]),
        )
        assert_search_rows(updated_search, [updated_row_zero], "Search after update")

        client.delete_rows(table_id, delete_rows_payload([1]))

        size_after_delete = client.get_size(table_id)
        assert_equal(size_after_delete["row_count"], MIN_TABLE_ROWS, "Table size after delete")

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


def run_parallel_single_table_tests(client: DatabaseClient):
    worker_count = int(os.getenv("PARALLEL_SINGLE_WORKERS", "4"))
    rows_per_worker = int(os.getenv("PARALLEL_SINGLE_ROWS_PER_WORKER", "100"))

    table_name = unique_table_name("parallel_single")
    table_id = None

    update_targets = [
        make_user_row(f"{table_name}_update_target_{worker}", 20_000 + worker)
        for worker in range(worker_count)
    ]
    delete_targets = [
        make_user_row(f"{table_name}_delete_target_{worker}", 21_000 + worker)
        for worker in range(worker_count)
    ]
    reserved_rows = update_targets + delete_targets
    inserted_total = worker_count * rows_per_worker

    worker_batches = []
    for worker in range(worker_count):
        batch = [
            make_user_row(
                f"{table_name}_parallel_insert_{worker}_{row_idx}",
                40_000 + worker * rows_per_worker + row_idx,
            )
            for row_idx in range(rows_per_worker)
        ]
        worker_batches.append((worker, batch))

    try:
        table_id = create_table_with_schema(client, table_name)
        bulk_insert_rows(client, table_id, reserved_rows, "single-table reserved rows")

        filler_rows = MIN_TABLE_ROWS - len(reserved_rows)
        bulk_insert_generated_rows(
            client,
            table_id,
            filler_rows,
            lambda i: make_user_row(f"{table_name}_bulk_{i}", 100_000 + i),
            "single-table preload",
        )

        create_indexes(client, table_id)

        initial_size = client.get_size(table_id)
        assert_equal(initial_size["row_count"], MIN_TABLE_ROWS, "Single-table initial size")

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
            max_workers=worker_count,
        )

        size_after_insert = client.get_size(table_id)
        assert_equal(
            size_after_insert["row_count"],
            MIN_TABLE_ROWS + inserted_total,
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
            max_workers=worker_count,
        )

        update_specs = []
        for worker in range(worker_count):
            row_id = worker
            updated_row = make_user_row(f"{table_name}_updated_{worker}", 300_000 + worker)
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
            max_workers=worker_count,
        )

        delete_specs = []
        for worker in range(worker_count):
            row_id = worker_count + worker
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
            max_workers=worker_count,
        )

        final_size = client.get_size(table_id)
        assert_equal(
            final_size["row_count"],
            MIN_TABLE_ROWS + inserted_total - worker_count,
            "Single-table final size",
        )

        sample_bulk_index = min(123_456, filler_rows - 1)
        sample_bulk_row = make_user_row(f"{table_name}_bulk_{sample_bulk_index}", 100_000 + sample_bulk_index)
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

        for worker, _, updated_row in update_specs:
            updated_search = client.search(
                table_id,
                search_payload(
                    criteria=[criterion(0, "Equal", string_value(cells_by_column(updated_row)[0]))]
                ),
            )
            assert_search_rows(
                updated_search,
                [updated_row],
                f"Single-table updated row still searchable for worker {worker}",
            )
    finally:
        if table_id is not None:
            client.drop_table(table_id)


def run_parallel_multi_table_tests(client: DatabaseClient):
    table_count = int(os.getenv("PARALLEL_MULTI_TABLES", "2"))
    inserted_rows_per_table = int(os.getenv("PARALLEL_MULTI_ROWS_PER_TABLE", "100"))

    table_specs = []

    try:
        for table_idx in range(table_count):
            table_name = unique_table_name(f"parallel_multi_{table_idx}")
            table_id = create_table_with_schema(client, table_name)

            control_rows = [
                make_user_row(f"{table_name}_update_target", 50_000),
                make_user_row(f"{table_name}_delete_target", 50_001),
                make_user_row(f"{table_name}_search_target", 50_002),
            ]
            bulk_insert_rows(client, table_id, control_rows, f"{table_name} control rows")

            filler_rows = MIN_TABLE_ROWS - len(control_rows)
            bulk_insert_generated_rows(
                client,
                table_id,
                filler_rows,
                lambda i: make_user_row(f"{table_name}_bulk_{i}", 200_000 + i),
                f"{table_name} preload",
            )

            create_indexes(client, table_id)

            size = client.get_size(table_id)
            assert_equal(size["row_count"], MIN_TABLE_ROWS, f"{table_name} initial size")

            inserted_rows = [
                make_user_row(f"{table_name}_parallel_insert_{i}", 900_000 + i)
                for i in range(inserted_rows_per_table)
            ]
            updated_row = make_user_row(f"{table_name}_updated", 950_000 + table_idx)
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
                MIN_TABLE_ROWS + len(inserted_rows) - 1,
                f"Multi-table final size for {table_name}",
            )
            return table_name

        run_parallel(
            "parallel multi-table workflows",
            table_specs,
            table_worker,
            max_workers=table_count,
        )
    finally:
        for spec in table_specs:
            client.drop_table(spec["table_id"])


def run_benchmark_if_requested(client: DatabaseClient):
    if os.getenv("RUN_BENCHMARK", "0").lower() not in {"1", "true", "yes"}:
        return

    base_url = client.base_url
    benchmark_load_profiles(base_url)

    if os.getenv("RUN_FULL_OPERATION_BENCHMARK", "0").lower() not in {"1", "true", "yes"}:
        return

    benchmark_table_name = unique_table_name("users_benchmark")
    benchmark_table = client.create_table(benchmark_table_name)
    benchmark_table_id = benchmark_table["table_id"]

    try:
        client.create_column(
            benchmark_table_id,
            create_column_payload("name", DataType.String),
        )
        client.create_column(
            benchmark_table_id,
            create_column_payload("age", DataType.IntegerI32),
        )

        total_rows = int(os.getenv("BENCH_TOTAL_ROWS", "1000000"))
        batch_size = int(os.getenv("BENCH_BATCH_SIZE", "400000"))
        benchmark_all_operations(
            client,
            benchmark_table_id,
            total_rows=total_rows,
            batch_size=batch_size,
        )
    finally:
        client.drop_table(benchmark_table_id)


def run_all_tests():
    base_url = os.getenv("DATABASE_URL", "http://localhost:8080")
    print(
        f"Running tests against {base_url} with at least {MIN_TABLE_ROWS} rows per test table"
    )
    client = DatabaseClient(base_url=base_url)
    run_functional_tests(client)
    run_parallel_single_table_tests(client)
    run_parallel_multi_table_tests(client)
    run_benchmark_if_requested(client)
    print("All tests passed!")


if __name__ == "__main__":
    run_all_tests()
