from typing import Any, Dict
import sqlite3, json

from .lineage_tracker import tracker

DDL = [
    """
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        created_at REAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS datasets (
        id TEXT PRIMARY KEY,
        name TEXT,
        kind TEXT,
        fmt TEXT,
        path TEXT,
        code_file TEXT,
        code_line INTEGER,
        rows INTEGER,
        run_id TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS columns (
        id TEXT PRIMARY KEY,
        dataset_id TEXT,
        name TEXT,
        dtype TEXT,
        run_id TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS transforms (
        id TEXT PRIMARY KEY,
        name TEXT,
        code_file TEXT,
        code_line INTEGER,
        params_hash TEXT,
        run_id TEXT,
        created_at REAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_to_transform_edges (
        src_dataset_id TEXT,
        transform_id TEXT,
        run_id TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS transform_to_dataset_edges (
        transform_id TEXT,
        dest_dataset_id TEXT,
        run_id TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS column_to_transform_edges (
        src_col_id TEXT,
        transform_id TEXT,
        run_id TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS transform_to_column_edges (
        transform_id TEXT,
        dest_col_id TEXT,
        run_id TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS column_stats (
        dataset_id TEXT,
        column TEXT,
        dtype TEXT,
        count INTEGER,
        nulls INTEGER,
        mean REAL,
        std REAL,
        top TEXT,
        top_freq INTEGER,
        run_id TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        node_kind TEXT,
        node_id TEXT,
        change_type TEXT,
        detail TEXT,
        severity TEXT
    );
    """
]

def init_db(path: str):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    for statement in DDL:
        cur.execute(statement)
    conn.commit()
    return conn

def persist_current_run(db_path: str):
    conn = init_db(db_path)
    cur = conn.cursor()

    cur.execute(
        "INSERT OR REPLACE INTO runs (run_id, created_at) VALUES (?, strftime('%s', 'now'))",
        (tracker.run_id,)
    )

    cur.executemany("""
        INSERT OR REPLACE INTO datasets(id, name, kind, fmt, path, code_file, code_line, rows, run_id)
        VALUES (:id, :name, :kind, :fmt, :path, :code_file, :code_line, :rows, :run_id)
    """, [d.__dict__ for d in tracker.datasets.values()])

    cur.executemany("""
        INSERT OR REPLACE INTO columns(id, dataset_id, name, dtype, run_id)
        VALUES (:id, :dataset_id, :name, :dtype, :run_id)
    """, [c.__dict__ for c in tracker.columns])

    cur.executemany("""
        INSERT OR REPLACE INTO transforms(id, name, code_file, code_line, params_hash, run_id)
        VALUES (:id, :name, :code_file, :code_line, :params_hash, :run_id)
    """, [t.__dict__ for t in tracker.transforms.values()])

    cur.executemany("""
        INSERT OR REPLACE INTO dataset_to_transform_edges(src_dataset_id, transform_id, run_id)
        VALUES (?, ?, ?)
    """, [(e.src_ds_id, e.transform_id, e.run_id) for e in tracker.dataset_to_transform])

    cur.executemany("""
        INSERT OR REPLACE INTO transform_to_dataset_edges(transform_id, dest_dataset_id, run_id)
        VALUES (?, ?, ?)
    """, [(e.transform_id, e.dest_ds_id, e.run_id) for e in tracker.transform_to_dataset])

    cur.executemany("""
        INSERT OR REPLACE INTO column_to_transform_edges(src_col_id, transform_id, run_id)
        VALUES (?, ?, ?)
    """, [(e.src_col_id, e.transform_id, e.run_id) for e in tracker.col_to_transform])

    cur.executemany("""
        INSERT OR REPLACE INTO transform_to_column_edges(transform_id, dest_col_id, run_id)
        VALUES (?, ?, ?)
    """, [(e.transform_id, e.dest_col_id, e.run_id) for e in tracker.transform_to_col])

    cur.executemany("""
        INSERT OR REPLACE INTO column_stats(dataset_id, column, dtype, count, nulls, mean, std, top, top_freq, run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(s.dataset_id, s.column, s.dtype, s.count, s.nulls, s.mean, s.std, s.top, s.top_freq, s.run_id)
      for s in tracker.column_stats])

    conn.commit()
    conn.close()

def detect_changes(db_path: str, base_run: str, curr_run: str, null_spike=0.1, mean_tol=0.2, std_tol=0.3):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    def load_stats(run):
        cur.execute("SELECT * FROM column_stats WHERE run_id = ?", (run,))
        cols = [c[0] for c in cur.description]
        out = {}
        for r in cur.fetchall():
            d = dict(zip(cols, r))
            out[(d["dataset_id"], d["column"])] = d
        return out
    A, B = load_stats(base_run), load_stats(curr_run)
    changes = []

    keys = set(A.keys()) | set(B.keys())
    for key in keys:
        a, b = A.get(key), B.get(key)
        ds_id, col = key if b else (a["dataset_id"], a["column"])
        col_id = f"{ds_id}|{col}"

        if a and not b:
            changes.append({"run_id": curr_run, "node_kind": "column", "node_id": col_id,
                            "change_type": "schema_drop", "severity": "CRITICAL",
                            "detail": json.dumps({"dataset_id": ds_id, "column": col})})
            continue
        if b and not a:
            changes.append({"run_id": curr_run, "node_kind": "column", "node_id": col_id,
                            "change_type": "schema_add", "severity": "LOW",
                            "detail": json.dumps({"dataset_id": ds_id, "column": col})})
            continue
        if a["dtype"] != b["dtype"]:
            changes.append({"run_id": curr_run, "node_kind":"column", "node_id":col_id,
                            "change_type":"type_change", "severity":"HIGH",
                            "detail":json.dumps({"from":a["dtype"], "to":b["dtype"]})})

        a_null = (a["nulls"] or 0) / (a["count"] or 1)
        b_null = (b["nulls"] or 0) / (b["count"] or 1)
        if b_null - a_null >= null_spike:
            changes.append({"run_id": curr_run, "node_kind": "column", "node_id": col_id,
                            "change_type": "null_spike", "severity": "MEDIUM",
                            "detail": json.dumps({"from": a_null, "to": b_null})})
        if a["mean"] is not None and b["mean"] is not None:
            # relative deltas; guard divide-by-zero
            def rel(old, new):
                return abs(new - old) / (abs(old) if abs(old) > 1e-9 else 1.0)

            if rel(a["mean"], b["mean"]) >= mean_tol or \
                    (a["std"] is not None and b["std"] is not None and rel(a["std"], b["std"]) >= std_tol):
                changes.append({"run_id": curr_run, "node_kind": "column", "node_id": col_id,
                                "change_type": "value_shift", "severity": "LOW",
                                "detail": json.dumps({"mean_from": a["mean"], "mean_to": b["mean"],
                                                      "std_from": a["std"], "std_to": b["std"]})})
    conn.close()
    return changes

def latest_run_id(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else ""

def export_json_from_db(db_path: str, json_path: str, run_id: str | None = None):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    if not run_id:
        run_id = latest_run_id(conn)

    result: Dict[str, Any] = {"run_id": run_id, "nodes": {}, "edges": {}}

    for table, key in [
        ("datasets", "datasets"),
        ("columns", "columns"),
        ("transforms", "transforms"),
    ]:
        cur.execute(f"SELECT * FROM {table} WHERE run_id=?", (run_id,))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        result["nodes"][key] = rows

    for table, key in [
        ("dataset_to_transform_edges", "dataset_to_transform"),
        ("transform_to_dataset_edges", "transform_to_dataset"),
        ("column_to_transform_edges", "column_to_transform"),
        ("transform_to_column_edges", "transform_to_column"),
    ]:
        cur.execute(f"SELECT * FROM {table} WHERE run_id=?", (run_id,))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        result["edges"][key] = rows

    conn.close()

    with open(json_path, "w") as f:
        json.dump(result, f)