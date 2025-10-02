# LineageKit

Data lineage & impact analyzer for Python pipelines.  
Capture **column-level provenance** from pandas/ML code via lightweight decorators + static AST hints, persist to **SQLite/JSON**, explore an interactive **DAG UI**, and run **diff**/**impact** analyses to prevent breakage.

## ✨ Features

- **Runtime lineage**: `@dataset` & `@transform` record dataset/column nodes and edges (incl. passthrough/rename/derived).
- **Static assist (AST)**: infers edges from common pandas patterns (`.assign`, `.rename`, `df["a"] + df["b"]`).
- **Store**: SQLite schema + JSON export for portability/time-travel.
- **CLI**: `lineagekit run|export|ui|diff|impact` to integrate with any pipeline.
- **Impact analysis**: column-level BFS with severity scoring (schema/type/null/value changes).
- **UI**: Streamlit DAG explorer (dataset-level & column-level views).
- **sklearn helpers**: lineage for `OneHotEncoder`/`StandardScaler`.
- **Guardrail (optional)**: pre-commit CLI to block risky changes.
- **Privacy**: optional redaction hooks for dataset names/samples.

---

## 📦 Install (from source)

Requirements: Python **3.9–3.12**

```bash
# from repo root
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -e .         # installs console script `lineagekit`
```

If you keep the UI inside the package at `lineagekit/ui/streamlit_app.py`, make sure you have `lineagekit/ui/__init__.py` (can be empty).

---

## 🧭 Quickstart

Create two runs (baseline vs changed) and explore:

```bash
# Baseline run
LINEAGE_VARIANT=A lineagekit run examples/test_runs.py --db lineage.db --json runA.json

# Changed run (type change + null spike + optional schema add)
LINEAGE_VARIANT=B lineagekit run examples/test_runs.py --db lineage.db --json runB.json

# Export latest run to JSON
lineagekit export --db lineage.db --json lineage_latest.json

# Launch the UI
lineagekit ui --db lineage.db
```

---

## 🗂️ Repo layout (suggested)

```
lineagekit/
├── lineagekit/
│   ├── __init__.py
│   ├── cli.py
│   ├── lineage_tracker.py         # tracker + node/edge dataclasses
│   ├── store.py                   # SQLite DDL + persist/export
│   ├── dataset.py                 # @dataset decorator
│   ├── transform.py               # @transform decorator (AST assist merge)
│   ├── ast_assist.py              # static patterns for pandas
│   ├── sklearn_helpers.py         # OneHotEncoder / StandardScaler helpers
│   └── ui/
│       ├── __init__.py
│       └── streamlit_app.py
├── examples/
│   └── test_runs.py               # small demo pipeline
├── pyproject.toml
├── README.md
└── .gitignore
```

---

## 🧪 Minimal example

```python
# examples/test_runs.py
import os, pandas as pd
from lineagekit.dataset import dataset
from lineagekit.transform import transform

VARIANT = os.getenv("LINEAGE_VARIANT", "A")

@dataset(name="orders_raw", io="read", fmt="csv")
def load_orders():
    df = pd.DataFrame({
        "order_id":[1,2,3], "qty":[2,1,5], "price":[10.0,5.0,7.5], "cust_id":[100,200,100]
    })
    if VARIANT == "B":
        df["qty"] = df["qty"].astype(float)   # TYPE_CHANGE
        df.loc[[1],"price"] = None            # NULL_SPIKE
    return df

@transform(
  name="clean_orders",
  produces="orders_cleaned",
  passthrough=["order_id","qty","price"],
  rename={"cust_id":"customer_id"},
  derives={"total":["qty","price"]}
)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={"cust_id":"customer_id"}).copy()
    out["total"] = out["qty"] * out["price"]
    return out

@dataset(name="orders_sink", io="write", fmt="parquet")
def write_orders(df: pd.DataFrame, out_path: str):
    pass

if __name__ == "__main__":
    write_orders(clean(load_orders()), "data/clean/orders.parquet")
```

Run it via CLI (persists to SQLite, optional JSON):

```bash
lineagekit run examples/test_runs.py --db lineage.db --json run.json
lineagekit ui --db lineage.db
```

---

## 🧰 CLI

```bash
lineagekit --help
```

**Run a pipeline and persist lineage**
```bash
lineagekit run path/to/script.py --db lineage.db [--json lineage_run.json]
```

**Export latest run to JSON**
```bash
lineagekit export --db lineage.db --json lineage_latest.json
```

**Open the DAG UI**
```bash
lineagekit ui --db lineage.db
```

**Diff two runs (schema/type/value/null)**
```bash
# find run ids
sqlite3 lineage.db "SELECT run_id, datetime(created_at,'unixepoch') FROM runs ORDER BY created_at;"
# compare
lineagekit diff --db lineage.db --base RUN_A --curr RUN_B --save 1
```

**Impact analysis (blast radius from a column)**
```bash
# list column ids for a run
python - <<'PY'
import sqlite3, sys
conn=sqlite3.connect("lineage.db"); cur=conn.cursor()
run_id=sys.argv[1] if len(sys.argv)>1 else cur.execute("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()[0]
for r in cur.execute("SELECT c.id, d.name||'.'||c.name FROM columns c JOIN datasets d ON c.dataset_id=d.id WHERE c.run_id=?", (run_id,)):
    print(r[0], r[1])
PY

# compute impact for a change type
lineagekit impact "<COLUMN_ID>" --change type_change --db lineage.db --run RUN_B
```

---

## 🧱 How it works

- **Nodes**: Datasets (source/temp/sink), Columns, Transforms.
- **Edges**:
  - Dataset→Transform, Transform→Dataset
  - Column→Transform, Transform→Column
- **Runtime capture**:
  - `@dataset(io="read")` tags DataFrame with `__ds_id__` and registers nodes/columns/stats.
  - `@transform` registers output dataset/columns + transform node and edges:
    - passthrough (same name),
    - rename mapping,
    - derived columns (declared and/or inferred).
- **Static assist**: `ast_assist.analyze_transform_source()` parses the function body to infer:
  - `.assign(new=expr)` → inputs of `expr`
  - `.rename(columns={old:new})`
  - `df["x"] + df["y"] → "out"` patterns
  User hints override static hints.
- **Store**: `store.persist_current_run()` creates tables and inserts nodes/edges/stats; `export_json_from_db()` writes a coherent JSON for visualization.
- **Diff**: compares `column_stats` between runs to detect `schema_add/drop`, `type_change`, `null_spike`, `value_shift` (mean/std drift).
- **Impact**: BFS over column-level graph, escalating severity at transforms/sinks.

---

## 🖥️ UI

- Streamlit app draws the DAG (NetworkX spring layout) with toggles for **Dataset-level** vs **Column-level** view.
- Side panel shows current **run_id**, optional run selector, and (optionally) an impact overlay.

Launch:
```bash
lineagekit ui --db lineage.db
```

---

## 🤖 sklearn helpers

```python
from lineagekit.sklearn_helpers import one_hot, standardize

ohe = one_hot(cleaned, cols=["customer_id"])   # maps each OHE feature → source col
sc  = standardize(cleaned, cols=["total"])     # maps total → total_scaled
```
