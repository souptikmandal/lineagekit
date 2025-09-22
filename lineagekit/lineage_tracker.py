from dataclasses import dataclass, asdict
from typing import Literal, Optional, Dict, List, Any
from enum import Enum
import time, hashlib
import pandas as pd

@dataclass
class DatasetNode:
    id: str
    name: str
    kind: Literal["source", "temp", "sink"]
    fmt: Optional[str]
    path: Optional[str]
    code_file: str
    code_line: int
    rows: Optional[int]
    run_id: str
    created_at: float

@dataclass
class ColumnNode:
    id: str
    name: str
    dataset_id: str
    dtype: str
    run_id: str

@dataclass
class TransformNode:
    id: str
    name: str
    code_file: str
    code_line: int
    params_hash: str
    run_id: str
    created_at: float

@dataclass
class ColToTransformEdge:
    src_col_id: str
    transform_id: str
    run_id: str

@dataclass
class TransformToColEdge:
    transform_id: str
    dest_col_id: str
    run_id: str

@dataclass
class DatasetToTransformEdge:
    src_ds_id: str
    transform_id: str
    run_id: str

@dataclass
class TransformToDatasetEdge:
    transform_id: str
    dest_ds_id: str
    run_id: str

@dataclass
class ColumnStats:
    dataset_id: str
    column: str
    dtype: str
    count: int
    nulls: int
    mean: float | None
    std: float | None
    top: str | None
    top_freq: int | None
    run_id: str

class ChangeType(str, Enum):
    SCHEMA_ADD = "schema_add"
    SCHEMA_DROP = "schema_drop"
    TYPE_CHANGE = "type_change"
    NULL_SPIKE = "null_spike"
    VALUE_SHIFT = "value_shift"

def _get_id(*parts):
    key = "|".join([x or "" for x in parts])
    return hashlib.sha1(key.encode()).hexdigest()[:16]

def _col_id(ds_id: str, col: str):
    return _get_id(ds_id, col)

def _params_hash(d: Dict[str, Any]):
    return hashlib.sha1(repr(sorted(d.items())).encode()).hexdigest()[:12]

def _ensure_dataset_node_from_df(df: pd.DataFrame, fallback_name: str) -> str:
    ds_id = df.attrs.get("__ds_id__")
    if ds_id:
        return ds_id
    # If the input DF wasn't produced by @dataset/@transform, synthesize a node
    ds_id = _get_id("anon", fallback_name)
    node = DatasetNode(
        id=ds_id, name=fallback_name, kind="temp", fmt=None, path=None,
        code_file="<runtime>", code_line=0, rows=len(df),
        run_id=tracker.run_id, created_at=time.time()
    )
    tracker.insert_dataset(node)
    # register columns
    cols = [ColumnNode(id=_col_id(ds_id, c), dataset_id=ds_id, name=str(c),
                       dtype=str(df[c].dtype), run_id=tracker.run_id)
            for c in df.columns]
    tracker.insert_columns(cols)
    df.attrs["__ds_id__"] = ds_id
    return ds_id

def _stats_for(df: pd.DataFrame, ds_id: str):
    for c in df.columns:
        s = df[c]
        if s.dtype.kind in "biufc":
            mean = float(s.mean()) if s.size else None
            std = float(s.std(ddof=1)) if s.size > 1 else None
            top = top_freq = None
        else:
            vc = s.value_counts(dropna=True)
            top = str(vc.index[0]) if len(vc) else None
            top_freq = int(vc.iloc[0]) if len(vc) else None
            mean = std = None
        tracker.column_stats.append(ColumnStats(dataset_id=ds_id, column=c, dtype=str(s.dtype),
                                                count=int(s.size), nulls=int(s.isna().sum()),
                                                mean=mean, std=std, top=top, top_freq=top_freq,
                                                run_id=tracker.run_id))

class LineageTracker:
    def __init__(self):
        self.run_id = f"run_{int(time.time())}"
        self.datasets: Dict[str, DatasetNode] = {}
        self.transforms: Dict[str, TransformNode] = {}
        self.columns: List[ColumnNode] = []
        self.col_to_transform: List[ColToTransformEdge] = []
        self.transform_to_col: List[TransformToColEdge] = []
        self.dataset_to_transform: List[DatasetToTransformEdge] = []
        self.transform_to_dataset: List[TransformToDatasetEdge] = []
        self.column_stats: List[ColumnStats] = []

    def insert_dataset(self, node: DatasetNode):
        self.datasets[node.id] = node

    def insert_columns(self, cols: List[ColumnNode]):
        self.columns.extend(cols)

    def insert_transform(self, node: TransformNode):
        self.transforms[node.id] = node

    def insert_col_to_transform(self, e: List[ColToTransformEdge]):
        self.col_to_transform.extend(e)

    def insert_transform_to_col(self, e: List[TransformToColEdge]):
        self.transform_to_col.extend(e)

    def insert_dataset_to_transform(self, e: List[DatasetToTransformEdge]):
        self.dataset_to_transform.extend(e)

    def insert_transform_to_dataset(self, e: List[TransformToDatasetEdge]):
        self.transform_to_dataset.extend(e)

    def export_json(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "nodes": {
                "datasets": [asdict(d) for d in self.datasets.values()],
                "columns": [asdict(c) for c in self.columns],
                "transforms": [asdict(t) for t in self.transforms.values()],
            },
            "edges": {
                "dataset_to_transform": [asdict(e) for e in self.dataset_to_transform],
                "transform_to_dataset": [asdict(e) for e in self.transform_to_dataset],
                "column_to_transform": [asdict(e) for e in self.col_to_transform],
                "transform_to_column": [asdict(e) for e in self.transform_to_col],
            },
        }

tracker = LineageTracker()