from typing import List, Optional
import time, hashlib
import pandas as pd
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .lineage_tracker import tracker, _get_id, _col_id, DatasetNode, ColumnNode, TransformNode, DatasetToTransformEdge, TransformToDatasetEdge, ColToTransformEdge, TransformToColEdge

def one_hot(df: pd.DataFrame, cols: List[str], *, name="onehot", produces="features_onehot",
            handle_unknown="ignore", drop=None):
    t0 = time.time()
    in_ds_id = df.attrs.get("__ds_id__") or _get_id("anon", "sk_in")
    # synthesize dataset if missing
    if "__ds_id__" not in df.attrs:
        node = DatasetNode(id=in_ds_id, name="sk_input", kind="temp", fmt=None, path=None,
                           code_file="<runtime>", code_line=0, rows=len(df),
                           run_id=tracker.run_id, created_at=t0)
        tracker.insert_dataset(node)
        cols0 = [ColumnNode(id=_col_id(in_ds_id, str(c)), dataset_id=in_ds_id, name=str(c),
                            dtype=str(df[c].dtype), run_id=tracker.run_id) for c in df.columns]
        tracker.insert_columns(cols0)
        df.attrs["__ds_id__"] = in_ds_id

    enc = OneHotEncoder(sparse_output=False, handle_unknown=handle_unknown, drop=drop)
    X = enc.fit_transform(df[cols])
    out_names = enc.get_feature_names_out(cols).tolist()
    out = pd.DataFrame(X, index=df.index, columns=out_names)

    out_ds_id = _get_id("ds", produces)
    node = DatasetNode(id=out_ds_id, name=produces, kind="temp", fmt=None, path=None,
                       code_file="<runtime>", code_line=0, rows=len(out),
                       run_id=tracker.run_id, created_at=t0)
    tracker.insert_dataset(node)
    tracker.insert_columns([
        ColumnNode(id=_col_id(out_ds_id, c), dataset_id=out_ds_id, name=c,
                   dtype=str(out[c].dtype), run_id=tracker.run_id) for c in out.columns
    ])
    out.attrs["__ds_id__"] = out_ds_id

    tr_id = _get_id("tr", f"sklearn.OneHotEncoder", name)
    tr = TransformNode(id=tr_id, name=f"{name}", code_file="<runtime>", code_line=0,
                       params_hash=hashlib.sha1(repr((cols, handle_unknown, drop)).encode()).hexdigest()[:12],
                       run_id=tracker.run_id, created_at=t0)
    tracker.insert_transform(tr)
    tracker.insert_dataset_to_transform([DatasetToTransformEdge(src_ds_id=in_ds_id, transform_id=tr_id, run_id=tracker.run_id)])
    tracker.insert_transform_to_dataset([TransformToDatasetEdge(transform_id=tr_id, dest_ds_id=out_ds_id, run_id=tracker.run_id)])

    for outc in out_names:
        src_col = outc.split("_", 1)[0]
        if src_col not in cols:
            src_col = next((c for c in cols if outc.startswith(c)), src_col)
        tracker.insert_col_to_transform([ColToTransformEdge(src_col_id=src_col, transform_id=tr_id, run_id=tracker.run_id)])
        tracker.insert_transform_to_col([TransformToColEdge(transform_id=tr_id, dest_col_id=src_col, run_id=tracker.run_id)])

    return out

def standardize(df: pd.DataFrame, cols: List[str], *, name="standardize", produces="features_scaled", suffix="_scaled"):
    t0 = time.time()
    in_ds_id = df.attrs.get("__ds_id__") or _get_id("anon", "sk_in")
    if "__ds_id__" not in df.attrs:
        node = DatasetNode(id=in_ds_id, name="sk_input", kind="temp", fmt=None, path=None,
                           code_file="<runtime>", code_line=0, rows=len(df),
                           run_id=tracker.run_id, created_at=t0)
        tracker.insert_dataset(node)
        tracker.insert_columns([
            ColumnNode(id=_col_id(in_ds_id, str(c)), dataset_id=in_ds_id, name=str(c),
                       dtype=str(df[c].dtype), run_id=tracker.run_id) for c in df.columns
        ])
        df.attrs["__ds_id__"] = in_ds_id

    scaler = StandardScaler()
    X = scaler.fit_transform(df[cols])
    out_cols = [f"{c}{suffix}" for c in cols]
    out = pd.DataFrame(X, index=df.index, columns=out_cols)

    out_ds_id = _get_id("ds", produces)
    node = DatasetNode(id=out_ds_id, name=produces, kind="temp", fmt=None, path=None,
                       code_file="<runtime>", code_line=0, rows=len(out),
                       run_id=tracker.run_id, created_at=t0)
    tracker.add_dataset(node)
    tracker.insert_columns([
        ColumnNode(id=_col_id(out_ds_id, c), dataset_id=out_ds_id, name=c,
                   dtype=str(out[c].dtype), run_id=tracker.run_id) for c in out.columns
    ])
    out.attrs["__ds_id__"] = out_ds_id

    tr_id = _get_id("tr", f"sklearn.StandardScaler", name)
    tr = TransformNode(id=tr_id, name=name, code_file="<runtime>", code_line=0,
                       params_hash=hashlib.sha1(repr(cols).encode()).hexdigest()[:12],
                       run_id=tracker.run_id, created_at=t0)
    tracker.insert_transform(tr)
    tracker.insert_dataset_to_transform([DatasetToTransformEdge(src_ds_id=in_ds_id, transform_id=tr_id, run_id=tracker.run_id)])
    tracker.insert_transform_to_dataset([TransformToDatasetEdge(transform_id=tr_id, dest_ds_id=out_ds_id, run_id=tracker.run_id)])

    for src, dest in zip(cols, out_cols):
        tracker.insert_col_to_transform([ColToTransformEdge(src_col_id=src, transform_id=tr_id, run_id=tracker.run_id)])
        tracker.insert_transform_to_col([TransformToColEdge(transform_id=tr_id, dest_col_id=dest, run_id=tracker.run_id)])

    return out
