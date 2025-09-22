from typing import Dict, Optional, List
import time, functools, inspect
import pandas as pd

from .ast_assist import analyze_transform_source
from .lineage_tracker import tracker, TransformNode, DatasetNode, ColumnNode, ColToTransformEdge, TransformToColEdge, TransformToDatasetEdge, DatasetToTransformEdge, _params_hash, _get_id, _col_id, _ensure_dataset_node_from_df, _stats_for

def transform(name: str,
              produces: str,
              passthrough: Optional[List[str]] = None,
              rename: Optional[Dict[str, str]] = None,
              derives: Optional[Dict[str, List[str]]] = None):

    passthrough = passthrough or []
    rename = rename or {}
    derives = derives or {}

    def decorator(func):
        source_file = inspect.getsourcefile(func) or "<unknown>"
        source_line = inspect.getsourcelines(func)[1] if inspect.getsourcelines(func) else None
        p_hash = _params_hash({"passthrough": passthrough, "rename": rename, "derives": derives})

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            t0 = time.time()

            df_in = None
            for a in list(args) + list(kwargs.values()):
                if isinstance(a, pd.DataFrame):
                    df_in = a
                    break
            if df_in is None:
                raise ValueError("@transform expects a pandas DataFrame argument")

            in_ds_id = _ensure_dataset_node_from_df(df_in, fallback_name=f"{name}_input")

            df_out = func(*args, **kwargs)
            if not isinstance(df_out, pd.DataFrame):
                return df_out

            out_ds_id = _get_id("ds", produces, source_file, str(source_line))
            out_node = DatasetNode(id=out_ds_id,
                                   name=produces,
                                   kind="temp",
                                   fmt=None,
                                   path=None,
                                   code_file=source_file,
                                   code_line=source_line,
                                   rows=len(df_out),
                                   run_id=tracker.run_id,
                                   created_at=t0)
            tracker.insert_dataset(out_node)
            out_cols = [ColumnNode(id=_get_id(out_ds_id, col),
                                    name=str(col),
                                    dataset_id=out_ds_id,
                                    dtype=str(df_out[col].dtype),
                                    run_id=tracker.run_id)
                         for col in df_out.columns]
            tracker.insert_columns(out_cols)
            df_out.attrs["__ds_id__"] = out_ds_id
            _stats_for(df_out, out_ds_id)

            try:
                src = inspect.getsource(func)
                static_rename, static_derives = analyze_transform_source(src, df_param_names=["df"])
            except Exception:
                static_rename, static_derives = {}, {}

            eff_rename = {**static_rename, **rename}
            eff_derives = {**static_rename, **rename}

            if not passthrough:
                common = set(df_in.columns) & set(df_out.columns)
                inferred_passthrough = sorted(
                    c for c in common
                    if c not in eff_rename.keys() and c not in eff_derives.keys()
                )
            else:
                inferred_passthrough = passthrough

            transform_id = _get_id("tr", name, source_file, str(source_line), p_hash)
            transform_node = TransformNode(id=transform_id,
                                           name=name,
                                           code_file=source_file,
                                           code_line=source_line,
                                           params_hash=p_hash,
                                           run_id=tracker.run_id,
                                           created_at=t0)
            tracker.insert_transform(transform_node)
            tracker.insert_dataset_to_transform([DatasetToTransformEdge(src_ds_id=in_ds_id, transform_id=transform_id, run_id=tracker.run_id)])
            tracker.insert_transform_to_dataset([TransformToDatasetEdge(transform_id=transform_id, dest_ds_id=out_ds_id, run_id=tracker.run_id)])

            for c in inferred_passthrough:
                in_cid = _col_id(in_ds_id, c)
                out_cid = _col_id(out_ds_id, c)
                tracker.insert_col_to_transform([ColToTransformEdge(src_col_id=in_cid, transform_id=transform_id, run_id=tracker.run_id)])
                tracker.insert_transform_to_col([TransformToColEdge(transform_id=transform_id, dest_col_id=out_cid, run_id=tracker.run_id)])

            for old, new in eff_rename.items():
                in_cid = _col_id(in_ds_id, old)
                out_cid = _col_id(out_ds_id, new)
                tracker.insert_col_to_transform([ColToTransformEdge(src_col_id=in_cid, transform_id=transform_id, run_id=tracker.run_id)])
                tracker.insert_transform_to_col([TransformToColEdge(transform_id=transform_id, dest_col_id=out_cid, run_id=tracker.run_id)])

            for new, inputs in eff_derives.items():
                out_cid = _col_id(out_ds_id, new)
                tracker.insert_transform_to_col([TransformToColEdge(transform_id=transform_id, dest_col_id=out_cid, run_id=tracker.run_id)])
                for src in inputs:
                    in_cid = _col_id(in_ds_id, src)
                    tracker.insert_col_to_transform([ColToTransformEdge(src_col_id=in_cid, transform_id=transform_id, run_id=tracker.run_id)])

            return df_out
        return wrapper
    return decorator