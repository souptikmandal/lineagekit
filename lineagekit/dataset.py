from typing import Literal, Optional
import time, functools, inspect
import pandas as pd

from .lineage_tracker import tracker, _get_id, DatasetNode, ColumnNode, _stats_for

def dataset(name: str,
            io: Literal["read", "write"],
            fmt: Optional[str] = None,
            path: Optional[str] = None):
    def decorator(func):
        source_file = inspect.getsourcefile(func) or "<unknown>"
        source_line = inspect.getsourcelines(func)[1] if inspect.getsourcelines(func) else None
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            t0 = time.time()

            res = func(*args, **kwargs)

            try:
                bound = sig.bind_partial(*args, **kwargs)
                bound.apply_defaults()
            except Exception:
                bound = None

            if bound:
                runtime_path = path or bound.arguments.get("path") or bound.arguments.get("out_path")
            else:
                runtime_path = path

            if io == "read":
                df = res
                if isinstance(df, pd.DataFrame):
                    dataset_id = _get_id("read", name, path or source_file, fmt)
                    dataset_node = DatasetNode(id=dataset_id,
                                               name=name,
                                               kind="source",
                                               fmt=fmt,
                                               path=path,
                                               code_file=source_file,
                                               code_line=source_line,
                                               rows=len(df),
                                               run_id=tracker.run_id,
                                               created_at=t0)
                    tracker.insert_dataset(dataset_node)
                    df.attrs["__ds_id__"] = dataset_id
                    cols = [ColumnNode(id=_get_id(dataset_id, col),
                                       name=str(col),
                                       dataset_id=dataset_id,
                                       dtype=str(df[col].dtype),
                                       run_id=tracker.run_id)
                            for col in df.columns]
                    tracker.insert_columns(cols)
                    _stats_for(df, dataset_id)
                return res

            elif io == "write":
                df = None
                if bound:
                    df = bound.arguments.get("df")
                if df is None:
                    for a in list(args) + list(kwargs.values()):
                        if isinstance(a, pd.DataFrame):
                            df = a
                            break
                if isinstance(df, pd.DataFrame):
                    dataset_id = _get_id("write", name, runtime_path or source_file, fmt)
                    dataset_node = DatasetNode(id=dataset_id,
                                               name=name,
                                               kind="sink",
                                               fmt=fmt,
                                               path=runtime_path,
                                               code_file=source_file,
                                               code_line=source_line,
                                               rows=len(df),
                                               run_id=tracker.run_id,
                                               created_at=t0)
                    tracker.insert_dataset(dataset_node)
                    df.attrs["__ds_id__"] = dataset_id
                    cols = [ColumnNode(id=_get_id(dataset_id, col),
                                       name=str(col),
                                       dataset_id=dataset_id,
                                       dtype=str(df[col].dtype),
                                       run_id=tracker.run_id)
                            for col in df.columns]
                    tracker.insert_columns(cols)
                    _stats_for(df, dataset_id)
                return res

            else:
                return res
        return wrapper
    return decorator