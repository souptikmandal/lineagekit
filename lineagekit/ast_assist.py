import ast
from typing import Dict, List, Set, Tuple

# extract column names from df["cols"] or df.col
def _extract_cols(node: ast.AST, df_names: Set[str]):
    cols = set()
    class V(ast.NodeVisitor):
        def visit_Subscript(self, n: ast.Subscript):
            base = getattr(n.value, "id", None) or getattr(n.value, "attr", None)
            if base in df_names:
                key = None
                if isinstance(n.slice, ast.Constant): key = n.slice.value
                elif isinstance(n.slice, ast.Str): key = n.slice.s
                if isinstance(key, str): cols.add(key)
            self.generic_visit(n)

        def visit_Attribute(self, n: ast.Attribute):
            base = getattr(n.value, "id", None) or getattr(n.value, "attr", None)
            if base in df_names and isinstance(n.attr, str):
                cols.add(n.attr)
            self.generic_visit(n)
    V().visit(node)
    return cols

def analyze_transform_source(src: str, df_param_names: List[str] = ["df"]):
    """
    :param src: transform function
    :param df_param_names: names
    :return: (rename_map, derives_map) from Python source of a @transform function
    """

    tree = ast.parse(src)
    df_names = set(df_param_names)

    rename: Dict[str, str] = {}
    derives: Dict[str, List[str]] = {}

    class V(ast.NodeVisitor):
        def visit_Call(self, n: ast.Call):
            if isinstance(n.func, ast.Attribute) and n.func.attr == "rename":
                for kw in n.keywords or []:
                    if kw.arg == "columns" and isinstance(kw.value, (ast.Dict,)):
                        for k, v in zip(kw.value.keys, kw.value.values):
                            if isinstance(k, (ast.Str, ast.Constant)) and isinstance(v, (ast.Str, ast.Constant)):
                                old = k.s if isinstance(k, ast.Str) else k.value
                                new = v.s if isinstance(v, ast.Str) else v.value
                                if isinstance(old, str) and isinstance(new, str):
                                    rename[old] = new
            if isinstance(n.func, ast.Attribute) and n.func.attr == "assign":
                for kw in n.keywords or []:
                    new_col = kw.arg
                    if isinstance(new_col, str):
                        inputs = sorted(_extract_cols(kw.value, df_names))
                        if inputs:
                            derives.setdefault(new_col, inputs)
                self.generic_visit(n)

        def visit_Assign(self, n: ast.Assign):
            if len(n.targets) == 1 and isinstance(n.targets[0], ast.Subscript):
                tgt = n.targets[0]
                key = None
                if isinstance(tgt.slice, ast.Constant): key = tgt.slice.value
                elif isinstance(tgt.slice, ast.Str): key = tgt.slice.s
                if isinstance(key, str):
                    inputs = sorted(_extract_cols(n.value, df_names))
                    if inputs:
                        derives.setdefault(key, inputs)
            self.generic_visit(n)

    V().visit(tree)
    return rename, derives