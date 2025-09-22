import sys, argparse, sqlite3
import streamlit as st
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

st.set_page_config(page_title="LineageKit DAG", layout="wide")

parser = argparse.ArgumentParser()
parser.add_argument("--db", required=True, help="Path to SQLite DB")
args, _ = parser.parse_known_args()

@st.cache_data
def load_graph(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT run_id, created_at FROM runs ORDER BY created_at DESC")
    rows = cur.fetchall()
    run_id = rows[0][0] if rows else ""

    data = {"run_id": run_id, "datasets": [], "columns": [], "transforms": [], "edges": {}}

    for table, key in [
        ("datasets", "datasets"),
        ("columns", "columns"),
        ("transforms", "transforms"),
    ]:
        cur.execute(f"SELECT * FROM {table} WHERE run_id=?", (run_id,))
        cols = [c[0] for c in cur.description]
        data[key] = [dict(zip(cols, r)) for r in cur.fetchall()]

    for table, key in [
        ("dataset_to_transform_edges", "dataset_to_transform"),
        ("transform_to_dataset_edges", "transform_to_dataset"),
        ("column_to_transform_edges", "column_to_transform"),
        ("transform_to_column_edges", "transform_to_column"),
    ]:
        cur.execute(f"SELECT * FROM {table} WHERE run_id=?", (run_id,))
        cols = [c[0] for c in cur.description]
        data.setdefault("edges", {})[key] = [dict(zip(cols, r)) for r in cur.fetchall()]

    conn.close()
    return data

data = load_graph(args.db)

st.sidebar.title("LineageKit")
st.sidebar.write(f"Run: `{data['run_id']}`")
view_mode = st.sidebar.radio("View", ["Dataset-level", "Column-level"], index=0)

runs = pd.read_sql("SELECT run_id, created_at FROM runs ORDER BY created_at DESC", sqlite3.connect(args.db))
sel = st.sidebar.selectbox("Run", runs["run_id"])

G = nx.DiGraph()
if view_mode == "Dataset-level":
    for d in data["datasets"]:
        G.add_node(d['id'], label=f"DS: {d['name']} ({d['kind']})", kind="dataset")
    for t in data["transforms"]:
        G.add_node(t['id'], label=f"TR: {t['name']}", kind="transform")
    for e in data["edges"].get("dataset_to_transform", []):
        G.add_edge(e["src_dataset_id"], e["transform_id"])
    for e in data["edges"].get("transform_to_dataset", []):
        G.add_edge(e["transform_id"], e["dest_dataset_id"])
else:
    for c in data["columns"]:
        ds_name = next((d["name"] for d in data["datasets"] if d["id"] == c["dataset_id"]), "")
        G.add_node(c["id"], label=f"{ds_name}.{c['name']}", kind="column")
    for t in data["transforms"]:
        G.add_node(t["id"], label=f"TR: {t['name']}", kind="transform")
    for e in data["edges"].get("column_to_transform", []):
        G.add_edge(e["src_col_id"], e["transform_id"])
    for e in data["edges"].get("transform_to_column", []):
        G.add_edge(e["transform_id"], e["dest_col_id"])

pos = nx.spring_layout(G, seed=7, k=0.7)

fig = plt.figure(figsize=(10, 7))
ax = plt.gca()
ax.axis("off")

for (u, v) in G.edges():
    x1, y1 = pos[u]
    x2, y2 = pos[v]
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="-|>", lw=1))

for n, (x, y) in pos.items():
    label = G.nodes[n].get("label", n[:6])
    kind = G.nodes[n].get("kind", "other")
    ax.scatter([x], [y], s=220 if kind == "transform" else 160)
    ax.text(x, y, label, ha='center', va='center', fontsize=8)

st.pyplot(fig, clear_figure=True)

with st.expander("Datasets"):
    st.dataframe(pd.DataFrame(data["datasets"]))
with st.expander("Columns"):
    st.dataframe(pd.DataFrame(data["columns"]))
with st.expander("Transforms"):
    st.dataframe(pd.DataFrame(data["transforms"]))