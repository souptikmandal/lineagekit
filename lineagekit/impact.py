from collections import deque, defaultdict
import sqlite3, json

SEV_RANK = {"LOW":1,"MEDIUM":2,"HIGH":3,"CRITICAL":4}

def severity_for(change_type: str, tr_tags: list[str]):
    if change_type in ("schema_drop", "type_change"): return "CRITICAL"
    if any(t in tr_tags for t in ("agg", "model", "sklearn")): return "MEDIUM"
    return "LOW"

def impact_bfs(db_path: str, run_id: str, start_col_id: str, change_type: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT src_col_id, transform_id FROM column_to_transform_edges WHERE run_id=?", (run_id,))
    col_to_tr = defaultdict(list)
    for c, t in cur.fetchall(): col_to_tr[c].append(t)
    cur.execute("SELECT transform_id, dest_col_id FROM transform_to_column_edges WHERE run_id=?", (run_id,))
    tr_to_col = defaultdict(list)
    for t, c in cur.fetchall(): tr_to_col[t].append(c)

    tr_tags = defaultdict(list)
    try:
        cur.execute("SELECT id, params_hash, name FROM transforms WHERE run_id=?", (run_id,))
        for tid, _, _ in cur.fetchall():
            tr_tags[tid] = []
    except Exception:
        pass

    q = deque([(start_col_id, "LOW")])
    best = {start_col_id: "LOW"}
    hits = []  # (node_id, kind, severity)

    while q:
        node, sev = q.popleft()
        # step to transforms
        for tr in col_to_tr.get(node, []):
            s = severity_for(change_type, tr_tags[tr])
            if SEV_RANK[s] > SEV_RANK[best.get(tr, "LOW")]:
                best[tr] = s
                q.append((tr, s))
                hits.append((tr, "transform", s))
            # step to produced columns
            for outc in tr_to_col.get(tr, []):
                if SEV_RANK[s] > SEV_RANK[best.get(outc, "LOW")]:
                    best[outc] = s
                    q.append((outc, s))
                    hits.append((outc, "column", s))
    conn.close()

    return hits