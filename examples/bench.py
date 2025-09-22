import time, json, random, sqlite3
from lineagekit.cli import run as cli_run  # or just subprocess call to your CLI
from lineagekit.store import export_json_from_db
from pathlib import Path
import subprocess, sys

def bench_run(script, db, label):
    t0 = time.perf_counter()
    subprocess.check_call([sys.executable, "-m", "lineagekit.cli", "run", script, "--db", db])
    t1 = time.perf_counter()
    subprocess.check_call([sys.executable, "-m", "lineagekit.cli", "export", "--db", db, "--json", f"{label}.json"])
    t2 = time.perf_counter()
    return {"label": label, "run_s": t1-t0, "export_s": t2-t1}

def bench_impact(db, run_id, starts, change="type_change"):
    from lineagekit.impact import impact_bfs
    t0 = time.perf_counter()
    n=0
    for s in starts:
        impact_bfs(db, run_id, s, change); n+=1
    t1 = time.perf_counter()
    return {"impact_avg_ms": (t1-t0)*1000/max(n,1), "samples": n}

if __name__ == "__main__":
    db=""
    # run A and B
    subprocess.check_call(["env","LINEAGE_VARIANT=A",sys.executable,"-m","lineagekit.cli","run","test_runs.py","--db",db])
    subprocess.check_call(["env","LINEAGE_VARIANT=B",sys.executable,"-m","lineagekit.cli","run","test_runs.py","--db",db])
    # latest run id
    conn=sqlite3.connect(db); cur=conn.cursor()
    cur.execute("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1"); run_id=cur.fetchone()[0]
    cols=[r[0] for r in cur.execute("SELECT id FROM columns WHERE run_id=?",(run_id,))]
    conn.close()
    res1 = bench_run("test_runs.py", db, "bench")
    res2 = bench_impact(db, run_id, random.sample(cols, min(20, len(cols))))
    print(json.dumps({**res1, **res2}, indent=2))