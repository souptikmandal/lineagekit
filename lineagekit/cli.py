import sqlite3

from rich import print
from pathlib import Path
import typer
import runpy
import subprocess
from importlib.resources import files

from .lineage_tracker import tracker
from .impact import impact_bfs, SEV_RANK
from .store import persist_current_run, export_json_from_db, detect_changes, latest_run_id
from .ui import streamlit_app_path

app = typer.Typer(help="Lineage: run pipelines, persist lineage, and view the DAG")

@app.command()
def run(script: str = typer.Argument(..., help="Path to your pipeline script"),
        db: str = typer.Option("lineage.db", "--db", help="SQLite DB path"),
        json_out: str = typer.Option("", "--json", help="Optional: export run to JSON file after")):
    script_path = Path(script)
    if not script_path.exists():
        raise typer.BadParameter(f"{script} not found")
    print(f"[bold]> Running[/bold] {script_path}")

    runpy.run_path(str(script_path), run_name="__main__")
    print(f"[green]✓ Script finished[/green]; run_id={tracker.run_id}")

    persist_current_run(db)
    print(f"[green]✓ Persisted[/green] to {db}")

    if json_out:
        export_json_from_db(db, json_out)
        print(f"[green]✓ Exported to JSON[/green] to {json_out}")

@app.command()
def export(db: str = typer.Option("lineage.db", "--db"),
           json_out: str = typer.Option("lineage_run.json", "--json")):
    export_json_from_db(db, json_out)
    print(f"[green]✓ Exported to JSON[/green] to {json_out}")

@app.command()
def ui(db: str = typer.Option("lineage.db", "--db")):
    ui_file = streamlit_app_path()
    subprocess.run(["streamlit", "run", str(ui_file), "--", "--db", db])

@app.command()
def diff(base: str, curr: str, db: str = typer.Option("lineage.db", "--db"),
         save: str = typer.Option("", "--save", help="Optional: persist to 'changes' table")):
    changes = detect_changes(db, base, curr)
    for ch in changes:
        print(f"[bold]{ch['change_type']}[/bold] {ch['node_id']} sev={ch['severity']} detail={ch['detail']}")
    if save:
        conn = sqlite3.connect(db)
        cur = conn.cursor()
        cur.executemany("""
                  INSERT INTO changes(run_id,node_kind,node_id,change_type,detail,severity)
                  VALUES (:run_id,:node_kind,:node_id,:change_type,:detail,:severity)
                """, changes)
        conn.commit()
        conn.close()
        print(f"[green]✓ Saved[/green] {len(changes)} changes")

@app.command()
def impact(column_id: str,
           change: str = typer.Option(..., "--change", help="ChangeType, e.g. type_change"),
           db: str = typer.Option("lineage.db", "--db"),
           run: str = typer.Option("", "--run")):
    if not run:
        conn = sqlite3.connect(db)
        run = latest_run_id(conn)
        conn.close()
    hits = impact_bfs(db, run, column_id, change)
    hits.sort(key=lambda x: SEV_RANK[x[2]], reverse=True)
    for nid, kind, sev in hits[:50]:
        print(f"{sev:9} {kind:9} {nid}")

@app.command()
def guard(db: str = typer.Option("lineage.db", "--db"),
          base: str= typer.Option(..., "--base", help="Baseline run_id"),
          curr: str = typer.Option("", "--curr", help="Current run_id (default latest)"),
          threshold: str = typer.Option("HIGH", "--threshold", help="LOW|MEDIUM|HIGH|CRITICAL")):
    if not curr:
        conn = sqlite3.connect(db)
        curr = latest_run_id(conn)
        conn.close()

    changes = detect_changes(db, base, curr)
    if not changes:
        print("[yellow]No changes detected[/yellow]")
        raise typer.Exit(0)

    bad = []
    for ch in changes:
        start = ch["node_id"]
        sev_hits = impact_bfs(db, curr, start, ch["change_type"])
        max_sev = max([SEV_RANK.get(s, 1) for _,_,s in sev_hits], default=0)
        if max_sev >= SEV_RANK[threshold]:
            bad.append((ch, max_sev))

    if bad:
        print(f"[red]Guard failed[/red] ({len(bad)} risky changes >= {threshold}")
        for ch, _ in bad[:10]:
            print(f"- {ch['change_type']} @ {ch['node_id']} detail={ch['detail']}")
        raise typer.Exit(1)
    else:
        print("[green]Guard passed[/green]")

def main():
    app()

if __name__ == "__main__":
    main()