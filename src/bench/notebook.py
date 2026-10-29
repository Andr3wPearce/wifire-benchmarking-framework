"""Display helpers for notebooks/red_sky.ipynb.

They keep the notebook's cells about the workflow rather than about formatting.
Nothing here changes what runs; everything takes the compiled plan (bench.wstc) or
what the WSTC client returns.
"""
from __future__ import annotations

import base64
import html
import time
from pathlib import Path

from IPython.display import HTML, Markdown, display

from .wstc import decode_preview


def md_table(rows: list[dict], cols: list[str] | None = None) -> Markdown:
    if not rows:
        return Markdown("_(none)_")
    cols = cols or list(dict.fromkeys(c for r in rows for c in r))
    cell = lambda v: "" if v is None else str(v).replace("|", "\\|").replace("\n", " ")
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(cell(r.get(c)) for c in cols) + " |" for r in rows]
    return Markdown("\n".join(lines))


def show_file(path: Path, lang: str = "yaml") -> None:
    display(Markdown(f"```{lang}\n{Path(path).read_text().strip()}\n```"))


def plan_tables(cp: dict) -> None:
    """The compiled plan as three tables: input steps, runs, and what was skipped."""
    display(Markdown("**Input steps** (each is one asset, one pod)"))
    display(md_table([{"slot": e["slot"], "step": e["name"], "block": e["block"], "kind": e["kind"],
                       "image": f"bench-{e['image']}", "why inserted": e.get("reason") or ""}
                      for e in cp["inputs"]]))
    if cp["reference"]:
        display(Markdown(f"**Reference:** `{cp['reference']['block']}` (the observed perimeter)"))
    display(Markdown(f"**Runs** ({len(cp['runs'])}; each is one partition of `prediction`)"))
    display(md_table([{"run": k, "model": r["model"], "params": r["params"],
                       "inputs": r["inputs"] or "-", "image": f"bench-{r['image']}"}
                      for k, r in sorted(cp["runs"].items())]))
    if cp["skipped"]:
        display(Markdown("**Skipped** (the resolver explains why)"))
        display(md_table([{"model": s["model"], "inputs": s["inputs"], "reason": s["reason"]}
                          for s in cp["skipped"]]))
    for w in cp["warnings"]:
        display(Markdown(f"> warning: {w}"))


def mermaid(cp: dict) -> Markdown:
    """The asset graph as a Mermaid flowchart (JupyterLab 4.1+ draws it)."""
    ids, lines = {}, ["flowchart LR"]

    def node(key, label, cls):
        nid = f"n{len(ids)}"
        ids[key] = nid
        lines.append(f'  {nid}["{label}"]:::{cls}')
        return nid

    for e in cp["inputs"]:
        node(tuple(e["key"]), f"{e['slot']}<br/>{e['block']}", "step")
        for d in e["deps"]:
            lines.append(f"  {ids[tuple(d)]} --> {ids[tuple(e['key'])]}")
    tails = [tuple(e["key"]) for e in cp["inputs"]
             if not any(tuple(e["key"]) == tuple(d) for o in cp["inputs"] for d in o["deps"])]
    pred = node("prediction", f"prediction<br/>{len(cp['runs'])} runs", "step")
    for t in tails:
        lines.append(f"  {ids[t]} --> {pred}")
    ref = node("reference", "reference<br/>final perimeter", "step") if cp["reference"] else None
    rep = node("report", "report", "step")
    for m in cp["metrics"]:
        s = node(f"score/{m}", f"score<br/>{m}", "step")
        lines.append(f"  {pred} --> {s}")
        if ref:
            lines.append(f"  {ref} --> {s}")
        lines.append(f"  {s} --> {rep}")
    lines += ["  classDef step fill:#fde3c8,stroke:#c2410c,color:#111"]
    src = "\n".join(lines)
    link = "https://mermaid.live/view#base64:" + base64.urlsafe_b64encode(
        ('{"code":' + __import__("json").dumps(src) + ',"mermaid":{"theme":"default"}}').encode()).decode()
    return Markdown(f"```mermaid\n{src}\n```\n\n_No diagram above? Your Jupyter is older than "
                    f"JupyterLab 4.1; [open it in the Mermaid viewer]({link})._")


def identity_table(cp: dict) -> Markdown:
    rows = [{"step": e["name"], "identity": e["identity"], "artifact (shared volume)": e["path"]}
            for e in cp["inputs"]]
    if cp["reference"]:
        rows.append({"step": "reference", "identity": cp["reference"]["identity"],
                     "artifact (shared volume)": cp["reference"]["path"]})
    return md_table(rows)


def run_partitions(d, job: str, keys: list[str], tags: dict | None = None, max_parallel: int = 3,
                   poll: int = 10) -> dict:
    """Launch one run per partition, at most `max_parallel` at a time (WSTC allows 4 runs
    across all users). Returns {partition: {"run_id", "status", "seconds"}}."""
    todo, active, out = list(keys), {}, {}
    while todo or active:
        while todo and len(active) < max_parallel:
            k = todo.pop(0)
            active[k] = (d.launch(job, tags={**(tags or {}), "dagster/partition": k}), time.time())
            print(f"launched {k}")
        time.sleep(poll)
        for k, (rid, t) in list(active.items()):
            st = d.status(rid)
            if st in ("SUCCESS", "FAILURE", "CANCELED"):
                out[k] = {"run_id": rid, "status": st, "seconds": round(time.time() - t)}
                print(f"{st:8s} {k}  ({out[k]['seconds']} s)")
                del active[k]
    return out


def asset_rows(results: dict, fields: list[str]) -> list[dict]:
    """d.results(run_id) -> one row per asset with the chosen metadata fields."""
    rows = []
    for asset, md in results.items():
        rows.append({"asset": asset.split("/", 2)[-1], **{f: md.get(f) for f in fields if f in md}})
    return rows


def show_previews(previews: dict[str, str], width: int = 520) -> None:
    """{label: preview markdown} -> images side by side."""
    cells = []
    for label, md in previews.items():
        png = decode_preview(md)
        if png:
            src = "data:image/png;base64," + base64.b64encode(png).decode()
            cells.append(f'<figure style="margin:0 12px 12px 0"><img src="{src}" width="{width}"/>'
                         f'<figcaption style="font-size:12px">{html.escape(label)}</figcaption></figure>')
    display(HTML('<div style="display:flex;flex-wrap:wrap">' + "".join(cells) + "</div>"
                 if cells else "<i>no previews yet</i>"))
