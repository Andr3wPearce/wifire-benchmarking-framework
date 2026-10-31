"""Display helpers for notebooks/red_sky.ipynb.

They keep the notebook's cells about the workflow rather than about formatting.
Nothing here changes what runs; everything takes the compiled plan (bench.wstc) or
what the WSTC client returns.
"""
from __future__ import annotations

import base64
import html
import json
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
    """The compiled plan: the recipe's steps, then what each fire gets."""
    display(Markdown("**Steps** (one asset each, partitioned by fire; one pod per step)"))
    display(md_table([{"slot": a["slot"], "step": a["name"], "block": a["block"], "kind": a["kind"],
                       "image": f"bench-{a['image']}", "why inserted": a.get("reason") or ""}
                      for a in cp["assets"]]))
    m = cp["model"]
    display(Markdown(f"**Model:** `{m['block']}` with `{json.dumps(m['params'])}` (image `bench-{m['image']}`)  \n"
                     f"**Reference:** `{cp['reference']['block']}`  \n**Metrics:** "
                     + ", ".join(f"`{x}`" for x in cp["metrics"])))
    display(Markdown("**Fires**"))
    display(md_table([{"fire": name, "domain (W, S, E, N)": f["run"]["run_json"]["case"]["domain_wgs84"],
                       "t0": f["run"]["run_json"]["case"]["t0"],
                       "reference": "local file" if name in cp["references"] else
                                    (f["reference"]["payload"]["case"]["reference"].get("url") or "none")}
                      for name, f in sorted(cp["fires"].items())]))
    for fire, ws in cp["warnings"].items():
        for w in ws:
            display(Markdown(f"> warning ({fire}): {w}"))


def mermaid(cp: dict) -> Markdown:
    """The asset graph as a Mermaid flowchart (JupyterLab 4.1+ draws it)."""
    ids, lines = {}, ["flowchart LR"]

    def node(key, label):
        ids[key] = f"n{len(ids)}"
        lines.append(f'  {ids[key]}["{label}"]:::step')
        return ids[key]

    for a in cp["assets"]:
        node(tuple(a["key"]), f"{a['slot']}<br/>{a['block']}")
        if a["after"]:
            lines.append(f"  {ids[tuple(a['after'])]} --> {ids[tuple(a['key'])]}")
    tails = [tuple(a["key"]) for a in cp["assets"] if not any(a["key"] == b["after"] for b in cp["assets"])]
    pred = node("prediction", f"prediction<br/>{cp['model']['block']}")
    for t in tails:
        lines.append(f"  {ids[t]} --> {pred}")
    ref = node("reference", "reference<br/>observed perimeter")
    rep = node("report", "report<br/>one row per fire")
    for m in cp["metrics"]:
        s = node(f"score/{m}", f"score<br/>{m}")
        lines += [f"  {pred} --> {s}", f"  {ref} --> {s}", f"  {s} --> {rep}"]
    lines.append("  classDef step fill:#fde3c8,stroke:#c2410c,color:#111")
    src = "\n".join(lines)
    link = "https://mermaid.live/view#base64:" + base64.urlsafe_b64encode(
        json.dumps({"code": src, "mermaid": {"theme": "default"}}).encode()).decode()
    return Markdown(f"```mermaid\n{src}\n```\n\n_No diagram above? Your Jupyter is older than "
                    f"JupyterLab 4.1; [open it in the Mermaid viewer]({link})._")


def identity_table(cp: dict, fire: str) -> Markdown:
    f = cp["fires"][fire]
    rows = [{"step": s["name"], "identity": s["identity"], "artifact (shared volume)": s["path"]}
            for s in f["steps"].values()]
    rows += [{"step": "reference", "identity": f["reference"]["identity"], "artifact (shared volume)": f["reference"]["path"]},
             {"step": "prediction", "identity": f["run"]["identity"], "artifact (shared volume)": f["run"]["path"]}]
    return md_table(rows)


def shared_with(cp_a: dict, cp_b: dict, fire: str) -> Markdown:
    """Which artifacts two workflows share for a fire (same identity = built once)."""
    a, b = cp_a["fires"][fire]["steps"], cp_b["fires"][fire]["steps"]
    rows = [{"step": n, "identity": a[n]["identity"],
             "shared": "yes, built once" if n in b and b[n]["identity"] == a[n]["identity"] else "no"}
            for n in a]
    return md_table(rows)


def run_fires(d, job: str, fires: list[str], tags: dict | None = None, max_parallel: int = 3,
              poll: int = 10) -> dict:
    """Launch one run per fire, at most `max_parallel` at a time (WSTC allows 4 runs across
    all users). Returns {fire: {"run_id", "status", "seconds"}}."""
    todo, active, out = list(fires), {}, {}
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
    return [{"asset": asset.split("/", 2)[-1], **{f: md.get(f) for f in fields if f in md}}
            for asset, md in results.items()]


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
