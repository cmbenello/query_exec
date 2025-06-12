#!/usr/bin/env python3
"""
bp_sweep.py  –  benchmark TPCH sort across (BP, threads) pairs,
                stream each result to NDJSON, survive missing timing lines,
                and plot time vs threads (one line per BP).

USAGE
-----
python bp_sweep.py \
        --bp 3000 5000 10000 30000 \
        --threads 1 2 4 8 16 32 40
"""

import argparse
import datetime as dt
import json
import pathlib
import re
import subprocess
import sys
from typing import List, Optional

import matplotlib.pyplot as plt
import pandas as pd


# ───────────────────────── configuration / regexes ─────────────────────── #

GEN_RE   = re.compile(r"generation (?:duration|took)\s+([0-9.]+)s", re.I)
MERGE_RE = re.compile(r"merge (?:duration|took)\s+([0-9.]+)s", re.I)
FAN_RE   = re.compile(r"fan-ins \[([0-9,\s]+)\]", re.I)

# ─────────────────────────── helper functions ──────────────────────────── #

def parse_float(regex: re.Pattern, text: str) -> Optional[float]:
    """Return float from first capture group or None if not found."""
    m = regex.search(text)
    return float(m.group(1)) if m else None


def run_once(bp: int, thr: int, log_dir: pathlib.Path) -> dict:
    """Execute one benchmark; return a dict with timings (or None)."""
    proc = subprocess.run(
        ["bash", "run_tpch.sh", str(bp), str(thr)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    full_log = proc.stdout + proc.stderr

    gen   = parse_float(GEN_RE,   full_log)
    merg  = parse_float(MERGE_RE, full_log)
    fan_m = FAN_RE.search(full_log)
    fan   = [int(x) for x in fan_m.group(1).replace(" ", "").split(",")] if fan_m else []

    if gen is None or merg is None:
        # write failing log for later inspection
        fail_log = log_dir / f"failed_BP-{bp}_THR-{thr}.log"
        fail_log.write_text(full_log)
        print(f"   ⚠️  could not parse timings; full log → {fail_log.name}")

    return {
        "bp": bp,
        "threads": thr,
        "bp_rel": bp / 30_000.0,
        "gen_s": gen,
        "merge_s": merg,
        "total_s": None if gen is None or merg is None else gen + merg,
        "fan_ins": fan,
        "return_code": proc.returncode,
    }

# ───────────────────────────────── main ─────────────────────────────────── #

def main() -> None:
    p = argparse.ArgumentParser("Thread–scaling sweep across BP sizes")
    p.add_argument("--bp",      type=int, nargs="+", required=True)
    p.add_argument("--threads", type=int, nargs="+", required=True)
    p.add_argument("--outdir",  type=pathlib.Path, default=".")
    p.add_argument("--metric",  choices=["total_s", "gen_s", "merge_s"],
                   default="total_s", help="Timing to plot on Y axis")
    args = p.parse_args()

    outdir = args.outdir.expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    stamp   = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    ndjson  = outdir / f"results_{stamp}.ndjson"
    png_fp  = outdir / f"thread_scaling_{stamp}.png"
    rows: List[dict] = []

    with ndjson.open("a") as sink:
        for bp in args.bp:
            for thr in args.threads:
                print(f"▶ BP={bp:<6} threads={thr}")
                rec = run_once(bp, thr, outdir)
                rows.append(rec)
                sink.write(json.dumps(rec) + "\n")
                sink.flush()
                y = rec[args.metric]
                print(f"   {args.metric}="
                      f"{'n/a' if y is None else f'{y:.2f}s'}  (saved)")

    print(f"[✓] NDJSON appended → {ndjson}")

    # plot only points with a numeric Y value
    df = pd.DataFrame([r for r in rows if r[args.metric] is not None])
    if df.empty:
        print("Nothing to plot – every run failed to yield the chosen metric.")
        sys.exit(0)

    plt.figure(figsize=(8, 4.5))
    for bp, g in df.groupby("bp"):
        g_sorted = g.sort_values("threads")
        plt.plot(g_sorted["threads"], g_sorted[args.metric],
                 marker="o", label=f"BP={bp}")
    plt.xlabel("Threads")
    plt.ylabel(f"{args.metric.replace('_s','').capitalize()} time (s)")
    plt.title("Thread scaling across buffer-pool sizes")
    plt.grid(True)
    plt.legend(title="Buffer-pool size")
    plt.tight_layout()
    plt.savefig(png_fp)
    print(f"[✓] Plot saved → {png_fp}")


if __name__ == "__main__":
    main()