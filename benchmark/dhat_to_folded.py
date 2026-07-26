#!/usr/bin/env python3
"""Convert a dhat-heap.json into folded stacks weighted by peak (t-gmax) bytes.

Usage: dhat_to_folded.py dhat-heap.json > peak.folded
"""
import json
import re
import sys

ADDR = re.compile(r"^0x[0-9a-fandABCDEF]+:\s*")
TAIL = re.compile(r"\s*\(.*?\)\s*$")


def clean(frame: str) -> str:
    frame = ADDR.sub("", frame)
    frame = TAIL.sub("", frame)
    # Drop generic/turbofish noise to keep the graph readable.
    frame = frame.replace("<", "&lt;").replace(">", "&gt;")
    return frame.strip() or "<unknown>"


def main() -> None:
    data = json.load(open(sys.argv[1]))
    ftbl = data["ftbl"]
    pps = data["pps"]
    # Which metric: default gb (bytes at global peak). Override with argv[2].
    key = sys.argv[2] if len(sys.argv) > 2 else "gb"

    folded: dict[str, int] = {}
    for pp in pps:
        weight = pp.get(key, 0)
        if weight <= 0:
            continue
        fs = pp["fs"]
        # fs[0] is the allocation leaf (dhat alloc); outermost is last.
        frames = [ftbl[i] for i in fs]
        # Drop the dhat allocator shim frames at the leaf side.
        frames = [f for f in frames if "dhat::Alloc" not in f and "GlobalAlloc" not in f]
        # Reverse so outermost (root) comes first for the flamegraph base.
        frames = list(reversed(frames))
        stack = ";".join(clean(f) for f in frames)
        folded[stack] = folded.get(stack, 0) + weight

    for stack, weight in sorted(folded.items(), key=lambda kv: -kv[1]):
        print(f"{stack} {weight}")


if __name__ == "__main__":
    main()
