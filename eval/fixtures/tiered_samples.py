"""
Build prefix-tier JSON samples for the degradation study.

Mirrors realistic "user pasted the first N rows they saw" behavior — first N
array elements only (NOT even sub-sampling).

  python -m eval.fixtures.tiered_samples
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple, Union

FIXTURES_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(FIXTURES_DIR))

# (source under data/, output basename prefix under eval/fixtures/)
SOURCES: List[Tuple[str, str]] = [
    ("data/sample_data_github.json", "sample_data_github"),
    ("data/sample_data_ga4.json", "sample_data_ga4"),
]

# Numeric tiers only — "full" points at the original data/ file (no copy).
PREFIX_TIERS = [2, 5, 20]


def _primary_array_key(obj: Dict[str, Any]) -> str:
    array_keys = [k for k, v in obj.items() if isinstance(v, list)]
    if not array_keys:
        raise ValueError("expected a root object with at least one array-valued key")
    if len(array_keys) == 1:
        return array_keys[0]
    return max(array_keys, key=lambda k: len(obj[k]))


def take_prefix_tier(sample: Any, n: int) -> Any:
    """Keep the first ``n`` elements of the primary root array; copy other keys."""
    if isinstance(sample, list):
        return sample[:n]
    if not isinstance(sample, dict):
        raise TypeError(f"unsupported sample root type: {type(sample).__name__}")
    key = _primary_array_key(sample)
    out = dict(sample)
    out[key] = list(sample[key][:n])
    return out


def tier_path(basename_prefix: str, tier: Union[int, str]) -> str:
    if tier == "full":
        # Resolve back to the original data/ file for this prefix.
        for src, prefix in SOURCES:
            if prefix == basename_prefix:
                return os.path.join(REPO_ROOT, src)
        raise KeyError(basename_prefix)
    return os.path.join(FIXTURES_DIR, f"{basename_prefix}_tier{tier}.json")


def generate_all(*, write: bool = True) -> Dict[str, str]:
    """
    Write tier2/5/20 fixture files. Returns mapping of logical tier id -> path.
    """
    written: Dict[str, str] = {}
    os.makedirs(FIXTURES_DIR, exist_ok=True)

    for rel_src, prefix in SOURCES:
        src_path = os.path.join(REPO_ROOT, rel_src)
        with open(src_path, "r", encoding="utf-8") as f:
            sample = json.load(f)

        written[f"{prefix}:full"] = src_path

        for n in PREFIX_TIERS:
            tiered = take_prefix_tier(sample, n)
            out_path = tier_path(prefix, n)
            if write:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(tiered, f, indent=2, ensure_ascii=False)
                    f.write("\n")
            written[f"{prefix}:tier{n}"] = out_path

            # Sanity: prefix length
            if isinstance(tiered, dict):
                key = _primary_array_key(tiered)
                assert len(tiered[key]) == min(n, len(sample[key]))

    return written


def main() -> None:
    paths = generate_all(write=True)
    print("Wrote tier fixtures:")
    for k, p in sorted(paths.items()):
        print(f"  {k}: {p}")


if __name__ == "__main__":
    main()
