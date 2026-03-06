from __future__ import annotations

import os
from functools import lru_cache


@lru_cache(maxsize=64)
def load_prompt(name: str) -> str:
    """
    Load a prompt markdown file from the repo-level prompts/ directory.
    """
    root = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(root)
    path = os.path.join(repo_root, "prompts", name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

