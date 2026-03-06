from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from .schema_index import Field, SchemaIndex


@dataclass(frozen=True)
class CatalogField:
    path: str
    scalar_type: str
    label: str
    sample: str


IMPORTANT_KEYS = {
    "id",
    "event_id",
    "user_id",
    "email",
    "name",
    "timestamp",
    "event_type",
    "type",
    "category",
    "price",
    "amount",
    "total",
    "total_amount",
    "currency",
    "state",
    "city",
    "country",
}


def _label_from_path(path: str) -> str:
    last = path.split(":")[-1]
    last = last.replace("[*]", "")
    return last


def _importance_score(path: str) -> int:
    label = _label_from_path(path).lower()
    score = 0
    if label in IMPORTANT_KEYS:
        score += 10
    if "id" in label:
        score += 3
    if "timestamp" in label or "date" in label:
        score += 2
    if "amount" in label or "price" in label or "total" in label:
        score += 2
    # prefer shorter paths (more general) slightly
    score -= min(5, path.count(":"))
    return score


def build_field_catalog(index: SchemaIndex, limit: int = 60) -> List[CatalogField]:
    fields: Sequence[Field] = list(index.fields.values())
    fields = sorted(fields, key=lambda f: (-_importance_score(f.path), f.path))

    catalog: List[CatalogField] = []
    for f in fields[:limit]:
        sample = f.sample_values[0] if f.sample_values else ""
        catalog.append(
            CatalogField(
                path=f.path,
                scalar_type=f.scalar_type,
                label=_label_from_path(f.path),
                sample=sample[:80],
            )
        )
    return catalog

