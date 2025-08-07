# tests/test_aggregator.py
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter

from pycaddy.aggregator import Aggregator  # adjust import to your package layout
from pycaddy.save import save_json


# ----------------------------------------------------------------------
# 1) happy-path aggregation (plain dicts)
# ----------------------------------------------------------------------
def test_aggregate_basic(project, tmp_path: Path):
    file_tag = "data"

    id_to_payloads = {'id_A': [
        {'a': 1, 'b': {'c': 2}},
        {'a': 2, 'b': {'c': 3}},
    ],
        'id_B': [
            {'d': 1, 'e': {'f': 2}},
            {'d': 3, 'e': {'f': 4}},
        ]
    }

    # expected flattened merge
    expected = [{'a': 1, 'b__c': 2, 'd': 1, 'e__f': 2, 'uid': '000'},
                {'a': 2, 'b__c': 3, 'd': 3, 'e__f': 4, 'uid': '001'}
                ]

    for id_, payloads in id_to_payloads.items():
        for payload in payloads:
            session = project.session(id_, params=payload)
            p = session.path('data.json')
            save_json(p, payload)
            session.attach_files({'data': p})

    grouping_config = {'grp': list(id_to_payloads.keys())}

    out = {k: Aggregator(identifiers=v).aggregate(file_tag=file_tag, ledger=project.ledger)
           for k, v in grouping_config.items()}


    assert list(out) == ["grp"]
    rows = out["grp"]
    assert len(rows) == 2

    for row, expected_row in zip(rows, expected):
        assert row == expected_row


# ----------------------------------------------------------------------
# 2) aggregation with TypeAdapter + custom flatten()
# ----------------------------------------------------------------------
class Point(BaseModel):
    x: int
    y: int
    prefix: str

    def flatten(self) -> dict[str, int]:  # type: ignore[override]
        return {f"{self.prefix}_x": self.x, f"{self.prefix}_y": self.y}


def test_aggregate_with_adapter(project, tmp_path: Path):

    file_tag = "data"

    id_to_payloads = {
        'P1': [{"x": 5, "y": 7, 'prefix': 'A'}],
        'P2': [{"x": 1, "y": 2, 'prefix': 'B'}]
    }

    # expected flattened merge
    expected = [{'A_x': 5, 'A_y': 7, 'B_x': 1, 'B_y': 2, 'uid': '000'}]

    for id_, payloads in id_to_payloads.items():
        for payload in payloads:
            session = project.session(id_, params=payload)
            p = session.path('data.json')
            save_json(p, payload)
            session.attach_files({file_tag: p})

    grouping_config = {"vec": list(id_to_payloads.keys())}


    adapter = TypeAdapter(Point)
    out = {k: Aggregator(identifiers=v).aggregate(file_tag=file_tag, ledger=project.ledger, adapter=adapter)
           for k, v in grouping_config.items()}

    assert out['vec'] == expected


# ----------------------------------------------------------------------
# 3) fail-fast when a file is missing
# ----------------------------------------------------------------------
def test_missing_file_raises(project, tmp_path: Path):

    session = project.session('I1', params={})
    file_tag = "metrics"
    missing = tmp_path / "nowhere.json"
    session.attach_files({file_tag: missing})

    agg = Aggregator(identifiers=["I1"])

    with pytest.raises(FileNotFoundError):
        _ = agg.aggregate(file_tag=file_tag, ledger=project.ledger)


