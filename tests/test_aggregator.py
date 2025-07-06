# tests/test_aggregator.py
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter

from pykit.aggregator import Aggregator  # adjust import to your package layout
from pykit.ledger import Ledger
from pykit.dict_utils import flatten
from pykit.save import save_json


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

    agg = Aggregator(
        name_to_identifier_lst=grouping_config,
        ledger_file_path=project.ledger_path,
    )

    out = agg.aggregate(file_tag=file_tag)

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

    agg = Aggregator(
        name_to_identifier_lst={"vec": list(id_to_payloads.keys())},
        ledger_file_path=project.ledger_path,
    )

    adapter = TypeAdapter(Point)
    out = agg.aggregate(file_tag=file_tag, adapter=adapter)

    assert out['vec'] == expected


# ----------------------------------------------------------------------
# 3) fail-fast when a file is missing
# ----------------------------------------------------------------------
def test_missing_file_raises(project, tmp_path: Path):

    session = project.session('I1', params={})
    file_tag = "metrics"
    missing = tmp_path / "nowhere.json"
    session.attach_files({file_tag: missing})

    agg = Aggregator(
        name_to_identifier_lst={"g": ["I1"]},
        ledger_file_path=project.ledger_path,
    )

    with pytest.raises(FileNotFoundError):
        _ = agg.aggregate(file_tag=file_tag)


# ----------------------------------------------------------------------
# 4) guard against unsupported 'by'
# # ----------------------------------------------------------------------
# def test_unsupported_by_value(tmp_ledger: Ledger):
#     agg = Aggregator(
#         name_to_identifier_lst={"g": []},
#         ledger_file_path=tmp_ledger.file,
#     )
#     with pytest.raises(ValueError):
#         _ = agg.aggregate(file_tag="dummy", by="iteration")  # type: ignore[arg-type]
