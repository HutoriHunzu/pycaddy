from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

from ..dict_utils import flatten_with_pretty_keys, apply_adapter
from ..load import load_json
from ..ledger import Ledger, UID_RECORD_DICT


class Aggregator(BaseModel):
    """
    Join JSON artefacts for matching *uids* across several identifiers.

    The class reads a single :class:`~ledger.Ledger` file instead of
    traversing directory trees.  For each *aggregation group* it:

    1.  Computes the intersection of UIDs that appear under **all**
        identifiers of that group.
    2.  Loads the same artefact (``file_tag``) from every identifier.
    3.  Optionally validates each payload through a
        :class:`pydantic.TypeAdapter` → converts it to a typed model.
    4.  Flattens the (typed or raw) dict and merges the key–value pairs.
    5.  Returns one consolidated row per UID, plus a ``{by: uid}`` tag.

    The implementation is **fail-fast**: any missing file or validation
    error raises immediately – no silent row skipping.
    """

    name_to_identifier_dict: dict[str, list[str]] = Field(
        ..., description="Aggregation recipe: group name → list of identifiers."
    )

    # ledger_file_path: PathLike = Field(
    #     ..., description="Path to the `ledger.json` file."
    # )

    # ------------------------------------------------------------------ #
    # public API                                                          #
    # ------------------------------------------------------------------ #
    def aggregate(
            self,
            ledger: Ledger,
            file_tag: str,
            *,
            relpath: Path = Path(""),
            by: Literal["uid"] = "uid",
            adapter: TypeAdapter | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Merge *file_tag* artefacts for every aggregation group.

        Parameters
        ----------
        ledger
            A Ledger instance, used to load records in which the files
            to aggregation are found
        file_tag
            Key in :pyattr:`RunRecord.files` that points to the JSON
            artefact to merge.
        by
            Tag column for the resulting rows.  Only ``'uid'`` is
            supported today.
        adapter
            If supplied, each payload is first converted via
            ``apply_adapter(raw_payload, adapter)`` before flattening.

        Returns
        -------
        dict[str, list[dict[str, Any]]]
            ``{group_name: [row, …]}``

        Raises
        ------
        ValueError
            If *by* ≠ ``'uid'``.
        FileNotFoundError, RuntimeError, ValidationError, …
            Anything that goes wrong while reading or validating payloads.
        """
        if by != "uid":
            raise ValueError(f"Unsupported 'by' mode: {by!r}")

        unique_identifiers = reduce(lambda x, y: x | set(y), self.name_to_identifier_dict.values(), set())
        data: dict[str, UID_RECORD_DICT] = {id_: ledger.get_uid_record_dict(id_, relpath=relpath)
                                            for id_ in unique_identifiers}  # id → {uid: RunRecord}

        aggregated: dict[str, list[dict[str, Any]]] = {
            group: [] for group in self.name_to_identifier_dict
        }

        for group_name, identifiers in self.name_to_identifier_dict.items():
            common_uids = sorted(self._uids_common_to_all(identifiers, data))

            uid_path_pairs = (
                (uid, [data[id_][uid].files[file_tag] for id_ in identifiers])
                for uid in common_uids
            )

            aggregated[group_name] = [
                self._load_and_merge_row(paths, adapter) | {by: uid}
                for uid, paths in uid_path_pairs
            ]

        return aggregated

    # ------------------------------------------------------------------ #
    # internals                                                           #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _uids_common_to_all(
            identifiers: list[str],
            data: dict[str, UID_RECORD_DICT],
    ) -> set[str]:
        """Return UIDs present under **all** given identifiers."""
        uid_sets = (set(data.get(i, {})) for i in identifiers)
        return set.intersection(*uid_sets) if identifiers else set()

    # ------------------------------------------------------------------ #
    # one-row helper                                                      #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _load_and_merge_row(
            paths: list[Path],
            adapter: TypeAdapter | None = None,
    ) -> dict[str, Any]:
        """
        Merge a single UID’s artefacts.

        Workflow
        --------
        * read file → dict
        * ``apply_adapter`` (optional, creates typed model)
        * flatten (`dict_utils.flatten` if dict, else model.flatten())

        Raises
        ------
        FileNotFoundError
            If a path is missing.
        RuntimeError
            Wraps any nested exception with file-context.
        """
        merged: dict[str, Any] = {}

        for path in paths:
            if not path.exists():
                raise FileNotFoundError(path)

            try:
                raw_payload = load_json(path)  # -> dict
                payload = apply_adapter(raw_payload, adapter=adapter)

                flat = (
                    flatten_with_pretty_keys(payload)  # dict branch
                    if isinstance(payload, dict)
                    else payload.flatten()  # typed model branch
                )
                merged.update(flat)

            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"while processing {path}") from exc

        return merged
