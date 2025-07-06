from dataclasses import dataclass
from pydantic import BaseModel, Field, PrivateAttr
from .utils import PathLike
from ..ledger import Ledger, RunRecord, Status
from pathlib import Path
from .structs import StorageMode



@dataclass
class Session:
    identifier: str
    uid: str
    project_path: Path
    ledger: Ledger
    param_hash: str | None
    storage_mode: StorageMode = StorageMode.SUBFOLDER

    @property
    def status(self) -> Status:
        record = self.ledger.get_record(self.identifier, self.uid)
        return record.status

    def start(self):
        self.ledger.log(self.identifier, self.uid, status=Status.RUNNING)

    def error(self):
        self.ledger.log(self.identifier, self.uid, status=Status.ERROR)

    def done(self):
        self.ledger.log(self.identifier, self.uid, status=Status.DONE)

    def attach_files(self, path_dict: dict[str, str | Path]):
        self.ledger.log(self.identifier, self.uid, path_dict=path_dict)

    @property
    def files(self) -> dict[str, Path]:
        files_dict = self.ledger.get_record(self.identifier, self.uid).files
        return {k: Path(v) for k, v in files_dict.items()}

    def is_done(self) -> bool:
        return self.status == Status.DONE

    @property
    def folder(self) -> Path:
        path = self.project_path
        if self.storage_mode == StorageMode.SUBFOLDER:
            path = self.project_path / self.uid

        path.mkdir(parents=True, exist_ok=True)
        return path

    def path(self, name: str = None,
             include_identifier: bool = True):
        file_name_list = []
        if self.storage_mode == StorageMode.PREFIX:
            file_name_list.append(self.uid)

        if include_identifier:
            file_name_list.append(self.identifier)

        if name:
            file_name_list.append(name)

        return self.folder / '_'.join(file_name_list)


    # def __enter__(self):
    #     return self
    #
    # def __exit__(self, exc_type, *_):
    #     status = Status.ERROR if exc_type else Status.DONE
    #     self.project.ledger.log(
    #         self.identifier, self.base,
    #         status=status,
    #         add_time_end=True,
    #     )
