from dataclasses import dataclass
from ..ledger import Ledger, Status
from pathlib import Path
from .structs import StorageMode



@dataclass
class Session:
    """
    Represents an individual experiment run with unique ID and path.
    
    Provides methods to manage run status and attach files to runs.
    Sessions are created by Project.session() and manage their own
    folder structure and file organization.
    """
    identifier: str
    uid: str
    relpath: Path
    absolute_path: Path
    ledger: Ledger
    param_hash: str | None
    storage_mode: StorageMode = StorageMode.SUBFOLDER

    @property
    def status(self) -> Status:
        record = self.ledger.get_record(self.identifier, self.uid, relpath=self.relpath)
        return record.status

    def start(self):
        self.ledger.log(self.identifier, self.uid, relpath=self.relpath, status=Status.RUNNING)

    def error(self):
        self.ledger.log(self.identifier, self.uid, relpath=self.relpath, status=Status.ERROR)

    def done(self):
        self.ledger.log(self.identifier, self.uid, relpath=self.relpath, status=Status.DONE)

    def attach_files(self, path_dict: dict[str, Path]):
        self.ledger.log(self.identifier, self.uid, relpath=self.relpath, path_dict=path_dict)

    @property
    def files(self) -> dict[str, Path]:
        files_dict = self.ledger.get_record(self.identifier, self.uid, relpath=self.relpath).files
        return {k: Path(v) for k, v in files_dict.items()}

    def is_done(self) -> bool:
        return self.status == Status.DONE

    @property
    def folder(self) -> Path:
        path = self.absolute_path
        if self.storage_mode == StorageMode.SUBFOLDER:
            path = path / self.uid

        path.mkdir(parents=True, exist_ok=True)
        return path

    def path(self,
             name: str = None,
             suffix: str = '',
             include_identifier: bool = True,
             include_uid: bool = True) -> Path:

        file_name_list = []
        if self.storage_mode == StorageMode.PREFIX and include_uid:
            file_name_list.append(self.uid)

        if include_identifier:
            file_name_list.append(self.identifier)

        if name:
            file_name_list.append(name)

        path = self.folder / '_'.join(file_name_list)

        if path.suffix == '':
            path = path.with_suffix(suffix)

        return path


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
