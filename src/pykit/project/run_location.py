from pydantic.dataclasses import dataclass
from .utils import PathLike
from ..ledger import Ledger
from pathlib import Path



@dataclass
class Run:
    ledger: Ledger
    identifier: str
    folder: Path               # ← write everything here
    # base: str





        pass

    def start(self):
        # generates a unique identifier for this run and set
        pass



    # convenience
    def save(self, rel_name: str, data: bytes):
        path = self.folder / rel_name
        path.write_bytes(data)
        self.project.ledger.attach_file(
            self.identifier, self.base,
            {rel_name: rel_name}        # store run-relative name
        )


    def





    # context-manager sugar
    def __enter__(self): return self
    def __exit__(self, exc_type, *_):
        status = Status.ERROR if exc_type else Status.DONE
        self.project.ledger.log(
            self.identifier, self.base,
            status=status,
            add_time_end=True,
        )


class Run(BaseModel):
    run_id: str
    folder: PathLike
