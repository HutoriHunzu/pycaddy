from .status import Status
from datetime import datetime
from pydantic import BaseModel
from pathlib import Path


class RunRecord(BaseModel):
    status: Status
    param_hash: str | None = None
    files: dict[str, Path] = {}
    timestamp_status_lst: list[tuple[datetime, Status]] = []

    def timestamp_status(self):
        """Append (now, current status) to the history list."""
        self.timestamp_status_lst.append((datetime.now(), self.status))

