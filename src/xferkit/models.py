"""Plain data objects describing upload sessions and their results."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import InvalidPart
from .protocol import total_parts


@dataclass(frozen=True)
class UploadSession:
    """An in-progress upload. Immutable once created."""

    upload_id: str
    filename: str
    size: int
    chunk_size: int
    created_at: float
    expires_at: float | None = None
    checksum: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    owner: str | None = None  # principal that created it; None = anonymous

    @property
    def total_parts(self) -> int:
        return total_parts(self.size, self.chunk_size)

    def part_offset(self, index: int) -> int:
        self._check_index(index)
        return index * self.chunk_size

    def part_size(self, index: int) -> int:
        self._check_index(index)
        if index == self.total_parts - 1:
            return self.size - index * self.chunk_size
        return self.chunk_size

    def is_expired(self, now: float | None = None) -> bool:
        if self.expires_at is None:
            return False
        return (time.time() if now is None else now) >= self.expires_at

    def _check_index(self, index: int) -> None:
        if not isinstance(index, int) or isinstance(index, bool):
            raise InvalidPart(f"part index must be an integer, got {index!r}")
        if not 0 <= index < self.total_parts:
            raise InvalidPart(
                f"part index {index} is out of range (0..{self.total_parts - 1})",
                details={"total_parts": self.total_parts},
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "upload_id": self.upload_id,
            "filename": self.filename,
            "size": self.size,
            "chunk_size": self.chunk_size,
            "total_parts": self.total_parts,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "checksum": self.checksum,
            "metadata": dict(self.metadata),
            "owner": self.owner,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UploadSession:
        return cls(
            upload_id=str(data["upload_id"]),
            filename=str(data["filename"]),
            size=int(data["size"]),
            chunk_size=int(data["chunk_size"]),
            created_at=float(data["created_at"]),
            expires_at=None if data.get("expires_at") is None else float(data["expires_at"]),
            checksum=data.get("checksum"),
            metadata={str(k): str(v) for k, v in (data.get("metadata") or {}).items()},
            owner=None if data.get("owner") is None else str(data["owner"]),
        )


@dataclass(frozen=True)
class PartRecord:
    """A part that has been received and verified."""

    index: int
    size: int
    checksum: str

    def to_dict(self) -> dict[str, Any]:
        return {"index": self.index, "size": self.size, "checksum": self.checksum}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PartRecord:
        return cls(index=int(data["index"]), size=int(data["size"]), checksum=str(data["checksum"]))


@dataclass(frozen=True)
class UploadStatus:
    """Snapshot of an upload session and the parts received so far."""

    session: UploadSession
    received_parts: tuple[int, ...]
    state: str = "pending"  # "pending" or "completing"

    @property
    def missing_parts(self) -> list[int]:
        received = set(self.received_parts)
        return [i for i in range(self.session.total_parts) if i not in received]

    @property
    def received_bytes(self) -> int:
        return sum(self.session.part_size(i) for i in self.received_parts)

    def to_dict(self) -> dict[str, Any]:
        data = self.session.to_dict()
        data.update(
            state=self.state,
            received_parts=list(self.received_parts),
            missing_parts=self.missing_parts,
            received_bytes=self.received_bytes,
        )
        return data


@dataclass(frozen=True)
class CompletedUpload:
    """A finished upload. ``path`` is where the assembled file lives on the server."""

    upload_id: str
    filename: str
    size: int
    path: Path
    composite_checksum: str
    checksum: str | None
    created_at: float
    completed_at: float
    metadata: dict[str, str] = field(default_factory=dict)
    chunk_size: int = 0  # 0 for records written by xferkit < 0.1.0 final
    owner: str | None = None

    def to_dict(self, *, include_path: bool = False) -> dict[str, Any]:
        data: dict[str, Any] = {
            "upload_id": self.upload_id,
            "filename": self.filename,
            "size": self.size,
            "chunk_size": self.chunk_size,
            "total_parts": total_parts(self.size, self.chunk_size) if self.chunk_size else None,
            "composite_checksum": self.composite_checksum,
            "checksum": self.checksum,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "metadata": dict(self.metadata),
            "owner": self.owner,
        }
        if include_path:
            data["path"] = str(self.path)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, path: Path) -> CompletedUpload:
        return cls(
            upload_id=str(data["upload_id"]),
            filename=str(data["filename"]),
            size=int(data["size"]),
            path=path,
            composite_checksum=str(data["composite_checksum"]),
            checksum=data.get("checksum"),
            created_at=float(data["created_at"]),
            completed_at=float(data["completed_at"]),
            metadata={str(k): str(v) for k, v in (data.get("metadata") or {}).items()},
            chunk_size=int(data.get("chunk_size") or 0),
            owner=None if data.get("owner") is None else str(data["owner"]),
        )
