from typing import Optional, List
from sqlmodel import SQLModel, Field, Session, select, create_engine
from datetime import datetime, timezone
from pydantic import field_validator



class FileRecord(SQLModel, table=True):
    __tablename__ = "file_metadata"

    id: Optional[int] = Field(default=None, primary_key=True)

    # required
    name: str = Field(nullable=False)
    link: str = Field(nullable=False)
    dataset_pid: str = Field(nullable=False, index=True)

    # optional
    size: Optional[int] = Field(default=None)
    mime_type: Optional[str] = Field(default=None)
    ext: Optional[str] = Field(default=None)
    checksum_value: Optional[str] = Field(default=None)
    checksum_type: Optional[str] = Field(default=None)
    access_request: Optional[bool] = Field(default=None)

    publication_date: Optional[datetime] = Field(
        default=None,
        index=True,
    )
    embargo: Optional[datetime] = Field(
        default=None,
        index=True,
    )

    file_pid: Optional[str] = Field(
        default=None,
        index=True,
    )

    @field_validator("publication_date", "embargo", mode="before")
    @classmethod
    def normalise_datetime(cls, v):
        if v is None:
            return v
        if isinstance(v, datetime) and v.tzinfo:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v

    @field_validator("size", mode="before")
    @classmethod
    def safe_int(cls, s):
        if s is None:
            return None
        try:
            return int(s.strip())
        except (ValueError, TypeError):
            return None

def create_pg_engine(dsn):
    return create_engine(dsn, pool_pre_ping=True, echo=False)

class FileRecordStore:
    def __init__(self, engine):
        self.engine = engine

    # ---------- SCHEMA INITIALIZATION ----------

    def init_schema(self):
        SQLModel.metadata.create_all(self.engine)
        return True


    # ---------- CREATE ----------

    def create(self, record: FileRecord) -> FileRecord:
        with Session(self.engine) as session:
            session.add(record)
            session.commit()
            session.refresh(record)
            return record

    # ---------- READ ----------

    def get(self, file_id: int) -> Optional[FileRecord]:
        with Session(self.engine) as session:
            return session.get(FileRecord, file_id)

    def get_by_pid(self, file_pid: str) -> Optional[FileRecord]:
        with Session(self.engine) as session:
            stmt = select(FileRecord).where(
                FileRecord.file_pid == file_pid
            )
            return session.exec(stmt).first()

    def list(self, limit: int = 100, offset: int = 0) -> List[FileRecord]:
        with Session(self.engine) as session:
            stmt = (
                select(FileRecord)
                .limit(limit)
                .offset(offset)
            )
            return session.exec(stmt).all()

    # ---------- UPDATE ----------

    def update(self, file_id: int, data: dict) -> Optional[FileRecord]:
        with Session(self.engine) as session:
            record = session.get(FileRecord, file_id)
            if not record:
                return None

            for key, value in data.items():
                setattr(record, key, value)

            session.add(record)
            session.commit()
            session.refresh(record)
            return record

    def upsert(self, record: FileRecord) -> FileRecord:
        with Session(self.engine) as session:
            stmt = select(FileRecord).where(
                FileRecord.file_pid == record.file_pid
            )
            existing = session.exec(stmt).first()

            if existing:
                for field, value in record.model_dump(
                    exclude_unset=True
                ).items():
                    setattr(existing, field, value)

                session.add(existing)
                session.commit()
                session.refresh(existing)
                return existing

            session.add(record)
            session.commit()
            session.refresh(record)
            return record

    # ---------- DELETE ----------

    def delete(self, file_id: int) -> bool:
        with Session(self.engine) as session:
            record = session.get(FileRecord, file_id)
            if not record:
                return False

            session.delete(record)
            session.commit()
            return True

    # ---------- DOMAIN QUERIES ----------

    def list_since(
        self,
        since: datetime,
        limit: int = 100,
    ) -> List[FileRecord]:
        with Session(self.engine) as session:
            stmt = (
                select(FileRecord)
                .where(FileRecord.publication_date >= since)
                .order_by(FileRecord.publication_date)
                .limit(limit)
            )
            return session.exec(stmt).all()

