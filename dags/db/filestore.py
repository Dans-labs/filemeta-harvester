from typing import Optional, List
from sqlmodel import SQLModel, BigInteger, Field, Session, Column, select, create_engine, DateTime, func
from datetime import datetime, timezone
from pydantic import field_validator
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import JSONB

class FileRawRecord(SQLModel, table=True):
    __tablename__ = "file_raw_metadata"
    __table_args__ = (
            UniqueConstraint('dataset_pid',
                            name='uq_file_raw_record'),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    dataset_pid: str = Field(nullable=False, index=True)
    raw_metadata: str = Field(
        sa_column=Column(JSONB, nullable=False)
    )
    last_updated: datetime = Field(
        sa_column=Column(
            DateTime(timezone=False),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        )
    )

class FileRecord(SQLModel, table=True):
    __tablename__ = "file_metadata"
    __table_args__ = (
            UniqueConstraint('dataset_pid',
                             'name',
                             'link',
                             name='uq_file_record'),
    )

    id: Optional[int] = Field(default=None, primary_key=True)

    # required
    name: str = Field(nullable=False)
    link: str = Field(nullable=False)
    dataset_pid: str = Field(nullable=False, index=True)

    # optional
    size: Optional[int] = Field(default=None,
                                sa_column=Column(
                                BigInteger, 
                                nullable=True))
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

    last_updated: datetime = Field(
        sa_column=Column(
            DateTime(timezone=False),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        )
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

class FileRawRecordStore:
    def __init__(self, engine):
        self.engine = engine

    def init_schema(self):
        SQLModel.metadata.create_all(self.engine)
        return True

    def create(self, record: FileRecord) -> FileRawRecord:
        try:
            with Session(self.engine) as session:
                session.add(record)
                session.commit()
                session.refresh(record)
                return record
        except IntegrityError as e:
                print(f"IntegrityError while creating FileRecord: {e}")
                session.rollback()

    def get_by_pid(self, dataset_pid: str) -> Optional[FileRawRecord]:
        with Session(self.engine) as session:
            stmt = select(FileRawRecord).where(
                FileRawRecord.dataset_pid == dataset_pid
            )
            return session.exec(stmt).first()

class FileRecordStore:
    def __init__(self, engine):
        self.engine = engine

    def init_schema(self):
        SQLModel.metadata.create_all(self.engine)
        return True

    def create(self, record: FileRecord) -> FileRecord:
        try:
            with Session(self.engine) as session:
                session.add(record)
                session.commit()
                session.refresh(record)
                return record
        except IntegrityError as e:
                print(f"IntegrityError while creating FileRecord: {e}")
                session.rollback()

    def get(self, file_id: int) -> Optional[FileRecord]:
        with Session(self.engine) as session:
            return session.get(FileRecord, file_id)

    def get_by_pid(self, dataset_pid: str) -> Optional[FileRecord]:
        with Session(self.engine) as session:
            stmt = select(FileRecord).where(
                FileRecord.dataset_pid == dataset_pid
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

    def delete(self, file_id: int) -> bool:
        with Session(self.engine) as session:
            record = session.get(FileRecord, file_id)
            if not record:
                return False

            session.delete(record)
            session.commit()
            return True

