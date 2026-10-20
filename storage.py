"""Durable database storage for the AI receptionist.

The schema works with Railway PostgreSQL via DATABASE_URL and uses SQLite only as a
local-development default. Application code should use these helpers rather than
holding call or lead records in process memory.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, create_engine, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


def utcnow() -> datetime:
    """Return a timezone-aware timestamp suitable for durable call records."""
    return datetime.now(timezone.utc)


def _database_url() -> str:
    """Return a SQLAlchemy-compatible DATABASE_URL, including legacy Railway URLs."""
    url = os.getenv("DATABASE_URL", "sqlite:///./receptionist.db").strip()
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
        return "postgresql+psycopg2://" + url[len("postgresql://"):]
    return url


DATABASE_URL = _database_url()
_engine_options: dict[str, Any] = {"pool_pre_ping": True, "future": True}
if DATABASE_URL.startswith("sqlite"):
    _engine_options["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, **_engine_options)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Call(Base):
    __tablename__ = "calls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    call_sid: Mapped[str | None] = mapped_column(String(64), unique=True, index=True, nullable=True)
    stream_sid: Mapped[str | None] = mapped_column(String(64), unique=True, index=True, nullable=True)
    caller_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="initiated")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    last_status_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    # Extracted lead fields. Transcript content is normalized into TranscriptLine.
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    rv_details: Mapped[str | None] = mapped_column(Text, nullable=True)
    goals: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_setup: Mapped[str | None] = mapped_column(Text, nullable=True)
    location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    extraction_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    extracted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    transcript_lines: Mapped[list["TranscriptLine"]] = relationship(
        back_populates="call", cascade="all, delete-orphan", order_by="TranscriptLine.sequence"
    )
    status_events: Mapped[list["CallStatusEvent"]] = relationship(
        back_populates="call", cascade="all, delete-orphan", order_by="CallStatusEvent.occurred_at"
    )


class TranscriptLine(Base):
    __tablename__ = "transcript_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    call_id: Mapped[int] = mapped_column(ForeignKey("calls.id", ondelete="CASCADE"), index=True)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    speaker: Mapped[str] = mapped_column(String(32), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    call: Mapped[Call] = relationship(back_populates="transcript_lines")


class CallStatusEvent(Base):
    __tablename__ = "call_status_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    call_id: Mapped[int] = mapped_column(ForeignKey("calls.id", ondelete="CASCADE"), index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    call: Mapped[Call] = relationship(back_populates="status_events")


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)


def init_db() -> None:
    """Create missing tables. Schema migrations should be used for later alterations."""
    Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope() -> Iterator[Any]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _status_time(value: datetime | None = None) -> datetime:
    if value is None:
        return utcnow()
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def upsert_call(
    call_sid: str,
    *,
    caller_number: str | None = None,
    status: str | None = None,
    occurred_at: datetime | None = None,
) -> int:
    """Create/update a call and append an auditable status event when provided."""
    if not call_sid:
        raise ValueError("call_sid is required for durable call storage")
    event_time = _status_time(occurred_at)
    with session_scope() as session:
        call = session.query(Call).filter_by(call_sid=call_sid).one_or_none()
        if call is None:
            call = Call(call_sid=call_sid, started_at=event_time)
            session.add(call)
            session.flush()
        if caller_number:
            call.caller_number = caller_number
            if not call.phone:
                call.phone = caller_number
        if status:
            normalized = status.strip().lower()[:32]
            call.status = normalized
            call.last_status_at = event_time
            session.add(CallStatusEvent(call_id=call.id, status=normalized, occurred_at=event_time))
            if normalized in {"completed", "busy", "failed", "no-answer", "canceled"}:
                call.completed_at = event_time
        session.flush()
        return call.id


def attach_stream(call_sid: str, stream_sid: str | None, caller_number: str | None = None) -> None:
    """Record stream linkage and a caller number learned after the initial webhook."""
    if not call_sid:
        return
    with session_scope() as session:
        call = session.query(Call).filter_by(call_sid=call_sid).one_or_none()
        if call is None:
            call = Call(call_sid=call_sid)
            session.add(call)
        if stream_sid:
            call.stream_sid = stream_sid
        if caller_number:
            call.caller_number = caller_number
            if not call.phone:
                call.phone = caller_number


def caller_for_call(call_sid: str) -> str | None:
    if not call_sid:
        return None
    with session_scope() as session:
        call = session.query(Call.caller_number).filter_by(call_sid=call_sid).one_or_none()
        return call[0] if call else None


def append_transcript_line(call_sid: str, speaker: str, content: str) -> None:
    """Persist one transcript line at capture time, rather than only at call end."""
    if not call_sid or not content:
        return
    with session_scope() as session:
        call = session.query(Call).filter_by(call_sid=call_sid).one_or_none()
        if call is None:
            call = Call(call_sid=call_sid)
            session.add(call)
            session.flush()
        sequence = (session.query(func.max(TranscriptLine.sequence)).filter_by(call_id=call.id).scalar() or 0) + 1
        session.add(TranscriptLine(call_id=call.id, sequence=sequence, speaker=speaker[:32], content=content))


def update_extracted_fields(call_sid: str, fields: dict[str, Any], extraction_status: str = "complete") -> None:
    """Save extraction output. Only known field names may be written."""
    if not call_sid:
        return
    allowed = {"name", "phone", "email", "rv_details", "goals", "current_setup", "location", "summary"}
    with session_scope() as session:
        call = session.query(Call).filter_by(call_sid=call_sid).one_or_none()
        if call is None:
            call = Call(call_sid=call_sid)
            session.add(call)
        for key in allowed:
            value = fields.get(key)
            if value:
                setattr(call, key, str(value))
        call.extraction_status = extraction_status[:32]
        call.extracted_at = utcnow()


def call_as_dict(call: Call) -> dict[str, Any]:
    """Return a view suitable for the existing email and dashboard presentation."""
    timestamp = call.started_at or call.created_at
    return {
        "timestamp": timestamp.isoformat() if timestamp else "",
        "call_sid": call.call_sid,
        "caller_number": call.caller_number,
        "phone": call.phone,
        "name": call.name,
        "email": call.email,
        "rv_details": call.rv_details,
        "goals": call.goals,
        "current_setup": call.current_setup,
        "location": call.location,
        "summary": call.summary,
        "status": call.status,
        "transcript": [f"{line.speaker}: {line.content}" for line in call.transcript_lines],
    }


def get_call_view(call_sid: str) -> dict[str, Any] | None:
    if not call_sid:
        return None
    with session_scope() as session:
        call = session.query(Call).filter_by(call_sid=call_sid).one_or_none()
        return call_as_dict(call) if call else None


def list_call_views() -> list[dict[str, Any]]:
    with session_scope() as session:
        calls = session.query(Call).order_by(Call.started_at.desc(), Call.id.desc()).all()
        return [call_as_dict(call) for call in calls]


def call_count() -> int:
    with session_scope() as session:
        return int(session.query(func.count(Call.id)).scalar() or 0)


def get_receptionist_state() -> tuple[bool, str]:
    with session_scope() as session:
        enabled = session.get(AppSetting, "receptionist_active")
        toggled_at = session.get(AppSetting, "receptionist_toggled_at")
        active = enabled.value.lower() == "true" if enabled else True
        return active, toggled_at.value if toggled_at else "Never"


def set_receptionist_state(active: bool) -> None:
    with session_scope() as session:
        setting = session.get(AppSetting, "receptionist_active")
        if setting is None:
            setting = AppSetting(key="receptionist_active", value="true")
            session.add(setting)
        setting.value = "true" if active else "false"
        timestamp = session.get(AppSetting, "receptionist_toggled_at")
        if timestamp is None:
            timestamp = AppSetting(key="receptionist_toggled_at", value="")
            session.add(timestamp)
        timestamp.value = utcnow().strftime("%Y-%m-%d %I:%M %p UTC")
