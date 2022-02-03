import enum

from sqlalchemy import (
    DECIMAL,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Interval,
    String,
    Text,
    Time,
    func,
)
from sqlalchemy.orm import relationship

from dijon.database import Base


class VenueTypeEnum(enum.IntEnum):
    NONE = 0
    IN_PERSON = 1
    VIRTUAL = 2
    HYBRID = 3


class DayOfWeekEnum(enum.IntEnum):
    SUNDAY = 1
    MONDAY = 2
    TUESDAY = 3
    WEDNESDAY = 4
    THURSDAY = 5
    FRIDAY = 6
    SATURDAY = 7


class ServiceBodyNawsCode(Base):
    __tablename__ = "service_body_naws_codes"

    id = Column(Integer, primary_key=True, index=True)
    root_server_id = Column(ForeignKey("root_servers.id"), nullable=False)
    root_server = relationship("RootServer", uselist=False)
    bmlt_id = Column(Integer, nullable=False)


class ServiceBody(Base):
    __tablename__ = "service_bodies"

    id = Column(Integer, primary_key=True, index=True)
    snapshot_id = Column(ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False)
    snapshot = relationship("Snapshot", uselist=False)
    bmlt_id = Column(Integer, nullable=False)
    parent_id = Column(ForeignKey("service_bodies.id"), nullable=True)
    parent = relationship("ServiceBody", remote_side=[id])
    name = Column(String(255), nullable=False)
    type = Column(String(20), nullable=False)
    description = Column(Text, nullable=True)
    url = Column(String(255), nullable=True)
    helpline = Column(String(255), nullable=True)
    world_id = Column(String(20), nullable=True)
    service_body_naws_code_id = Column(ForeignKey("service_body_naws_codes.id"), nullable=True)
    naws_code = relationship("ServiceBodyNawsCode", uselist=False)
    meetings = relationship("Meeting", back_populates="service_body")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class FormatNawsCode(Base):
    __tablename__ = "format_naws_codes"

    id = Column(Integer, primary_key=True, index=True)
    root_server_id = Column(ForeignKey("root_servers.id"), nullable=False)
    root_server = relationship("RootServer", uselist=False)
    bmlt_id = Column(Integer, nullable=False)


class Format(Base):
    __tablename__ = "formats"

    id = Column(Integer, primary_key=True, index=True)
    snapshot_id = Column(ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False)
    snapshot = relationship("Snapshot", uselist=False)
    bmlt_id = Column(Integer, nullable=False)
    key_string = Column(String(255), nullable=False)
    name = Column(String(255), nullable=True)
    world_id = Column(String(20), nullable=True)
    format_naws_code_id = Column(ForeignKey("format_naws_codes.id"), nullable=True)
    naws_code = relationship("FormatNawsCode", uselist=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class MeetingNawsCode(Base):
    __tablename__ = "meeting_naws_codes"

    id = Column(Integer, primary_key=True, index=True)
    root_server_id = Column(ForeignKey("root_servers.id", ondelete="CASCADE"), nullable=False)
    root_server = relationship("RootServer", uselist=False)
    bmlt_id = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Meeting(Base):
    __tablename__ = "meetings"

    id = Column(Integer, primary_key=True, index=True)
    snapshot_id = Column(ForeignKey("snapshots.id"), nullable=False)
    snapshot = relationship("Snapshot", uselist=False)
    bmlt_id = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False)
    day = Column(Enum(DayOfWeekEnum), nullable=False)
    service_body_id = Column(ForeignKey("service_bodies.id"), nullable=False)
    service_body = relationship("ServiceBody", back_populates="meetings", uselist=False)
    venue_type = Column(Enum(VenueTypeEnum), nullable=False)
    start_time = Column(Time, nullable=False)
    duration = Column(Interval, nullable=False)
    time_zone = Column(String(255), nullable=True)
    latitude = Column(DECIMAL(precision=15, scale=12, asdecimal=True), nullable=True)
    longitude = Column(DECIMAL(precision=15, scale=12, asdecimal=True), nullable=True)
    published = Column(Boolean, nullable=False)
    world_id = Column(String(20), nullable=True)
    meeting_formats = relationship("MeetingFormat", back_populates="meeting", cascade="all, delete", passive_deletes=True)
    meeting_naws_code_id = Column(ForeignKey("meeting_naws_codes.id"), nullable=True)
    naws_code = relationship("MeetingNawsCode", uselist=False)
    location_text = Column(Text, nullable=True)
    location_info = Column(Text, nullable=True)
    location_street = Column(Text, nullable=True)
    location_city_subsection = Column(Text, nullable=True)
    location_neighborhood = Column(Text, nullable=True)
    location_municipality = Column(Text, nullable=True)
    location_sub_province = Column(Text, nullable=True)
    location_province = Column(Text, nullable=True)
    location_postal_code_1 = Column(Text, nullable=True)
    location_nation = Column(Text, nullable=True)
    train_lines = Column(Text, nullable=True)
    bus_lines = Column(Text, nullable=True)
    comments = Column(Text, nullable=True)
    virtual_meeting_link = Column(Text, nullable=True)
    phone_meeting_number = Column(Text, nullable=True)
    virtual_meeting_additional_info = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class MeetingFormat(Base):
    __tablename__ = "meeting_formats"

    meeting_id = Column(ForeignKey("meetings.id", ondelete="CASCADE"), primary_key=True)
    format_id = Column(ForeignKey("formats.id", ondelete="CASCADE"), primary_key=True)
    meeting = relationship("Meeting", back_populates="meeting_formats", uselist=False)
    format = relationship("Format", uselist=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class RootServer(Base):
    __tablename__ = "root_servers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    url = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Snapshot(Base):
    __tablename__ = "snapshots"

    id = Column(Integer, primary_key=True, index=True)
    root_server_id = Column(ForeignKey("root_servers.id", ondelete="CASCADE"), nullable=False)
    root_server = relationship("RootServer", uselist=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
