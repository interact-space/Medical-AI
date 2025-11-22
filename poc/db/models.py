from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Date

class Base(DeclarativeBase):
    pass

class Person(Base):
    __tablename__ = "person"
    person_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    year_of_birth: Mapped[int] = mapped_column(Integer, nullable=False)
    gender: Mapped[str] = mapped_column(String(1), nullable=False)  # 'M'/'F'

class ConditionOccurrence(Base):
    __tablename__ = "condition_occurrence"
    condition_occurrence_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    person_id: Mapped[int] = mapped_column(Integer, nullable=False)
    condition: Mapped[str] = mapped_column(String(128), nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)  # 'YYYY-MM-DD'
