from .models import Person, ConditionOccurrence
from sqlalchemy.orm import Session

def seed(session: Session):
    people = [
        Person(person_id=1, year_of_birth=1980, gender="M"),
        Person(person_id=2, year_of_birth=1975, gender="F"),
        Person(person_id=3, year_of_birth=1990, gender="M"),
        Person(person_id=4, year_of_birth=1986, gender="F"),
    ]
    conds = [
        ConditionOccurrence(condition_occurrence_id=1, person_id=1, condition="type 2 diabetes", date="2022-05-01"),
        ConditionOccurrence(condition_occurrence_id=2, person_id=2, condition="type 2 diabetes", date="2021-07-03"),
        ConditionOccurrence(condition_occurrence_id=3, person_id=3, condition="hypertension", date="2023-03-15"),
        ConditionOccurrence(condition_occurrence_id=4, person_id=4, condition="type 2 diabetes", date="2020-10-10"),
    ]
    session.add_all(people + conds)
    session.commit()
