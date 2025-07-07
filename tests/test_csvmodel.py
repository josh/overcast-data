from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import NewType

from overcast_data.csvmodel import ascsvdict, ascsvrow, castcsvstr, csvstr, fromcsvdict

NonEmptyStr = NewType("NonEmptyStr", str)
PostiveInt = NewType("PostiveInt", int)


@dataclass
class Person:
    name: str
    age: int
    birthday: date
    balance: float = 0.0
    is_student: bool = False


def test_csvstr() -> None:
    assert csvstr(None) == ""
    assert csvstr(True) == "1"
    assert csvstr(False) == "0"
    assert csvstr("Alice") == "Alice"
    assert csvstr(NonEmptyStr("Alice")) == "Alice"
    assert csvstr(42) == "42"
    assert csvstr(PostiveInt(42)) == "42"
    assert csvstr(3.14) == "3.14"
    assert csvstr(date(2020, 1, 1)) == "2020-01-01"
    assert csvstr(datetime(2020, 1, 1, 12, 0, 0)) == "2020-01-01T12:00:00"
    assert csvstr(timedelta(hours=1, minutes=15)) == "4500"


def test_fromcsvstr() -> None:
    assert castcsvstr(type(None), "") is None
    assert castcsvstr(bool, "1") is True
    assert castcsvstr(bool, "0") is False
    assert castcsvstr(int, "42") == 42
    assert castcsvstr(float, "3.14") == 3.14
    assert castcsvstr(str, "") == ""
    assert castcsvstr(str, "Alice") == "Alice"

    assert castcsvstr(NonEmptyStr, "Alice") == NonEmptyStr("Alice")
    assert castcsvstr(PostiveInt, "42") == PostiveInt(42)

    assert castcsvstr(bool | None, "1") is True
    assert castcsvstr(None | bool, "1") is True
    assert castcsvstr(bool | None, "0") is False
    assert castcsvstr(bool | None, "0") is False
    assert castcsvstr(bool | None, "") is None

    assert castcsvstr(int | None, "42") == 42
    assert castcsvstr(None | int, "42") == 42
    assert castcsvstr(int | None, "42") == 42
    assert castcsvstr(int | None, "42") == 42
    assert castcsvstr(int | None, "") is None

    assert castcsvstr(str | None, "") is None
    assert castcsvstr(str | None, "Hello") == "Hello"

    assert castcsvstr(date, "2020-01-01") == date(2020, 1, 1)
    assert castcsvstr(datetime, "2020-01-01T12:00:00") == datetime(2020, 1, 1, 12, 0, 0)
    assert castcsvstr(timedelta, "4500") == timedelta(hours=1, minutes=15)

    assert castcsvstr(PostiveInt | None, "42") == PostiveInt(42)
    assert castcsvstr(PostiveInt | None, "") is None


def test_ascsvrow() -> None:
    p = Person(
        name="Alice",
        age=18,
        birthday=date(1996, 1, 1),
        balance=9.99,
        is_student=True,
    )
    assert ascsvrow(p) == ("Alice", "18", "1996-01-01", "9.99", "1")


def test_ascsvdict() -> None:
    p = Person(
        name="Alice",
        age=18,
        birthday=date(1996, 1, 1),
        balance=9.99,
        is_student=True,
    )
    assert ascsvdict(p) == {
        "name": "Alice",
        "age": "18",
        "birthday": "1996-01-01",
        "balance": "9.99",
        "is_student": "1",
    }


def test_fromcsvdict() -> None:
    p = Person(
        name="Alice",
        age=18,
        birthday=date(1996, 1, 1),
        balance=9.99,
        is_student=True,
    )
    assert (
        fromcsvdict(
            Person,
            {
                "name": "Alice",
                "age": "18",
                "birthday": "1996-01-01",
                "balance": "9.99",
                "is_student": "1",
            },
        )
        == p
    )


@dataclass
class FooFlag:
    flag: bool | None


def test_fromcsvdict_optionals() -> None:
    f1 = FooFlag(flag=None)
    f2 = FooFlag(flag=True)
    f3 = FooFlag(flag=False)

    assert fromcsvdict(FooFlag, {"flag": ""}) == f1
    assert fromcsvdict(FooFlag, {"flag": "1"}) == f2
    assert fromcsvdict(FooFlag, {"flag": "0"}) == f3
