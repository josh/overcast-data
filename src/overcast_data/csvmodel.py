import logging
import typing
from collections.abc import Callable
from dataclasses import Field, is_dataclass
from datetime import date, datetime, timedelta
from typing import (
    Any,
    ClassVar,
    Protocol,
    TypeVar,
    Union,
    cast,
)

logger = logging.getLogger("csvorm")

_CAST_STR_TO_VALUE = Callable[[str], Any]
_CAST_VALUE_TO_STR = Callable[[Any], str]

_STR_TO_VALUE_REGISTERY: dict[Any, _CAST_STR_TO_VALUE] = {}
_VALUE_TO_STR_REGISTERY: dict[Any, _CAST_VALUE_TO_STR] = {}


def register_cast(
    typ: type,
    fromstr: _CAST_STR_TO_VALUE,
    tostr: _CAST_VALUE_TO_STR = str,
) -> None:
    _STR_TO_VALUE_REGISTERY[typ] = fromstr
    _VALUE_TO_STR_REGISTERY[typ] = tostr

    def fromstr_optional(s: str) -> Any:
        if s == "":
            return None
        return fromstr(s)

    def tostr_optional(v: Any) -> str:
        if v is None:
            return ""
        return tostr(v)

    _STR_TO_VALUE_REGISTERY[typ | None] = fromstr_optional
    _VALUE_TO_STR_REGISTERY[typ | None] = tostr_optional


def _register_cast_alias(from_type: type, to_type: type) -> None:
    logger.debug(f"Registering cast alias: {from_type} -> {to_type}")
    fromstr = _STR_TO_VALUE_REGISTERY[from_type]
    tostr = _VALUE_TO_STR_REGISTERY[from_type]
    register_cast(to_type, fromstr, tostr)


def csvstr(obj: Any) -> str:
    typ = type(obj)
    if typ not in _VALUE_TO_STR_REGISTERY:
        raise ValueError(f"Unsupported type: {typ}")
    s = _VALUE_TO_STR_REGISTERY[typ](obj)
    assert isinstance(s, str), f"Expected str, got {repr(s)}"
    return s


def castcsvstr(typ: type | object, s: str) -> Any:
    assert isinstance(s, str), f"Expected str, got {repr(s)}"
    if typ in _STR_TO_VALUE_REGISTERY:
        return _STR_TO_VALUE_REGISTERY[typ](s)

    if otyp := _get_newtype_origin_type(typ):
        if otyp in _STR_TO_VALUE_REGISTERY:
            _register_cast_alias(otyp, cast(type, typ))
            return _STR_TO_VALUE_REGISTERY[typ](s)

    if otyp := _get_optional_origin_type(typ):
        if s == "":
            return None
        return castcsvstr(otyp, s)

    raise ValueError(f"Unsupported type: {typ}")


def _get_newtype_origin_type(typ: object) -> type | None:
    if not hasattr(typ, "__supertype__"):
        return None
    return cast(type, getattr(typ, "__supertype__"))


def _get_optional_origin_type(typ: object) -> type | None:
    if typing.get_origin(typ) != Union:
        return None
    args = typing.get_args(typ)
    if len(args) != 2 or args[1] != type(None):  # noqa: E721
        return None
    return cast(type, args[0])


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


def ascsvdict(obj: DataclassInstance) -> dict[str, str]:
    assert is_dataclass(obj), f"{repr(obj)} is not a dataclass"
    return {name: csvstr(getattr(obj, name)) for name in obj.__dataclass_fields__}


def ascsvrow(obj: DataclassInstance) -> tuple[str, ...]:
    assert is_dataclass(obj), f"{repr(obj)} is not a dataclass"
    return tuple(csvstr(getattr(obj, name)) for name in obj.__dataclass_fields__)


DataclassType = TypeVar("DataclassType", bound=DataclassInstance)


def fromcsvdict(cls: type[DataclassType], d: dict[str, str]) -> DataclassType:
    kwargs: dict[str, Any] = {
        name: castcsvstr(field.type, d[name])
        for name, field in cls.__dataclass_fields__.items()
    }
    return cls(**kwargs)


register_cast(type(None), fromstr=lambda _: None, tostr=lambda _: "")
register_cast(bool, fromstr=lambda s: s == "1", tostr=lambda v: "1" if v else "0")
register_cast(int, fromstr=int)
register_cast(float, fromstr=float)
register_cast(str, fromstr=str)
register_cast(date, fromstr=date.fromisoformat, tostr=date.isoformat)
register_cast(datetime, fromstr=datetime.fromisoformat, tostr=datetime.isoformat)
register_cast(
    timedelta,
    fromstr=lambda s: timedelta(seconds=int(s)),
    tostr=lambda td: str(int(td.total_seconds())),
)
