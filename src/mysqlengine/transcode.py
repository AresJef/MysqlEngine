# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython
from cython.cimports import numpy as np  # type: ignore
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.dict import PyDict_Check as is_dict  # type: ignore
from cython.cimports.cpython.dict import PyDict_Values as dict_vals  # type: ignore
from cython.cimports.cpython.dict import PyDict_Items as dict_items  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_Check as is_tuple  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Replace as uni_replace  # type: ignore
from cython.cimports.cytimes import cymath, cydatetime as cydt  # type: ignore
from cython.cimports.mysqlengine import constant  # type: ignore

np.import_array()
datetime.import_datetime()

# Python imports
from typing import Union, Callable
from re import compile
from decimal import Decimal
from time import struct_time
import datetime, numpy as np
from pandas import DatetimeIndex, TimedeltaIndex
from pandas import Timestamp, Timedelta, Series, DataFrame
from cytimes import cymath, cydatetime as cydt
from _collections_abc import dict_values, dict_keys
from mysqlengine import constant

__all__ = [
    "encode_item",
    "escape_item",
    "encode_args",
    "escape_args",
    "DECODERS",
]


# Constants --------------------------------------------------------------------------------------------------------------
# fmt: off
ESCAPE_TABLE: list = [chr(x) for x in range(128)]
ESCAPE_TABLE[0] = "\\0"
ESCAPE_TABLE[ord("\\")] = "\\\\"
ESCAPE_TABLE[ord("\n")] = "\\n"
ESCAPE_TABLE[ord("\r")] = "\\r"
ESCAPE_TABLE[ord("\032")] = "\\Z"
ESCAPE_TABLE[ord('"')] = '\\"'
ESCAPE_TABLE[ord("'")] = "\\'"

DECODE_DT_RE: object = compile(r"(\d{1,4})-(\d{1,2})-(\d{1,2})[T ](\d{1,2}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?")
DECODE_TIME_RE: object = compile(r"(\d{1,2}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?")
DECODE_DELTA_RE: object = compile(r"(-)?(\d{1,3}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?")
# fmt: on


# Encoder ================================================================================================================
# Base encoders ----------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_bool(val: cython.bint) -> str:
    return "1" if val else "0"


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_int(val: cython.longlong) -> str:
    return str(val)


@cython.cfunc
@cython.inline(True)
def _encode_float(val: object) -> str:
    if not cymath.is_finite(val):
        raise ValueError("MySQL only support finite `float`, instead: '%s'" % repr(val))

    f: str = repr(val)
    return f if "e" in f else f + "e0"


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_decimal(val: object) -> str:
    return str(val)


@cython.cfunc
@cython.inline(True)
def _translate_str(val: str) -> str:
    return val.translate(ESCAPE_TABLE)


@cython.cfunc
@cython.inline(True)
def _encode_str_bslash(val: str) -> str:
    return "'" + _translate_str(val) + "'"


@cython.cfunc
@cython.inline(True)
def _encode_str_no_bslash(val: str) -> str:
    return "'" + uni_replace(val, "'", "''", -1) + "'"


@cython.cfunc
@cython.inline(True)
def _translate_bytes(val: bytes) -> str:
    return _translate_str(val.decode("ascii", "surrogateescape"))


@cython.cfunc
@cython.inline(True)
def _encode_bytes(val: bytes) -> str:
    return "'" + _translate_bytes(val) + "'"


@cython.cfunc
@cython.inline(True)
def _encode_bytes_pfix(val: bytes) -> str:
    return "_binary'" + _translate_bytes(val) + "'"


@cython.cfunc
@cython.inline(True)
def _encode_bytearray(val: bytearray) -> str:
    return _encode_bytes(bytes(val))


@cython.cfunc
@cython.inline(True)
def _encode_bytearray_pfix(val: bytearray) -> str:
    return _encode_bytes_pfix(bytes(val))


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_date(val: object) -> str:
    return "'%04d-%02d-%02d'" % (
        cydt.get_year(val),
        cydt.get_month(val),
        cydt.get_day(val),
    )


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_datetime(val: object) -> str:
    microsecond: cython.int = cydt.get_dt_microsecond(val)
    if microsecond:
        return "'%04d-%02d-%02d %02d:%02d:%02d.%06d'" % (
            cydt.get_year(val),
            cydt.get_month(val),
            cydt.get_day(val),
            cydt.get_dt_hour(val),
            cydt.get_dt_minute(val),
            cydt.get_dt_second(val),
            microsecond,
        )
    else:
        return "'%04d-%02d-%02d %02d:%02d:%02d'" % (
            cydt.get_year(val),
            cydt.get_month(val),
            cydt.get_day(val),
            cydt.get_dt_hour(val),
            cydt.get_dt_minute(val),
            cydt.get_dt_second(val),
        )


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_datetime64(val: object) -> str:
    # Add back epoch seconds
    microseconds: cython.longlong = cydt.dt64_to_microseconds(val) + cydt.EPOCH_US
    # Clip microseconds
    microseconds = cymath.clip(microseconds, cydt.DT_MIN_US, cydt.DT_MAX_US)
    # Calculate ymd
    ymd = cydt.ordinal_to_ymd(microseconds // cydt.US_DAY)
    # Calculate hms
    hms = cydt.microseconds_to_hms(microseconds)
    # Return isoformat
    if hms.microsecond:
        return "'%04d-%02d-%02d %02d:%02d:%02d.%06d'" % (
            ymd.year,
            ymd.month,
            ymd.day,
            hms.hour,
            hms.minute,
            hms.second,
            hms.microsecond,
        )
    else:
        return "'%04d-%02d-%02d %02d:%02d:%02d'" % (
            ymd.year,
            ymd.month,
            ymd.day,
            hms.hour,
            hms.minute,
            hms.second,
        )


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_time(val: object) -> str:
    "Convert `datetime.time` to ISO format."
    microsecond: cython.int = cydt.get_time_microsecond(val)
    if microsecond:
        return "'%02d:%02d:%02d.%06d'" % (
            cydt.get_time_hour(val),
            cydt.get_time_minute(val),
            cydt.get_time_second(val),
            microsecond,
        )
    else:
        return "'%02d:%02d:%02d'" % (
            cydt.get_time_hour(val),
            cydt.get_time_minute(val),
            cydt.get_time_second(val),
        )


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_struct_time(val: object) -> str:
    return _encode_datetime(
        cydt.gen_dt(val[0], val[1], val[2], val[3], val[4], val[5], 0, None, 0)
    )


@cython.cfunc
@cython.inline(True)
@cython.cdivision(True)
@cython.exceptval(check=False)
def _encode_timedelta(val: object) -> str:
    days: cython.int = cydt.get_delta_days(val)
    secs: cython.int = cydt.get_delta_seconds(val)
    hours: cython.int = secs // cydt.SEC_HOUR % 24 + days * 24
    minutes: cython.int = secs // cydt.SEC_MINUTE % 60
    seconds: cython.int = secs % 60
    microseconds: cython.int = cydt.get_delta_microseconds(val)
    if microseconds:
        return "'%02d:%02d:%02d.%06d'" % (hours, minutes, seconds, microseconds)
    else:
        return "'%02d:%02d:%02d'" % (hours, minutes, seconds)


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_timedelta64(val: object) -> str:
    us: cython.longlong = cydt.delta64_to_microseconds(val)
    days: cython.longlong = us // cydt.US_DAY
    secs: cython.longlong = us // cydt.US_SECOND
    hours: cython.longlong = secs // cydt.SEC_HOUR % 24 + days * 24
    minutes: cython.longlong = secs // cydt.SEC_MINUTE % 60
    seconds: cython.longlong = secs % 60
    microseconds: cython.longlong = us % cydt.US_SECOND
    if microseconds:
        return "'%02d:%02d:%02d.%06d'" % (hours, minutes, seconds, microseconds)
    else:
        return "'%02d:%02d:%02d'" % (hours, minutes, seconds)


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _encode_none(_) -> str:
    return "NULL"


# Encode item ------------------------------------------------------------------------------------------------------------
# Item encoder - backslash
@cython.cfunc
@cython.inline(True)
def _encode_list_bslash(val: list) -> str:
    items: list = []
    for item in val:
        list_append(items, _encode_item_bslash(item))
    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_tuple_bslash(val: tuple) -> str:
    items: list = []
    for item in val:
        list_append(items, _encode_item_bslash(item))
    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_sequence_bslash(val: object) -> str:
    return _encode_list_bslash(list(val))


@cython.cfunc
@cython.inline(True)
def _encode_dict_bslash(val: dict) -> str:
    return _encode_list_bslash(dict_vals(val))


@cython.cfunc
@cython.inline(True)
def _encode_datetime_index(val: object) -> str:
    items: list = []

    # Without timezone
    if val.tz is None:
        for item in val.values:
            list_append(items, _encode_datetime64(item))
    # With timezone
    else:
        for item in val:
            list_append(items, _encode_datetime(item))
    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_timedelta_index(val: object) -> str:
    items: list = []
    for item in val.values:
        list_append(items, _encode_timedelta64(item))
    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_series_bslash(val: object) -> str:
    items: list = []
    kind: str = val.dtype.kind

    # Data type optimization
    if kind == "O":  # object
        for item in val.values:
            list_append(items, _encode_item_bslash(item))
    elif kind == "b":  # bool
        for item in val.values:
            list_append(items, _encode_bool(item))
    elif kind == "i" or kind == "u":  # int
        for item in val.values:
            list_append(items, _encode_int(item))
    elif kind == "f":  # float
        for item in val.values:
            list_append(items, _encode_float(item))
    elif kind == "M":  # datetime64
        if val.dt.tz is None:  # without timezone
            for item in val.values:
                list_append(items, _encode_datetime64(item))
        else:  # with timezone
            for item in val:
                list_append(items, _encode_datetime(item))
    elif kind == "m":  # timedelta64
        for item in val.values:
            list_append(items, _encode_timedelta64(item))
    else:  # unknown dtype
        for item in val:
            list_append(items, _encode_item_bslash(item))

    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_df_bslash(val: object) -> str:
    rows: list = []
    for row in val.values:
        list_append(rows, _encode_list_bslash(list(row)))
    return "(" + ",".join(rows) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_item_bslash(val: object) -> str:
    return ITEM_ENCODERS_BACKSLASH[type(val)](val)


ITEM_ENCODERS_BACKSLASH: dict[type, cython.cfunc] = {
    # Base types
    bool: _encode_bool,
    np.bool_: _encode_bool,
    int: _encode_int,
    np.int_: _encode_int,
    np.int8: _encode_int,
    np.int16: _encode_int,
    np.int32: _encode_int,
    np.int64: _encode_int,
    np.uint: _encode_int,
    np.uint16: _encode_int,
    np.uint32: _encode_int,
    np.uint64: _encode_int,
    float: _encode_float,
    np.float_: _encode_float,
    np.float16: _encode_float,
    np.float32: _encode_float,
    np.float64: _encode_float,
    Decimal: _encode_decimal,
    str: _encode_str_bslash,
    bytes: _encode_bytes,
    bytearray: _encode_bytearray,
    datetime.date: _encode_date,
    datetime.datetime: _encode_datetime,
    Timestamp: _encode_datetime,
    np.datetime64: _encode_datetime64,
    struct_time: _encode_struct_time,
    datetime.time: _encode_time,
    datetime.timedelta: _encode_timedelta,
    Timedelta: _encode_timedelta,
    np.timedelta64: _encode_timedelta64,
    type(None): _encode_none,
    # Complex types
    list: _encode_list_bslash,
    tuple: _encode_tuple_bslash,
    set: _encode_sequence_bslash,
    frozenset: _encode_sequence_bslash,
    dict_keys: _encode_sequence_bslash,
    dict_values: _encode_sequence_bslash,
    dict: _encode_dict_bslash,
    np.ndarray: _encode_sequence_bslash,
    np.record: _encode_sequence_bslash,
    DatetimeIndex: _encode_datetime_index,
    TimedeltaIndex: _encode_timedelta_index,
    Series: _encode_series_bslash,
    DataFrame: _encode_df_bslash,
}


# Item encoder - no backslash
@cython.cfunc
@cython.inline(True)
def _encode_list_no_bslash(val: list) -> str:
    items: list = []
    for item in val:
        list_append(items, _encode_item_no_bslash(item))
    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_tuple_no_bslash(val: tuple) -> str:
    items: list = []
    for item in val:
        list_append(items, _encode_item_no_bslash(item))
    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_sequence_no_bslash(val: object) -> str:
    return _encode_list_no_bslash(list(val))


@cython.cfunc
@cython.inline(True)
def _encode_dict_no_bslash(val: dict) -> str:
    return _encode_list_no_bslash(dict_vals(val))


@cython.cfunc
@cython.inline(True)
def _encode_series_no_bslash(val: object) -> str:
    items: list = []
    kind: str = val.dtype.kind

    # Data type optimization
    if kind == "O":  # object
        for item in val.values:
            list_append(items, _encode_item_no_bslash(item))
    elif kind == "b":  # bool
        for item in val.values:
            list_append(items, _encode_bool(item))
    elif kind == "i" or kind == "u":  # int
        for item in val.values:
            list_append(items, _encode_int(item))
    elif kind == "f":  # float
        for item in val.values:
            list_append(items, _encode_float(item))
    elif kind == "M":  # datetime64
        if val.dt.tz is None:  # without timezone
            for item in val.values:
                list_append(items, _encode_datetime64(item))
        else:  # with timezone
            for item in val:
                list_append(items, _encode_datetime(item))
    elif kind == "m":  # timedelta64
        for item in val.values:
            list_append(items, _encode_timedelta64(item))
    else:  # unknown dtype
        for item in val:
            list_append(items, _encode_item_no_bslash(item))

    return "(" + ",".join(items) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_df_no_bslash(val: object) -> str:
    rows: list = []
    for row in val.values:
        list_append(rows, _encode_list_no_bslash(list(row)))
    return "(" + ",".join(rows) + ")"


@cython.cfunc
@cython.inline(True)
def _encode_item_no_bslash(val: object) -> str:
    return ITEM_ENCODERS_NO_BACKSLASH[type(val)](val)


ITEM_ENCODERS_NO_BACKSLASH: dict[type, cython.cfunc] = ITEM_ENCODERS_BACKSLASH | {
    # Base types
    str: _encode_str_no_bslash,
    # Complex types
    list: _encode_list_no_bslash,
    tuple: _encode_tuple_no_bslash,
    set: _encode_sequence_no_bslash,
    frozenset: _encode_sequence_no_bslash,
    dict_keys: _encode_sequence_no_bslash,
    dict_values: _encode_sequence_no_bslash,
    dict: _encode_dict_no_bslash,
    np.ndarray: _encode_sequence_no_bslash,
    np.record: _encode_sequence_no_bslash,
    Series: _encode_series_no_bslash,
    DataFrame: _encode_df_no_bslash,
}


# Item encoder - master
@cython.cfunc
@cython.inline(True)
def encode_item(val: object, backslash: cython.bint) -> str:
    "(cfunc) Escape item to literal `<str>`."
    try:
        if backslash:
            return _encode_item_bslash(val)
        else:
            return _encode_item_no_bslash(val)
    except Exception as err:
        raise ValueError("Cannot escape %s.\nError: %s" % (repr(val), err)) from err


def escape_item(val: object, backslash: cython.bint = True) -> str:
    "Escape item to literal `<str>`."
    return encode_item(val, backslash)


# Encode Arguments -------------------------------------------------------------------------------------------------------
# Arguments encoder - backslash
@cython.cfunc
@cython.inline(True)
def _encode_args_list_bslash(val: list) -> tuple:
    items: list = []
    for item in val:
        list_append(items, _encode_args_bslash(item))
    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_tuple_bslash(val: tuple) -> tuple:
    items: list = []
    for item in val:
        list_append(items, _encode_args_bslash(item))
    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_sequence_bslash(val: object) -> tuple:
    return _encode_args_list_bslash(list(val))


@cython.cfunc
@cython.inline(True)
def _encode_args_dict_bslash(val: dict) -> dict:
    res: dict = {}
    pair: tuple
    for pair in dict_items(val):
        dict_setitem(res, pair[0], _encode_args_bslash(pair[1]))
    return res


@cython.cfunc
@cython.inline(True)
def _encode_args_datetime_index(val: object) -> tuple:
    items: list = []

    # Without timezone
    if val.tz is None:
        for item in val.values:
            list_append(items, _encode_datetime64(item))
    # With timezone
    else:
        for item in val:
            list_append(items, _encode_datetime(item))
    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_timedelta_index(val: object) -> tuple:
    items: list = []
    for item in val.values:
        list_append(items, _encode_timedelta64(item))
    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_series_bslash(val: object) -> tuple:
    items: list = []
    kind: str = val.dtype.kind

    # Data type optimization
    if kind == "O":  # object
        for item in val.values:
            list_append(items, _encode_args_bslash(item))
    elif kind == "b":  # bool
        for item in val.values:
            list_append(items, _encode_bool(item))
    elif kind == "i" or kind == "u":  # int
        for item in val.values:
            list_append(items, _encode_int(item))
    elif kind == "f":  # float
        for item in val.values:
            list_append(items, _encode_float(item))
    elif kind == "M":  # datetime64
        if val.dt.tz is None:  # without timezone
            for item in val.values:
                list_append(items, _encode_datetime64(item))
        else:  # with timezone
            for item in val:
                list_append(items, _encode_datetime(item))
    elif kind == "m":  # timedelta64
        for item in val.values:
            list_append(items, _encode_timedelta64(item))
    else:  # unknown dtype
        for item in val:
            list_append(items, _encode_args_bslash(item))

    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_df_bslash(val: object) -> tuple:
    rows: list = []
    for row in val.values:
        list_append(rows, _encode_args_list_bslash(list(row)))
    return tuple(rows)


@cython.cfunc
@cython.inline(True)
def _encode_args_bslash(val: object) -> object:
    return ARGS_ENCODERS_BACKSLASH[type(val)](val)


ARGS_ENCODERS_BACKSLASH: dict[type, cython.cfunc] = ITEM_ENCODERS_BACKSLASH | {
    # Base types
    bytes: _encode_bytes_pfix,
    bytearray: _encode_bytearray_pfix,
    # Complex types
    list: _encode_args_list_bslash,
    tuple: _encode_args_tuple_bslash,
    set: _encode_args_sequence_bslash,
    frozenset: _encode_args_sequence_bslash,
    dict_keys: _encode_args_sequence_bslash,
    dict_values: _encode_args_sequence_bslash,
    dict: _encode_args_dict_bslash,
    np.ndarray: _encode_args_sequence_bslash,
    np.record: _encode_args_sequence_bslash,
    DatetimeIndex: _encode_args_datetime_index,
    TimedeltaIndex: _encode_args_timedelta_index,
    Series: _encode_args_series_bslash,
    DataFrame: _encode_args_df_bslash,
}


# Argument encoder - no backslash
@cython.cfunc
@cython.inline(True)
def _encode_args_list_no_bslash(val: list) -> tuple:
    items: list = []
    for item in val:
        list_append(items, _encode_args_no_bslash(item))
    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_tuple_no_bslash(val: tuple) -> tuple:
    items: list = []
    for item in val:
        list_append(items, _encode_args_no_bslash(item))
    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_sequence_no_bslash(val: object) -> tuple:
    return _encode_args_list_no_bslash(list(val))


@cython.cfunc
@cython.inline(True)
def _encode_args_dict_no_bslash(val: dict) -> dict:
    res: dict = {}
    pair: tuple
    for pair in dict_items(val):
        dict_setitem(res, pair[0], _encode_args_no_bslash(pair[1]))
    return res


@cython.cfunc
@cython.inline(True)
def _encode_args_series_no_bslash(val: object) -> tuple:
    items: list = []
    kind: str = val.dtype.kind

    # Data type optimization
    if kind == "O":  # object
        for item in val.values:
            list_append(items, _encode_args_no_bslash(item))
    elif kind == "b":  # bool
        for item in val.values:
            list_append(items, _encode_bool(item))
    elif kind == "i" or kind == "u":  # int
        for item in val.values:
            list_append(items, _encode_int(item))
    elif kind == "f":  # float
        for item in val.values:
            list_append(items, _encode_float(item))
    elif kind == "M":  # datetime64
        if val.dt.tz is None:  # without timezone
            for item in val.values:
                list_append(items, _encode_datetime64(item))
        else:  # with timezone
            for item in val:
                list_append(items, _encode_datetime(item))
    elif kind == "m":  # timedelta64
        for item in val.values:
            list_append(items, _encode_timedelta64(item))
    else:  # unknown dtype
        for item in val:
            list_append(items, _encode_args_no_bslash(item))

    return tuple(items)


@cython.cfunc
@cython.inline(True)
def _encode_args_df_no_bslash(val: object) -> tuple:
    rows: list = []
    for row in val.values:
        list_append(rows, _encode_args_list_no_bslash(list(row)))
    return tuple(rows)


@cython.cfunc
@cython.inline(True)
def _encode_args_no_bslash(val: object) -> object:
    return ARGS_ENCODERS_NO_BACKSLASH[type(val)](val)


ARGS_ENCODERS_NO_BACKSLASH: dict[type, cython.cfunc] = ITEM_ENCODERS_NO_BACKSLASH | {
    # Base types
    bytes: _encode_bytes_pfix,
    bytearray: _encode_bytearray_pfix,
    # Complex types
    list: _encode_args_list_no_bslash,
    tuple: _encode_args_tuple_no_bslash,
    set: _encode_args_sequence_no_bslash,
    frozenset: _encode_args_sequence_no_bslash,
    dict_keys: _encode_args_sequence_no_bslash,
    dict_values: _encode_args_sequence_no_bslash,
    dict: _encode_args_dict_no_bslash,
    np.ndarray: _encode_args_sequence_no_bslash,
    np.record: _encode_args_sequence_no_bslash,
    DatetimeIndex: _encode_args_datetime_index,
    TimedeltaIndex: _encode_args_timedelta_index,
    Series: _encode_args_series_no_bslash,
    DataFrame: _encode_args_df_no_bslash,
}


# Argument encoder - master
@cython.cfunc
@cython.inline(True)
def encode_args(val: object, backslash: cython.bint) -> object:
    """(cfunc) Escape arguments to literal `<tuple/dict>`.

    - If the given 'val' is type of `<dict>`, returns `<dict>`.
    - All other supported data types returns `<tuple>`.
    """
    try:
        # Dictionary
        if is_dict(val):
            if backslash:
                return _encode_args_dict_bslash(val)
            else:
                return _encode_args_dict_no_bslash(val)
        # Rest dtype
        else:
            if backslash:
                res = ARGS_ENCODERS_BACKSLASH[type(val)](val)
            else:
                res = ARGS_ENCODERS_NO_BACKSLASH[type(val)](val)
            return res if is_tuple(res) else (res,)

    except Exception as err:
        raise ValueError("Cannot escape %s.\nError: %s" % (repr(val), err)) from err


def escape_args(val: object, backslash: cython.bint = True) -> Union[tuple, dict]:
    """Escape arguments to literal `<tuple/dict>`.

    - If the given 'val' is type of `<dict>`, returns `<dict>`.
    - All other supported data types returns `<tuple>`.
    """
    return encode_args(val, backslash)


# Decoder ================================================================================================================
# Decode DATE
@cython.cfunc
@cython.inline(True)
@cython.boundscheck(True)
@cython.exceptval(check=False)
def _decode_date(val: str) -> object:
    try:
        gp: list = val.split("-", 2)
        return cydt.gen_date(int(gp[0]), int(gp[1]), int(gp[2]))
    except Exception:
        return val


@cython.ccall
def decode_date(val: Union[str, bytes, bytearray]) -> object:
    """Returns a DATE column as a `date` object:

    >>> decode_date('2007-02-26')
        # datetime.date(2007, 2, 26)

    Illegal values are returned as `str`:
    >>> decode_date('2007-02-31')
        # '2007-02-31'
    >>> decode_date('0000-00-00')
        # '0000-00-00'
    """
    if isinstance(val, (bytes, bytearray)):
        return _decode_date(val.decode("ascii"))
    else:
        return _decode_date(val)


# Decode DATETIME
@cython.cfunc
@cython.inline(True)
@cython.boundscheck(True)
@cython.exceptval(check=False)
def _decode_datetime(val: str) -> object:
    match: object = DECODE_DT_RE.match(val)
    if not match:
        return _decode_date(val)

    try:
        gp: tuple = match.groups()
        us = _decode_microseconds(gp[6])
        return cydt.gen_dt(
            int(gp[0]),
            int(gp[1]),
            int(gp[2]),
            int(gp[3]),
            int(gp[4]),
            int(gp[5]),
            us,
            None,
            0,
        )
    except Exception:
        return _decode_date(val)


@cython.ccall
def decode_datetime(val: Union[str, bytes, bytearray]) -> object:
    """Returns a DATETIME or TIMESTAMP column value as a `datetime` object:

    >>> decode_datetime('2007-02-25 23:06:20')
        # datetime.datetime(2007, 2, 25, 23, 6, 20)

    >>> decode_datetime('2007-02-25T23:06:20')
        # datetime.datetime(2007, 2, 25, 23, 6, 20)

    Illegal values are returned as `str`:
    >>> decode_datetime('2007-02-31T23:06:20')
        # '2007-02-31T23:06:20'
    >>> decode_datetime('0000-00-00 00:00:00')
        # '0000-00-00 00:00:00'
    """
    if isinstance(val, (bytes, bytearray)):
        return _decode_datetime(val.decode("ascii"))
    else:
        return _decode_datetime(val)


# Decode Time
@cython.cfunc
@cython.inline(True)
@cython.boundscheck(True)
@cython.exceptval(check=False)
def _decode_time(val: str) -> object:
    match: object = DECODE_TIME_RE.match(val)
    if not match:
        return _decode_timedelta(val)

    try:
        gp: tuple = match.groups()
        us = _decode_microseconds(gp[3])
        return cydt.gen_time(int(gp[0]), int(gp[1]), int(gp[2]), us, None, 0)
    except Exception:
        return _decode_timedelta(val)


@cython.ccall
def decode_time(val: Union[str, bytes, bytearray]) -> object:
    """Returns a TIME column as a time object:

    >>> convert_time('15:06:17')
        # datetime.time(15, 6, 17)

    Illegal values are returned as str:

    >>> convert_time('-25:06:17')
        # '-25:06:17'
    >>> convert_time('random crap')
        # 'random crap'

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.

    Also note that MySQL's TIME column corresponds more closely to
    Python's timedelta and not time.
    """
    if isinstance(val, (bytes, bytearray)):
        return _decode_time(val.decode("ascii"))
    else:
        return _decode_time(val)


# Decode timedelta
@cython.cfunc
@cython.inline(True)
@cython.boundscheck(True)
@cython.exceptval(check=False)
def _decode_timedelta(val: str) -> object:
    match: object = DECODE_DELTA_RE.match(val)
    if not match:
        return val

    try:
        gp: tuple = match.groups()
        sign: cython.int = -1 if gp[0] else 1
        hours: cython.int = int(gp[1])
        minutes: cython.int = int(gp[2])
        seconds: cython.longlong = int(gp[3])
        microseconds: cython.int = _decode_microseconds(gp[4])
        us: cython.longlong = sign * (
            hours * cydt.US_HOUR
            + minutes * cydt.US_MINUTE
            + seconds * cydt.US_SECOND
            + microseconds
        )
        return cydt.delta_fr_microseconds(us)
    except Exception:
        return val


@cython.ccall
def decode_timedelta(val: Union[str, bytes, bytearray]) -> object:
    """Returns a TIME column as a timedelta object:

    >>> convert_timedelta('25:06:17')
        # datetime.timedelta(days=1, seconds=3977)
    >>> convert_timedelta('-25:06:17')
        # datetime.timedelta(days=-2, seconds=82423)

    Illegal values are returned as string:

    >>> convert_timedelta('random crap')
        # 'random crap'

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    """
    if isinstance(val, (bytes, bytearray)):
        return _decode_timedelta(val.decode("ascii"))
    else:
        return _decode_timedelta(val)


# Functions
@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=True)
def _decode_microseconds(val: str) -> cython.int:
    if val is None:
        return 0
    try:
        return int(val.ljust(6, "0")[0:6])
    except Exception:
        return 0


DECODERS: dict[int, Callable] = {
    # Value `None` means pass through
    constant.FIELD_TYPE_BIT: None,
    constant.FIELD_TYPE_TINY: int,
    constant.FIELD_TYPE_SHORT: int,
    constant.FIELD_TYPE_LONG: int,
    constant.FIELD_TYPE_FLOAT: float,
    constant.FIELD_TYPE_DOUBLE: float,
    constant.FIELD_TYPE_LONGLONG: int,
    constant.FIELD_TYPE_INT24: int,
    constant.FIELD_TYPE_YEAR: int,
    constant.FIELD_TYPE_TIMESTAMP: decode_datetime,
    constant.FIELD_TYPE_DATETIME: decode_datetime,
    constant.FIELD_TYPE_TIME: decode_timedelta,
    constant.FIELD_TYPE_DATE: decode_date,
    constant.FIELD_TYPE_BLOB: None,
    constant.FIELD_TYPE_TINY_BLOB: None,
    constant.FIELD_TYPE_MEDIUM_BLOB: None,
    constant.FIELD_TYPE_LONG_BLOB: None,
    constant.FIELD_TYPE_STRING: None,
    constant.FIELD_TYPE_VAR_STRING: None,
    constant.FIELD_TYPE_VARCHAR: None,
    constant.FIELD_TYPE_DECIMAL: float,
    constant.FIELD_TYPE_NEWDECIMAL: float,
}
