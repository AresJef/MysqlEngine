# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython
from cython.cimports import numpy as np  # type: ignore
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.set import PySet_Add as set_add  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.list import PyList_GET_SIZE as list_len  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.bytes import PyBytes_Check as is_bytes  # type: ignore
from cython.cimports.cpython.string import PyString_Check as is_str  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_READ_CHAR as uni_loc  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Replace as uni_replace  # type: ignore
from cython.cimports.cpython.unicode import Py_UNICODE_ISDIGIT as uni_isdigit  # type: ignore
from cython.cimports.cpython.unicode import Py_UNICODE_ISALPHA as uni_isalpha  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Contains as str_contains  # type: ignore
from cython.cimports.cytimes.pydt import pydt  # type: ignore
from cython.cimports.cytimes import cydatetime as cydt, cytimedelta  # type: ignore

np.import_array()
datetime.import_datetime()

# Python imports
import datetime
from decimal import Decimal
from hashlib import sha256, md5
from typing import Any, Union, Literal
from re import compile, sub, MULTILINE, Pattern
from pandas import DataFrame
from cytimes.pydt import pydt
from cytimes import cydatetime as cydt, cytimedelta


# Constants --------------------------------------------------------------------------------------------------------------
ONLY_WHITESPACE_RE: Pattern[str] = compile("^[ \t]+$", MULTILINE)
LEAD_WHITESPACE_RE: Pattern[str] = compile("(^[ \t]*)(?:[^ \t\n])", MULTILINE)


# Math -------------------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
@cython.cdivision(True)
@cython.exceptval(check=False)
def _round_half_away(num: cython.double, ndigits: cython.int) -> cython.double:
    """(cfunc) Round a number half away from zero.

    :param num: `<float>` Number to be rounded.
    :param ndigits: `<int>` Round to the nth digits after the decimal point. Defaults to `0`.
    :return `<float>`: Rounded number.
    """
    return _round_half_away_factor(num, int(10**ndigits))


def round_half_away(num: Union[str, int, float, Decimal], ndigits: int = 0) -> float:
    """Round a number half away from zero.

    :param num: Number to be rounded.
    :param ndigits: `<int>` Round to the nth digits after the decimal point. Defaults to `0`.
    :return `<float>`: Rounded number.
    """
    return _round_half_away(float(num), ndigits)


@cython.cfunc
@cython.inline(True)
@cython.cdivision(True)
@cython.exceptval(check=False)
def _round_half_away_factor(num: cython.double, f: cython.longlong) -> cython.double:
    """(cfunc) Round a number half away from zero (factor provided)

    :param num: `<float> Number to be rounded.
    :param f: `<int>` Equivalent to `10**ndigits`. Defaults to `10`.
        - `ndigit` is the nth digits after the decimal point to round to.
    :return `<float>`: Rounded number
    """
    adj: cython.double = 0.5 if num >= 0 else -0.5
    base: cython.longlong = int(num * f + adj)
    return float(base) / f


def round_half_away_factor(num: Union[str, int, float, Decimal], f: int = 10) -> float:
    """Round a number half away from zero (factor provided)

    :param num: `<str/int/float/Decimal>` Number to be rounded.
    :param f: `<int>` Equivalent to `10**ndigits`. Defaults to `10`.
        - `ndigit` is the nth digits after the decimal point to round to.
    :return `<float>`: Rounded number
    """
    return _round_half_away_factor(num, f)


# String -----------------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
def _str_clean_name(s: str) -> str:
    """(cfunc) Clean name for Database/Table/Column.
    (Strictly only keep ASCII characters in [a-zA-Z0-9_].)
    """
    length: cython.int = str_len(s)
    b_arr: bytearray = bytearray()
    char: cython.char
    idx: cython.int
    for idx in range(length):
        # Get charactor
        try:
            char = uni_loc(s, idx)
        except Exception:
            continue

        # Check alpha, digit & underscore
        if uni_isalpha(char) or uni_isdigit(char) or char == 95:
            b_arr.append(char)

    # Return decoded string
    return b_arr.decode("utf-8")


def str_clean_name(name: str) -> str:
    """Clean name for Database/Table/Column.
    (Strictly only keep ASCII characters in [a-zA-Z0-9_].)
    """
    return _str_clean_name(name)


@cython.cfunc
@cython.inline(True)
@cython.boundscheck(True)
def _str_dedent(text: str) -> str:
    """(cfunc) Remove any common leading whitespace from every line `<str>`.

    This can be used to make triple-quoted strings line up with the left
    edge of the display, while still presenting them in the source code
    in indented form.

    Note that tabs and spaces are both treated as whitespace, but they
    are not equal: the lines "  hello" and "\\thello" are
    considered to have no common leading whitespace.

    Entirely blank lines are normalized to a newline character.
    """
    # Empty string
    if text is None or not text:
        return ""

    # Look for the longest leading string of spaces and tabs common to all lines.
    margin: str = None
    text: str = ONLY_WHITESPACE_RE.sub("", text)
    indents: list[str] = LEAD_WHITESPACE_RE.findall(text)
    indent: str
    i: cython.longlong
    x: str
    y: str
    for indent in indents:
        if margin is None:
            margin = indent

        # Current line more deeply indented than previous winner:
        # no change (previous winner is still on top).
        elif indent.startswith(margin):
            pass

        # Current line consistent with and no deeper than previous winner:
        # it's the new winner.
        elif margin.startswith(indent):
            margin = indent

        # Find the largest common whitespace between current line and previous
        # winner.
        else:
            for i, (x, y) in enumerate(zip(margin, indent)):
                if x != y:
                    margin = margin[:i]
                    break

    if margin:
        text = sub(r"(?m)^" + margin, "", text)
    return text


def str_dedent(text: str) -> str:
    """Remove any common leading whitespace from every line `<str>`.

    This can be used to make triple-quoted strings line up with the left
    edge of the display, while still presenting them in the source code
    in indented form.

    Note that tabs and spaces are both treated as whitespace, but they
    are not equal: the lines "  hello" and "\\thello" are
    considered to have no common leading whitespace.

    Entirely blank lines are normalized to a newline character.
    """
    return _str_dedent(text)


@cython.cfunc
@cython.inline(True)
def _str_replace_iter(s: str, new: str, old: str, iterate: cython.int) -> str:
    """(cfunc) Replace an old character in a `str` with a new one in iteration.

    The `iterate <int>` argument determines the maximum iterations for replacing
    the old char by new char if the old char still exists after each replacement.
    Setting `iterate <= 0` means only replace once.
    """
    while str_contains(s, old) and iterate > 0:
        s = uni_replace(s, old, new, -1)
        iterate -= 1
    return s


@cython.cfunc
@cython.inline(True)
def _str_replace(s: str, new: str, old: str, iterate: cython.int) -> str:
    """(cfunc) Replace an old character in a `str` with a new one.

    The `iterate <int>` argument determines the maximum iterations for replacing
    the old char by new char if the old char still exists after each replacement.
    Setting `iterate <= 0` means only replace once.
    """
    if iterate > 0:
        return _str_replace_iter(s, new, old, iterate)
    else:
        return uni_replace(s, old, new, -1)


def str_replace(s: str, new: str, old: str, iterate: int = 0) -> str:
    """Replace an old character in a `str` with a new one.

    The `iterate <int>` argument determines the maximum iterations for replacing
    the old char by new char if the old char still exists after each replacement.
    Setting `iterate <= 0` means only replace once.
    """
    return _str_replace(s, new, old, iterate)


@cython.cfunc
@cython.inline(True)
def _str_replaces(s: str, new: str, olds: tuple, iterate: cython.int) -> str:
    """(cfunc) Replace multiple old characters in a `str` with a new one.

    The `iterate <int>` argument determines the maximum iterations for replacing
    the old char by new char if the old char still exists after each replacement.
    Setting `iterate <= 0` means only replace once.
    """
    if not olds:
        return s

    old: str
    if iterate > 0:
        for old in olds:
            s = _str_replace_iter(s, new, old, iterate)
    else:
        for old in olds:
            s = uni_replace(s, old, new, -1)
    return s


def str_replaces(s: str, new: str, *olds: str, iterate: int = 0) -> str:
    """Replace multiple old characters in a `str` with a new one.

    The `iterate <int>` argument determines the maximum iterations for replacing
    the old char by new char if the old char still exists after each replacement.
    Setting `iterate <= 0` means only replace once.
    """
    return _str_replaces(s, new, olds, iterate)


@cython.cfunc
@cython.inline(True)
def _str_replace_pairs(s: str, pairs: tuple, iterate: cython.int) -> str:
    """(cfunc) Replace a `str` by pairs of (new, old) characters.

    The `iterate <int>` argument determines the maximum iterations for replacing
    the old char by new char if the old char still exists after each replacement.
    Setting `iterate <= 0` means only replace once.
    """
    if not pairs:
        return s

    new: str
    old: str
    if iterate > 0:
        for new, old in pairs:
            s = _str_replace_iter(s, new, old, iterate)
    else:
        for new, old in pairs:
            s = uni_replace(s, old, new, -1)
    return s


def str_replace_pairs(s: str, *pairs: tuple[str, str], iterate: int = 0) -> str:
    """Replace a `str` by pairs of (new, old) characters.

    The `iterate <int>` argument determines the maximum iterations for replacing
    the old char by new char if the old char still exists after each replacement.
    Setting `iterate <= 0` means only replace once.
    """
    return _str_replace_pairs(s, pairs, iterate)


@cython.cfunc
@cython.inline(True)
def _str_squeeze_spaces(s: str, strip: cython.bint) -> str:
    """(cfunc) Squeeze all multi-spaces to single-space for a `str`.

    :param s: `<str>` The string contains multi-spaces.
    :param strip: `<bool>` Whether to strip spaces after squeeze operation.
    :return `<str>`: Squeezed string.

    ### Example:
    >>> s = "a    b   c"
    >>> str_squeeze_spaces(s)
    >>> "a b c"
    """
    if strip:
        return _str_replace(s, " ", "  ", 999_999).strip()
    else:
        return _str_replace(s, " ", "  ", 999_999)


def str_squeeze_spaces(s: str, strip: bool = True) -> str:
    """Squeeze all multi-spaces to single-space for a `str`.

    :param s: `<str>` The string contains multi-spaces.
    :param strip: `<bool>` Whether to strip spaces after squeeze operation. Defaults to `True`.
    :return `<str>`: Squeezed string.

    ### Example:
    >>> s = "a    b   c"
    >>> str_squeeze_spaces(s)
    >>> "a b c"
    """
    return _str_squeeze_spaces(s, strip)


@cython.cfunc
@cython.inline(True)
def _str_parse_int(s: str) -> cython.int:
    """(cfunc) Parse an integer from a `str`.
    - Only the first integer in the `str` will be parsed.
    """
    # Get length
    length: cython.int = str_len(s)

    # Parse
    b_arr: bytearray = bytearray()
    char: cython.char
    prev_char: cython.char
    beg: cython.bint = False
    end: cython.bint = False
    neg: cython.bint = False
    idx: cython.int
    for idx in range(length):
        # End loop
        if end:
            break

        # Get charactor
        try:
            char = uni_loc(s, idx)
        except Exception:
            continue

        # Parsing
        if uni_isdigit(char):  # is digit
            if beg == False:  # . begin of the numeric
                if idx > 0:  # . parse sign
                    try:
                        prev_char = uni_loc(s, idx - 1)
                        if prev_char == 45:
                            neg = True
                    except Exception:
                        pass
                beg = True  # . change state
            b_arr.append(char)
        elif beg:  # .irrelevant
            end = True  # . change state

    # Convert to intger
    if b_arr:
        res: cython.int = int(b_arr.decode("utf-8"))
        if neg:
            res *= -1
        return res

    # Invalid
    raise ValueError(f"String {repr(s)} does not contain any numeric digits.")


def str_parse_int(s: str) -> int:
    """Parse an integer from a `str`.
    - Only the first integer in the `str` will be parsed.
    """
    return _str_parse_int(s)


@cython.cfunc
@cython.inline(True)
def _str_parse_float(s: str) -> cython.double:
    """(cfunc) Parse float from a `str`.
    - Only the first float in the `str` will be parsed.
    - If the float ends with a percent sign '%', it will be treated
      as a percentage, and the result will be divided by 100.
    """
    # Get length
    length: cython.int = str_len(s)

    # Parse
    b_arr: bytearray = bytearray()
    char: cython.char
    prev_char: cython.char
    next_char: cython.char
    beg: cython.bint = False
    end: cython.bint = False
    neg: cython.bint = False
    pct: cython.bint = False
    dot: cython.bint = False
    plc: cython.int = 0
    idx: cython.int
    for idx in range(length):
        # End loop
        if end:
            break

        # Get charactor
        try:
            char = uni_loc(s, idx)
        except Exception:
            continue

        # Parsing
        if uni_isdigit(char):  # is digit
            if beg == False:  # . begin of the numeric
                if idx > 0:  # . parse sign
                    try:
                        prev_char = uni_loc(s, idx - 1)
                        if prev_char == 45:
                            neg = True
                    except Exception:
                        pass
                beg = True  # . change state
            b_arr.append(char)
            if dot:  # . track decimal places
                plc += 1
        elif char == 46:  # . is dot '.'
            if dot:  # end if dot already exists
                end = True
            elif beg:  # dot between digits. e.g. 123.123
                b_arr.append(char)
                dot = True
            elif length > idx + 1:  # begin with dot. e.g. .123
                try:
                    next_char = uni_loc(s, idx + 1)
                    if uni_isdigit(next_char):
                        b_arr.append(char)
                        dot = True
                        beg = True
                except Exception:
                    pass
        elif beg:
            if char == 37:  # . is pct '%'
                pct = True
            end = True  # . change state

    # Convert to float
    if b_arr:
        res: cython.double = float(b_arr.decode("utf-8"))
        if pct:
            res = _round_half_away(res / 100, plc + 2)
        if neg:
            res *= -1
        return res

    # Invalid
    raise ValueError(f"String {repr(s)} does not contain any numeric digits.")


def str_parse_float(s: str) -> float:
    """Parse float from a `str`.
    - Only the first float in the `str` will be parsed.
    - If the float ends with a percent sign '%', it will be treated
      as a percentage, and the result will be divided by 100.
    """
    return _str_parse_float(s)


# Tuple ------------------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def _parse_dtypes(seq: tuple) -> set:
    """(cfunc) Parse the data types of the items in a tuple `<set[type]>.
    - Only the first level of the tuple will be parsed.
    """
    res: set = set()
    for item in seq:
        set_add(res, type(item))
    return res


def parse_dtypes(seq: tuple) -> set[type]:
    """Parse the data types of the items in a tuple `<set[type]>.
    - Only the first level of the tuple will be parsed.
    """
    return _parse_dtypes(seq)


# List -------------------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
def _list_duplicated(lst: list) -> cython.bint:
    "(cfunc) Check if a `<list>` has duplicate items `<bool>`."
    seen: set = set()
    for item in lst:
        if set_contains(seen, item):
            return True
        else:
            set_add(seen, item)
    return False


def list_duplicated(lst: list) -> bool:
    "Check if a `<list>` has duplicate items `<bool>`."
    return _list_duplicated(lst)


@cython.cfunc
@cython.inline(True)
def _list_duplicates(lst: list) -> list:
    "(cfunc) Return the duplicates of a list `<list>`."
    res: list = []
    seen: set = set()
    for item in lst:
        if set_contains(seen, item):
            list_append(res, item)
        else:
            set_add(seen, item)
    return res


def list_duplicates(lst: list) -> list:
    "Return the duplicates of a list `<list>`."
    return _list_duplicates(lst)


@cython.cfunc
@cython.inline(True)
def _list_drop_duplicates(lst: list) -> list:
    "(cfunc) Drop duplicates of a `<list>` while maintaining its order `<list>`."
    res: list = []
    seen: set = set()
    for item in lst:
        if not set_contains(seen, item):
            list_append(res, item)
            set_add(seen, item)
    return res


def list_drop_duplicates(lst: list) -> list:
    "Drop duplicates of a `<list>` while maintaining its order `<list>`."
    return _list_drop_duplicates(lst)


@cython.cfunc
@cython.inline(True)
@cython.cdivision(True)
@cython.boundscheck(True)
def _chunk_list(lst: list, size: cython.int, chunks: cython.int) -> list:
    """(cfunc) Chunk a list into sub-lists.

    :param lst: `<list>` List to be chunked.
    :param size: `<int>` Desired size of each chunk.
        - If 'size' is specified (`size > 0`), 'chunks' is ignored.
        - All chunks will have the desired 'size' except potentially the last one.

    :param chunks: `<int>` Desired number of chunks.
        - Only applicable when `size=None` or `size <= 0`.
        - Chunks will be as evenly sized as possible.

    :raises ValueError: If both 'size' and 'chunks' are not specified.
    :return <list[list]>: List of chunked sub-lists.
    """
    # Get length
    length: cython.int = list_len(lst)

    # Skip empty list
    if length == 0:
        return [lst]

    # Chunk by size
    if size > 0:
        if size >= length:
            return [lst]
        chunks = (length + size - 1) // size
        res: list = []
        start: cython.int = 0
        end: cython.int
        for _ in range(chunks):
            end = start + size
            res.append(lst[start:end])
            start = end
        return res

    # Chunk by chunks
    if chunks > 0:
        if chunks == 1:
            return [lst]
        if chunks > length:
            chunks = length
        size = length // chunks
        res: list = []
        start: cython.int = 0
        end: cython.int
        extra: cython.int = length % chunks
        if extra > 0:
            size_adj: cython.int
            for _ in range(chunks):
                size_adj = size + 1 if extra > 0 else size
                end = start + size_adj
                res.append(lst[start:end])
                start = end
                extra -= 1
        else:
            for _ in range(chunks):
                end = start + size
                res.append(lst[start:end])
                start = end
        return res

    # Invalid
    raise ValueError("Either 'size' or 'chunks' must be greater than zero.")


def chunk_list(
    lst: list,
    size: Union[int, None] = None,
    chunks: Union[int, None] = None,
) -> list[list]:
    """Chunk a list into sub-lists.

    :param lst: `<list>` List to be chunked.
    :param size: `<int>` Desired size of each chunk. Defaults to `None`.
        - If 'size' is specified (`size > 0`), 'chunks' is ignored.
        - All chunks will have the desired 'size' except potentially the last one.

    :param chunks: `<int>` Desired number of chunks. Defaults to `None`.
        - Only applicable when `size=None` or `size <= 0`.
        - Chunks will be as evenly sized as possible.

    :raises ValueError: If both 'size' and 'chunks' are not specified.
    :return <list[list]>: List of chunked sub-lists.
    """
    return _chunk_list(lst, size or -1, chunks or -1)


# DataFrame --------------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
@cython.cdivision(True)
@cython.boundscheck(True)
def _chunk_df(df: DataFrame, size: cython.int, chunks: cython.int) -> list:
    """(cfunc) Chunk a DataFrame into sub-DataFrames.

    :param df: `<DataFrame>` DataFrame to be chunked.
    :param size: `<int>` Desired size of each chunk.
        - If 'size' is specified (`size > 0`), 'chunks' is ignored.
        - All chunks will have the desired 'size' except potentially the last one.

    :param chunks: `<int>` Desired number of chunks.
        - Only applicable when `size=None` or `size <= 0`.
        - Chunks will be as evenly sized as possible.

    :raises ValueError: If both 'size' and 'chunks' are not specified.
    :return <list[DataFrame]>: List of chunked sub-DataFrames.
    """
    # Get length
    length: cython.int = len(df)

    # Skip empty DataFrame
    if length == 0:
        return [df]

    # Chunk by size
    if size > 0:
        if size >= length:
            return [df]
        chunks = (length + size - 1) // size
        res: list = []
        start: cython.int = 0
        end: cython.int
        for _ in range(chunks):
            end = start + size
            res.append(df[start:end])
            start = end
        return res

    # Chunk by chunks
    if chunks > 0:
        if chunks == 1:
            return [df]
        if chunks > length:
            chunks = length
        size = length // chunks
        res: list = []
        start: cython.int = 0
        end: cython.int
        extra: cython.int = length % chunks
        if extra > 0:
            size_adj: cython.int
            for _ in range(chunks):
                size_adj = size + 1 if extra > 0 else size
                end = start + size_adj
                res.append(df[start:end])
                start = end
                extra -= 1
        else:
            for _ in range(chunks):
                end = start + size
                res.append(df[start:end])
                start = end
        return res

    # Invalid
    raise ValueError("Either 'size' or 'chunks' must be greater than zero.")


def chunk_df(
    df: DataFrame,
    size: Union[int, None] = None,
    chunks: Union[int, None] = None,
) -> list[DataFrame]:
    """Chunk a DataFrame into sub-DataFrames.

    :param df: `<DataFrame>` DataFrame to be chunked.
    :param size: `<int>` Desired size of each chunk. Defaults to `None`.
        - If 'size' is specified (`size > 0`), 'chunks' is ignored.
        - All chunks will have the desired 'size' except potentially the last one.

    :param chunks: `<int>` Desired number of chunks. Defaults to `None`.
        - Only applicable when `size=None` or `size <= 0`.
        - Chunks will be as evenly sized as possible.

    :raises ValueError: If both 'size' and 'chunks' are not specified.
    :return <list[DataFrame]>: List of chunked sub-DataFrames.
    """
    return _chunk_df(df, size or -1, chunks or -1)


# Datetime ---------------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
def _cal_time_span(
    start: object,
    end: object,
    days: cython.int,
    unit: str,
    raise_error: cython.bint,
) -> tuple:
    """(cfunc) Calculate that start & end time span based on the given parameters.

    :param start, end, days: The time span parameters:
        - If `start` and `end` are specified -> (start, end)
        - If `start` and `days` are specified -> (start, start + days - 1)
        - If only `start` is specified -> (start, datetime.now()) or (datetime.now(), start)
        - If `end` and `days` are specified -> (end - days + 1, end)
        - If only `end` is specified -> (datetime.now(), end) or (end, datetime.now())
        - If only `days` is specified -> (datetime.now() - days + 1, datetime.now())
        - If none of the params are given, see `raise_error` argument.

    :param unit: `<str>` Adjustment for the time span..
        - This parameter floor the values of 'start' after the `unit`, and ceil the
          values of the 'end' after the `unit`.
        - For example: if `unit="Y"`, the 'start' is floored to the start of the year
          (at 00:00:00 on Jan 1) and the 'end' is ceiled to the end of the year
          (at 23:59:59.999999 on Dec 31).
        - Both `None` and `us` means no adjustment.

    :param raise_error: `<bool>` Whether to raise error if none of the span parameters are given.
        - If `True` -> raise `ValueError` for not receiving any parameter.
        - If `False` -> return `(None, None)`.

    :return (<`datetime`>, <`datetime`>): The start & end time span.
    """
    # Pre binding
    s_dt: datetime.datetime
    e_dt: datetime.datetime

    # 'start' is specified
    if start is not None:
        if end is not None:  # 'end' provided
            s_dt = pydt(start)._dt
            e_dt = pydt(end)._dt
        elif days > 0:  # 'days' provided
            s_dt = pydt(start)._dt
            e_dt = cydt.dt_add(s_dt, days - 1, 0, 0)
        else:  # 'start' only
            s_dt = pydt(start)._dt
            e_dt = cydt.gen_dt_local()
    # 'end' is specified
    elif end is not None:
        if days > 0:  # 'days' provided
            e_dt = pydt(end)._dt
            s_dt = cydt.dt_add(e_dt, 1 - days, 0, 0)
        else:  # 'end' only
            e_dt = pydt(end)._dt
            s_dt = cydt.gen_dt_local()
    # 'days' is specified
    elif days >= 0:
        e_dt = cydt.gen_dt_local()
        s_dt = cydt.dt_add(e_dt, 1 - days, 0, 0)
    # Invalid
    else:
        if raise_error:
            raise ValueError("<cal_time_span> Must provide as least one arguments.")
        else:
            return (None, None)

    # Adjust start & end
    if s_dt > e_dt:
        s_dt, e_dt = e_dt, s_dt

    # Unit not specified
    if unit is None or not unit:
        return (s_dt, e_dt)

    # Adjust to 'year'
    if unit == "year" or unit == "Y":
        s_dt = cydt.dt_replace(s_dt, -1, 1, 1, 0, 0, 0, 0, -1, -1)
        e_dt = cydt.dt_replace(e_dt, -1, 12, 31, 23, 59, 59, 999999, -1, -1)
    # Adjust to 'month'
    elif unit == "month" or unit == "M":
        s_dt = cydt.dt_replace(s_dt, -1, -1, 1, 0, 0, 0, 0, -1, -1)
        e_dt = cydt.dt_replace(e_dt, -1, -1, 31, 23, 59, 59, 999999, -1, -1)
    # Adjust to 'day'
    elif unit == "day" or unit == "D" or unit == "week" or unit == "W":
        s_dt = cydt.dt_replace(s_dt, -1, -1, -1, 0, 0, 0, 0, -1, -1)
        e_dt = cydt.dt_replace(e_dt, -1, -1, -1, 23, 59, 59, 999999, -1, -1)
    # Adjust to 'hour'
    elif unit == "hour" or unit == "h":
        s_dt = cydt.dt_replace(s_dt, -1, -1, -1, -1, 0, 0, 0, -1, -1)
        e_dt = cydt.dt_replace(e_dt, -1, -1, -1, -1, 59, 59, 999999, -1, -1)
    # Adjust to 'minute'
    elif unit == "minute" or unit == "m":
        s_dt = cydt.dt_replace(s_dt, -1, -1, -1, -1, -1, 0, 0, -1, -1)
        e_dt = cydt.dt_replace(e_dt, -1, -1, -1, -1, -1, 59, 999999, -1, -1)
    # Adjust to 'second'
    elif unit == "second" or unit == "s":
        s_dt = cydt.dt_replace(s_dt, -1, -1, -1, -1, -1, -1, 0, -1, -1)
        e_dt = cydt.dt_replace(e_dt, -1, -1, -1, -1, -1, -1, 999999, -1, -1)
    # Adjust to 'microsecond'
    elif unit == "microsecond" or unit == "us":
        pass
    # Invalid
    else:
        raise ValueError("<cal_time_span> Invalid 'unit': {}".format(unit))

    # Return
    return (s_dt, e_dt)


def cal_time_span(
    start: Union[str, datetime.date, datetime.datetime] = None,
    end: Union[str, datetime.date, datetime.datetime] = None,
    days: int = None,
    unit: Literal["Y", "M", "W", "D", "h", "m", "s", "us"] = None,
    raise_error: bool = True,
) -> tuple[datetime.datetime, datetime.datetime]:
    """Calculate that start & end time span based on the given parameters.

    :param start, end, days: The time span parameters:
        - If `start` and `end` are specified -> (start, end)
        - If `start` and `days` are specified -> (start, start + days - 1)
        - If only `start` is specified -> (start, datetime.now()) or (datetime.now(), start)
        - If `end` and `days` are specified -> (end - days + 1, end)
        - If only `end` is specified -> (datetime.now(), end) or (end, datetime.now())
        - If only `days` is specified -> (datetime.now() - days + 1, datetime.now())
        - If none of the params are given, see `raise_error` argument.

    :param unit: `<str>` Adjustment for the time span. Defaults to `None`.
        - This parameter floor the values of 'start' after the `unit`, and ceil the
          values of the 'end' after the `unit`.
        - For example: if `unit="Y"`, the 'start' is floored to the start of the year
          (at 00:00:00 on Jan 1) and the 'end' is ceiled to the end of the year
          (at 23:59:59.999999 on Dec 31).
        - Both `None` and `us` means no adjustment.

    :param raise_error: `<bool>` Whether to raise error if none of the span parameters are given. Defaults to `True`.
        - If `True` -> raise `ValueError` for not receiving any parameter.
        - If `False` -> return `(None, None)`.

    :return (<`datetime`>, <`datetime`>): The start & end time span.
    """
    return _cal_time_span(start, end, days or -1, unit, raise_error)


@cython.cfunc
@cython.inline(True)
def _gen_time_span(
    start: object,
    end: object,
    days: cython.int,
    unit: str,
    raise_error: cython.bint,
) -> list:
    """(cfunc) Generate the time span (list[datetime]) based on the given parameters.

    :param start, end, days: The time span parameters:
        - If `start` and `end` are specified -> [start, ... end]
        - If `start` and `days` are specified -> [start, ... start + days - 1]
        - If only `start` is specified -> [start, ... datetime.now()] or [datetime.now(), ... start]
        - If `end` and `days` are specified -> [end - days + 1, ... end]
        - If only `end` is specified -> [datetime.now(), ... end] or [end, ... datetime.now()]
        - If only `days` is specified -> [datetime.now() - days + 1, ... datetime.now()]
        - If none of the params are given, see `raise_error` argument.

    :param unit: `<str>` The unit of the time span.
        - This parameter sets the interval between each datetime in the time span.
        - For example: if `unit="Y"` (interval 1 year), the return list will be
          composed of datetimes with 1 year difference from the `start` to the `end`.

    :param raise_error: `<bool>` Whether to raise error if none of the span parameters are given.
        - If `True` -> raise `ValueError` for not receiving any parameter.
        - If `False` -> return `None`.

    :return <list[datetime]>: The time span.
    """
    # Pre binding
    s_dt: datetime.datetime
    e_dt: datetime.datetime

    # 'start' is specified
    if start is not None:
        if end is not None:  # 'end' provided
            s_dt = pydt(start)._dt
            e_dt = pydt(end)._dt
        elif days > 0:  # 'days' provided
            s_dt = pydt(start)._dt
            e_dt = cydt.dt_add(s_dt, days - 1, 0, 0)
        else:  # 'start' only
            s_dt = pydt(start)._dt
            e_dt = cydt.gen_dt_local()
    # 'end' is specified
    elif end is not None:
        if days > 0:  # 'days' provided
            e_dt = pydt(end)._dt
            s_dt = cydt.dt_add(e_dt, 1 - days, 0, 0)
        else:  # 'end' only
            e_dt = pydt(end)._dt
            s_dt = cydt.gen_dt_local()
    # 'days' is specified
    elif days >= 0:
        e_dt = cydt.gen_dt_local()
        s_dt = cydt.dt_add(e_dt, 1 - days, 0, 0)
    # Invalid
    else:
        if raise_error:
            raise ValueError("<gen_time_span> Must provide as least one arguments.")
        else:
            return None

    # Adjust start & end
    if s_dt > e_dt:
        s_dt, e_dt = e_dt, s_dt

    # Pre binding
    res: list = [s_dt]
    delta: cython.int
    i: cython.int
    # Generate time span - 'year'
    if unit == "year" or unit == "Y":
        delta = pydt(e_dt).between(s_dt, "Y", False)
        if delta > 0:
            year: cython.int = cydt.get_year(s_dt)
            for i in range(1, delta):
                res.append(cydt.dt_replace(s_dt, year + i))
            res.append(e_dt)
    # Generate time span - 'month'
    elif unit == "month" or unit == "M":
        delta = pydt(e_dt).between(s_dt, "M", False)
        if delta > 0:
            for i in range(1, delta):
                res.append(s_dt + cytimedelta(months=i))
            res.append(e_dt)
    # Generate time span - 'week'
    elif unit == "week" or unit == "W":
        delta = pydt(e_dt).between(s_dt, "W", False)
        if delta > 0:
            for i in range(1, delta):
                res.append(cydt.dt_add(s_dt, i * 7, 0, 0))
            res.append(e_dt)
    # Generate time span - 'day'
    elif unit == "day" or unit == "D":
        delta = pydt(e_dt).between(s_dt, "D", False)
        if delta > 0:
            for i in range(1, delta):
                res.append(cydt.dt_add(s_dt, i, 0, 0))
            res.append(e_dt)
    # Generate time span - 'hour'
    elif unit == "hour" or unit == "h":
        delta = pydt(e_dt).between(s_dt, "h", False)
        if delta > 0:
            for i in range(1, delta):
                res.append(cydt.dt_add(s_dt, 0, 3600 * i, 0))
            res.append(e_dt)
    # Generate time span - 'minute'
    elif unit == "minute" or unit == "m":
        delta = pydt(e_dt).between(s_dt, "m", False)
        if delta > 0:
            for i in range(1, delta):
                res.append(cydt.dt_add(s_dt, 0, 60 * i, 0))
            res.append(e_dt)
    # Generate time span - 'second'
    elif unit == "second" or unit == "s":
        delta = pydt(e_dt).between(s_dt, "s", False)
        if delta > 0:
            for i in range(1, delta):
                res.append(cydt.dt_add(s_dt, 0, i, 0))
            res.append(e_dt)
    # Generate time span - 'microsecond'
    elif unit == "microsecond" or unit == "us":
        delta = pydt(e_dt).between(s_dt, "us", False)
        if delta > 0:
            for i in range(1, delta):
                res.append(cydt.dt_add(s_dt, 0, 0, i))
            res.append(e_dt)
    # Invalid
    else:
        raise ValueError("<gen_time_span> Invalid 'unit': {}".format(unit))

    # Return
    return res


def gen_time_span(
    start: Union[str, datetime.date, datetime.datetime] = None,
    end: Union[str, datetime.date, datetime.datetime] = None,
    days: Union[int, None] = None,
    unit: Literal["Y", "M", "W", "D", "h", "m", "s", "us"] = "D",
    raise_error: bool = True,
) -> list[datetime.datetime]:
    """Generate the time span (list[datetime]) based on the given parameters.

    :param start, end, days: The time span parameters:
        - If `start` and `end` are specified -> [start, ... end]
        - If `start` and `days` are specified -> [start, ... start + days - 1]
        - If only `start` is specified -> [start, ... datetime.now()] or [datetime.now(), ... start]
        - If `end` and `days` are specified -> [end - days + 1, ... end]
        - If only `end` is specified -> [datetime.now(), ... end] or [end, ... datetime.now()]
        - If only `days` is specified -> [datetime.now() - days + 1, ... datetime.now()]
        - If none of the params are given, see `raise_error` argument.

    :param unit: `<str>` The unit of the time span. Defaults to `"D"`.
        - This parameter sets the interval between each datetime in the time span.
        - For example: if `unit="Y"` (interval 1 year), the return list will be
          composed of datetimes with 1 year difference from the `start` to the `end`.

    :param raise_error: `<bool>` Whether to raise error if none of the span parameters are given. Defaults to `True`.
        - If `True` -> raise `ValueError` for not receiving any parameter.
        - If `False` -> return `None`.

    :return <list[datetime]>: The time span.
    """
    return _gen_time_span(start, end, days or -1, unit, raise_error)


# Hash -------------------------------------------------------------------------------------------------------------------
@cython.cfunc
@cython.inline(True)
def _hash_md5(obj: object) -> str:
    """(cfunc) MD5 hash an object.

    :param obj: `<Any>` Object can be stringified.
    :raises ValueError: If failed to md5 hash the object.
    :return <'str'>: The md5 hashed value in string.
    """
    try:
        if is_str(obj):
            val = obj.encode("utf-8")
        elif is_bytes(obj):
            val = obj
        else:
            val = str(obj).encode("utf-8")
    except Exception as err:
        raise ValueError(f"Failed to md5 object: {repr(obj)}.\nError: {err}") from err
    return md5(val).hexdigest()


def hash_md5(obj: Any) -> str:
    """MD5 hash an object.

    :param obj: `<Any>` Object can be stringified.
    :raises ValueError: If failed to md5 hash the object.
    :return <'str'>: The md5 hashed value in string.
    """
    return _hash_md5(obj)


@cython.cfunc
@cython.inline(True)
def _hash_sha256(obj: object) -> str:
    """(cfunc) SHA256 hash an object.

    :param obj: `<Any>` Object can be stringified.
    :raises ValueError: If failed to sha256 hash the object.
    :return <'str'>: The sha256 hashed value in string.
    """
    try:
        if is_str(obj):
            val = obj.encode("utf-8")
        elif is_bytes(obj):
            val = obj
        else:
            val = str(obj).encode("utf-8")
    except Exception as err:
        raise ValueError(
            f"Failed to SHA256 object: {repr(obj)}.\nError: {err}"
        ) from err
    return sha256(val).hexdigest()


def hash_sha256(obj: Any) -> str:
    """SHA256 hash an object.

    :param obj: `<Any>` Object can be stringified.
    :raises ValueError: If failed to sha256 hash the object.
    :return <'str'>: The sha256 hashed value in string.
    """
    return _hash_sha256(obj)
