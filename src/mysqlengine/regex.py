# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython

# Python imports
from typing import Union
from re import compile, Pattern

__all__ = ["Regex", "TableRegex"]


# Regex ------------------------------------------------------------------------------------------------
@cython.cclass
class Regex:
    """A set of regular expressions generated
    based on the given pattern and seperators.
    """

    # Pattern
    _pattern: str
    _pattern_lpin: str
    _pattern_rpin: str
    _pattern_dpin: str
    _pattern_lsep: str
    _pattern_rsep: str
    _pattern_dsep: str
    # Regex
    _re: Pattern[str]
    _re_lpin: Pattern[str]
    _re_rpin: Pattern[str]
    _re_dpin: Pattern[str]
    _re_lsep: Pattern[str]
    _re_rsep: Pattern[str]
    _re_dsep: Pattern[str]

    def __init__(self, pattern: str, sep: str = "[\s\.'\"\(\)]{1}") -> None:
        """A set of regular expressions generated
        based on the given pattern and seperators.

        :param pattern: `<str>` The base pattern.
        :param sep: `<str>` The pattern of the seperators. e.g. `"[\s\.'\"\(\)]{1}"`
        """

        # Pattern
        self._pattern = pattern
        self._pattern_lpin = "^" + pattern
        self._pattern_rpin = pattern + "$"
        self._pattern_dpin = "^" + pattern + "$"
        self._pattern_lsep = sep + pattern + "$"
        self._pattern_rsep = "^" + pattern + sep
        self._pattern_dsep = sep + pattern + sep
        # Regex
        self._re = compile(self._pattern)
        self._re_lpin = compile(self._pattern_lpin)
        self._re_rpin = compile(self._pattern_rpin)
        self._re_dpin = compile(self._pattern_dpin)
        self._re_lsep = compile(self._pattern_lsep)
        self._re_rsep = compile(self._pattern_rsep)
        self._re_dsep = compile(self._pattern_dsep)

    # Properties -------------------------------------------------------------
    @property
    def pattern(self) -> str:
        "The base pattern `<str>`."
        return self._pattern

    @property
    def pattern_lpin(self) -> str:
        """The left anchor pattern `<str>`.

        Formated as: `'^<pattern>'`.
        """
        return self._pattern_lpin

    @property
    def pattern_rpin(self) -> str:
        """The right anchor pattern `<str>`.

        Formated as: `'<pattern>$'`.
        """
        return self._pattern_rpin

    @property
    def pattern_dpin(self) -> str:
        """The left and right anchor pattern `<str>`.

        Formated as: `'^<pattern>$'`.
        """
        return self._pattern_dpin

    @property
    def pattern_lsep(self) -> str:
        """The left seperated pattern `<str>`.

        Formated as: `'<seperators><pattern>$'`.
        """
        return self._pattern_lsep

    @property
    def pattern_rsep(self) -> str:
        """The right seperated pattern `<str>`.

        Formated as: `'^<pattern><seperators>'`.
        """
        return self._pattern_rsep

    @property
    def pattern_dsep(self) -> str:
        """The left and right seperated pattern `<str>`.

        Formated as: `'<seperators><pattern><seperators>'`.
        """
        return self._pattern_dsep

    @property
    def re(self) -> Pattern[str]:
        "The base compiled regex `<Pattern[str]>`."
        return self._re

    @property
    def re_lpin(self) -> Pattern[str]:
        """The left anchor compiled regex `<Pattern[str]>`.

        Formated as: `'^<pattern>'`.
        """
        return self._re_lpin

    @property
    def re_rpin(self) -> Pattern[str]:
        """The right anchor compiled regex `<Pattern[str]>`.

        Formated as: `'<pattern>$'`.
        """
        return self._re_rpin

    @property
    def re_dpin(self) -> Pattern[str]:
        """The left and right anchor compiled regex `<Pattern[str]>`.

        Formated as: `'^<pattern>$'`.
        """
        return self._re_dpin

    @property
    def re_lsep(self) -> Pattern[str]:
        """The left seperated compiled regex `<Pattern[str]>`.

        Formated as: `'<seperators><pattern>$'`.
        """
        return self._re_lsep

    @property
    def re_rsep(self) -> Pattern[str]:
        """The right seperated compiled regex `<Pattern[str]>`.

        Formated as: `'^<pattern><seperators>'`.
        """
        return self._re_rsep

    @property
    def re_dsep(self) -> Pattern[str]:
        """The left and right seperated compiled regex `<Pattern[str]>`.

        Formated as: `'<seperators><pattern><seperators>'`.
        """
        return self._re_dsep

    # Special Methods --------------------------------------------------------
    def __repr__(self) -> str:
        return (
            "<Regex (pattern='%s')>"
            "\n[lpin_pattern='%s', rpin_pattern='%s', dpin_pattern='%s']"
            "\n[lsep_pattern='%s', rsep_pattern='%s', dsep_pattern='%s')]\n"
            % (
                self._pattern,
                self._pattern_lpin,
                self._pattern_rpin,
                self._pattern_dpin,
                self._pattern_lsep,
                self._pattern_rsep,
                self._pattern_dsep,
            )
        )

    def __hash__(self) -> int:
        return self.__repr__().__hash__()

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, Regex):
            return self._pattern_dsep == __o.pattern_dsep
        else:
            return False


# Table Regex ------------------------------------------------------------------------------------------
@cython.cclass
class TableRegex:
    """The set of regular expressions for `<Table>` class."""

    _name: Regex
    _name_gp: Regex
    _time_gp: Regex
    _fname: Regex
    _fname_gp: Regex
    _ftime_gp: Regex

    def __init__(
        self,
        db_name: str,
        tb_name: str,
        time_unit: Union[str, None] = None,
        sep: str = "[\s\.'\"\(\)]{1}",
    ) -> None:
        """The set of regular expressions for `<Table>` class.

        :param db_name: `<str>` The database name.
        :param tb_name: `<str>` The table name.
        :param time_unit: `<str>` The time unit. Defaults to `None`.
            - If `None`, generate regular expressions for normal Table.
            - If specified, generate regular expressions for TimeTable.

        :param sep: `<str>` The pattern of the seperators. Defaults to `"[\s\.'\"\(\)]{1}"`.
        """
        self.__setup(db_name, tb_name, time_unit, sep)

    # Properties -------------------------------------------------------------
    @property
    def name(self) -> Regex:
        """Regular expressions for Table name `<Regex>`.

        ### Example:
        - Normal `<Table>`: base pattern as `'<tb_name>'`.
        - `<TimeTable>`: base pattern as `'<tb_name>_[0-9]{8}'`.
        """
        return self._name

    @property
    def name_gp(self) -> Regex:
        """Regular expressions for Table name (grouped) `<Regex>`.

        ### Example:
        - Normal `<Table>`: base pattern as `'(<tb_name>)'`.
        - `<TimeTable>`: base pattern as `'(<tb_name>)_[0-9]{8}'`.
        """
        return self._name_gp

    @property
    def time_gp(self) -> Regex:
        """Regular expressions for Table time (grouped) `<Regex>`.
        (Only applicable to `<TimeTable>`)

        ### Example:
        - `<TimeTable>`: base pattern as `'<tb_name>_([0-9]{4})([0-9]{2})([0-9]{2})'`.
        - Normal `<Table>` returns `None`.
        """
        return self._time_gp

    @property
    def fname(self) -> Regex:
        """Regular expressions for Table full name `<Regex>`.

        ### Example:
        - Normal `<Table>`: base pattern as `'<db_name>.<tb_name>'`.
        - `<TimeTable>`: base pattern as `'<db_name>.<tb_name>_[0-9]{8}'`.
        """
        return self._fname

    @property
    def fname_gp(self) -> Regex:
        """Regular expressions for Table full name (grouped) `<Regex>`.

        ### Example:
        - Normal `<Table>`: base pattern as `'<db_name>.(<tb_name>)'`.
        - `<TimeTable>`: base pattern as `'<db_name>.(<tb_name>)_[0-9]{8}'`.
        """
        return self._fname_gp

    @property
    def ftime_gp(self) -> Regex:
        """Regular expressions for Table (full name) time (grouped) `<Regex>`.

        ### Example:
        - `<TimeTable>`: base pattern as `'<db_name>.<tb_name>_([0-9]{4})([0-9]{2})([0-9]{2})'`.
        - Normal `<Table>` returns `None`.
        """
        return self._ftime_gp

    # Setup ------------------------------------------------------------------
    def __setup(
        self,
        db_name: str,
        tb_name: str,
        time_unit: Union[str, None],
        sep: str,
    ) -> None:
        # Prefix
        db_pfix: str = db_name + "."

        # Normal Table
        if time_unit is None:
            # Pattern
            name_gp: str = "(" + tb_name + ")"

            # Table name
            self._name = Regex(tb_name, sep)
            self._name_gp = Regex(name_gp, sep)
            # Table full name
            self._fname = Regex(db_pfix + tb_name, sep)
            self._fname_gp = Regex(db_pfix + name_gp, sep)
            # Table time
            self._time_gp = None
            self._ftime_gp = None

        # TimeTable
        else:
            # Pattern
            if time_unit == "year":
                name: str = tb_name + "_[0-9]{4}"
                name_gp: str = "(" + tb_name + ")_[0-9]{4}"
                time_gp: str = tb_name + "_([0-9]{4})"
            elif time_unit == "month":
                name: str = tb_name + "_[0-9]{6}"
                name_gp: str = "(" + tb_name + ")_[0-9]{6}"
                time_gp: str = tb_name + "_([0-9]{4})([0-9]{2})"
            else:
                name: str = tb_name + "_[0-9]{8}"
                name_gp: str = "(" + tb_name + ")_[0-9]{8}"
                time_gp: str = tb_name + "_([0-9]{4})([0-9]{2})([0-9]{2})"

            # Table name
            self._name = Regex(name, sep)
            self._name_gp = Regex(name_gp, sep)
            # Table full name
            self._fname = Regex(db_pfix + name, sep)
            self._fname_gp = Regex(db_pfix + name_gp, sep)
            # Table time
            self._time_gp = Regex(time_gp, sep)
            self._ftime_gp = Regex(db_pfix + time_gp, sep)

    # Special Methods --------------------------------------------------------
    def __repr__(self) -> str:
        return "<TableRegex (pattern='%s')>" % self._name._pattern

    def __hash__(self) -> int:
        return self.__repr__().__hash__()

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, TableRegex):
            return (
                self._name == __o.name
                and self._fname == __o.fname
                and self._time_gp == __o.time_gp
            )
        else:
            return False
