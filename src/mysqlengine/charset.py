# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython

__all__ = [
    "Charset",
    "Charsets",
    "charset_by_id",
    "charset_by_name",
    "charset_by_name_and_collate",
    "MBLENGTH",
    "charsets",
]

# Constants ==================================================================================
MBLENGTH: dict[int, int] = {8: 1, 33: 3, 88: 2, 91: 2}


# Charset ====================================================================================
@cython.cclass
class Charset:
    """Represents a MySQL charset."""

    _id: cython.int
    _name: str
    _collate: str
    _is_default: cython.bint

    def __init__(
        self,
        id: cython.int,
        name: str,
        collate: str,
        is_default: cython.bint = False,
    ) -> None:
        """MySQL charset

        :param id: `<int>` The id of the charset.
        :param name: `<str>` The name of the charset.
        :param collate: `<str>` The collate of the charset.
        :param is_default: `<bool>` Whether belongs to the defualt charsets of MySQL.
        """
        self._id = id
        self._name = name.lower().strip()
        self._collate = collate.lower().strip()
        self._is_default = is_default

    # Properties
    @property
    def id(self) -> int:
        "ID of the charset `<int>`."
        return self._id

    @property
    def name(self) -> str:
        "Name of the charset `<str>`."
        return self._name

    @property
    def collate(self) -> str:
        "Collate of the charset `<str>`."
        return self._collate

    @property
    def is_default(self) -> bool:
        "Whether belongs to the default charsets of MySQL `<bool>`."
        return self._is_default

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_encoding(self) -> str:
        "(cfunc) Get the encoding of the charset `<str>`."
        if self._name == "utf8mb4" or self._name == "utf8mb3":
            return "utf8"
        if self._name == "latin1":
            return "cp1252"
        if self._name == "koi8r":
            return "koi8_r"
        if self._name == "koi8u":
            return "koi8_u"
        return self._name

    @property
    def encoding(self) -> str:
        "The encoding of the charset `<str>`."
        return self.get_encoding()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_is_binary(self) -> cython.bint:
        "(cfunc) Get whether the charset is binary `<bool>`."
        return self._id == 63

    @property
    def is_binary(self) -> bool:
        "Whether the charset is binary `<bool>`."
        return self.get_is_binary()

    def __repr__(self):
        return "<Charset (id=%d, name=%s, collate=%s)>" % (
            self._id,
            self._name,
            self._collate,
        )

    def __hash__(self) -> int:
        return self.__repr__.__hash__()


@cython.cclass
class Charsets:
    """The Collection of MySQL charsets."""

    _by_id: dict[int, Charset]
    _by_name: dict[str, Charset]
    _by_collate: dict[str, Charset]
    _by_name_and_collate: dict[str, Charset]

    def __init__(self) -> None:
        self._by_id = {}
        self._by_name = {}
        self._by_collate = {}
        self._by_name_and_collate = {}

    def add(self, charset: Charset) -> None:
        """Add MySQL charset to the collection.

        :param charset: An instance of `<Charset>`.
        """
        self._by_id[charset.id] = charset
        if charset.is_default:
            self._by_name[charset.name] = charset
        self._by_collate[charset.collate] = charset
        self._by_name_and_collate[charset.name + "_" + charset.collate] = charset

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get_by_id(self, id: cython.int) -> Charset:
        """(cfunc) Get MySQL charset by id.

        :param id: `<int>` The id of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """
        try:
            return self._by_id[id]
        except KeyError as err:
            raise KeyError("Unknown MySQL charset 'id': %d" % id) from err

    def get_by_id(self, id: int) -> Charset:
        """Get MySQL charset by id.

        :param id: `<int>` The id of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """
        return self._get_by_id(id)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get_by_name(self, name: str) -> Charset:
        """(cfunc) Get MySQL charset by name.

        :param name: `<str>` The name of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """
        name = str(name).lower()
        if name == "utf8":
            name = "utf8mb4"
        try:
            return self._by_name[name]
        except KeyError as err:
            raise KeyError("Unknown MySQL charset: %s" % repr(name)) from err

    def get_by_name(self, name: str) -> Charset:
        """Get MySQL charset by name.

        :param name: `<str>` The name of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """
        return self._get_by_name(name)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get_by_collate(self, collate: str) -> Charset:
        """(cfunc) Get MySQL charset by collation.

        :param collate: `<str>` The collation of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """
        collate = str(collate).lower()
        try:
            return self._by_collate[collate]
        except KeyError as err:
            raise KeyError("Unknown MySQL collation: %s" % repr(collate)) from err

    def get_by_collate(self, collate: str) -> Charset:
        """Get MySQL charset by collation.

        :param collate: `<str>` The collation of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """
        return self._get_by_collate(collate)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get_by_name_and_collate(self, name: str, collate: str) -> Charset:
        """(cfunc) Get MySQL charset by name and collate.

        :param name: `<str>` The name of the charset.
        :param collate: `<str>` The collate of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """

        name = str(name).lower()
        if name == "utf8":
            name = "utf8mb4"
        collate = str(collate).lower()
        key = name + "_" + collate
        try:
            return self._by_name_and_collate[key]
        except KeyError as err:
            raise KeyError(
                "Unknown MySQL charset %s & collate %s." % (repr(name), repr(collate))
            ) from err

    def get_by_name_and_collate(self, name: str, collate: str) -> Charset:
        """Get MySQL charset by name and collate.

        :param name: `<str>` The name of the charset.
        :param collate: `<str>` The collate of the charset.
        :raise `KeyError`: If the charset is not found.
        :return: `<Charset>` The MySQL charset.
        """
        return self._get_by_name_and_collate(name, collate)

    # Special methods
    def __del__(self):
        self._by_id = None
        self._by_name = None
        self._by_name_and_collate = None


# Initialize =================================================================================
charsets: Charsets = Charsets()

"""
TODO: update this script.

Generated with:

mysql -N -s -e "select id, character_set_name, collation_name, is_default
from information_schema.collations order by id;" | python -c "import sys
for l in sys.stdin.readlines():
    id, name, collation, is_default  = l.split(chr(9))
    if is_default.strip() == "Yes":
        print('_charsets.add(Charset(%s, \'%s\', \'%s\', True))' \
              % (id, name, collation))
    else:
        print('_charsets.add(Charset(%s, \'%s\', \'%s\'))' \
              % (id, name, collation, bool(is_default.strip()))
"""

charsets.add(Charset(1, "big5", "big5_chinese_ci", True))
charsets.add(Charset(2, "latin2", "latin2_czech_cs"))
charsets.add(Charset(3, "dec8", "dec8_swedish_ci", True))
charsets.add(Charset(4, "cp850", "cp850_general_ci", True))
charsets.add(Charset(5, "latin1", "latin1_german1_ci"))
charsets.add(Charset(6, "hp8", "hp8_english_ci", True))
charsets.add(Charset(7, "koi8r", "koi8r_general_ci", True))
charsets.add(Charset(8, "latin1", "latin1_swedish_ci", True))
charsets.add(Charset(9, "latin2", "latin2_general_ci", True))
charsets.add(Charset(10, "swe7", "swe7_swedish_ci", True))
charsets.add(Charset(11, "ascii", "ascii_general_ci", True))
charsets.add(Charset(12, "ujis", "ujis_japanese_ci", True))
charsets.add(Charset(13, "sjis", "sjis_japanese_ci", True))
charsets.add(Charset(14, "cp1251", "cp1251_bulgarian_ci"))
charsets.add(Charset(15, "latin1", "latin1_danish_ci"))
charsets.add(Charset(16, "hebrew", "hebrew_general_ci", True))
charsets.add(Charset(18, "tis620", "tis620_thai_ci", True))
charsets.add(Charset(19, "euckr", "euckr_korean_ci", True))
charsets.add(Charset(20, "latin7", "latin7_estonian_cs"))
charsets.add(Charset(21, "latin2", "latin2_hungarian_ci"))
charsets.add(Charset(22, "koi8u", "koi8u_general_ci", True))
charsets.add(Charset(23, "cp1251", "cp1251_ukrainian_ci"))
charsets.add(Charset(24, "gb2312", "gb2312_chinese_ci", True))
charsets.add(Charset(25, "greek", "greek_general_ci", True))
charsets.add(Charset(26, "cp1250", "cp1250_general_ci", True))
charsets.add(Charset(27, "latin2", "latin2_croatian_ci"))
charsets.add(Charset(28, "gbk", "gbk_chinese_ci", True))
charsets.add(Charset(29, "cp1257", "cp1257_lithuanian_ci"))
charsets.add(Charset(30, "latin5", "latin5_turkish_ci", True))
charsets.add(Charset(31, "latin1", "latin1_german2_ci"))
charsets.add(Charset(32, "armscii8", "armscii8_general_ci", True))
charsets.add(Charset(33, "utf8mb3", "utf8mb3_general_ci", True))
charsets.add(Charset(34, "cp1250", "cp1250_czech_cs"))
charsets.add(Charset(36, "cp866", "cp866_general_ci", True))
charsets.add(Charset(37, "keybcs2", "keybcs2_general_ci", True))
charsets.add(Charset(38, "macce", "macce_general_ci", True))
charsets.add(Charset(39, "macroman", "macroman_general_ci", True))
charsets.add(Charset(40, "cp852", "cp852_general_ci", True))
charsets.add(Charset(41, "latin7", "latin7_general_ci", True))
charsets.add(Charset(42, "latin7", "latin7_general_cs"))
charsets.add(Charset(43, "macce", "macce_bin"))
charsets.add(Charset(44, "cp1250", "cp1250_croatian_ci"))
charsets.add(Charset(45, "utf8mb4", "utf8mb4_general_ci", True))
charsets.add(Charset(46, "utf8mb4", "utf8mb4_bin"))
charsets.add(Charset(47, "latin1", "latin1_bin"))
charsets.add(Charset(48, "latin1", "latin1_general_ci"))
charsets.add(Charset(49, "latin1", "latin1_general_cs"))
charsets.add(Charset(50, "cp1251", "cp1251_bin"))
charsets.add(Charset(51, "cp1251", "cp1251_general_ci", True))
charsets.add(Charset(52, "cp1251", "cp1251_general_cs"))
charsets.add(Charset(53, "macroman", "macroman_bin"))
charsets.add(Charset(57, "cp1256", "cp1256_general_ci", True))
charsets.add(Charset(58, "cp1257", "cp1257_bin"))
charsets.add(Charset(59, "cp1257", "cp1257_general_ci", True))
charsets.add(Charset(63, "binary", "binary", True))
charsets.add(Charset(64, "armscii8", "armscii8_bin"))
charsets.add(Charset(65, "ascii", "ascii_bin"))
charsets.add(Charset(66, "cp1250", "cp1250_bin"))
charsets.add(Charset(67, "cp1256", "cp1256_bin"))
charsets.add(Charset(68, "cp866", "cp866_bin"))
charsets.add(Charset(69, "dec8", "dec8_bin"))
charsets.add(Charset(70, "greek", "greek_bin"))
charsets.add(Charset(71, "hebrew", "hebrew_bin"))
charsets.add(Charset(72, "hp8", "hp8_bin"))
charsets.add(Charset(73, "keybcs2", "keybcs2_bin"))
charsets.add(Charset(74, "koi8r", "koi8r_bin"))
charsets.add(Charset(75, "koi8u", "koi8u_bin"))
charsets.add(Charset(76, "utf8mb3", "utf8mb3_tolower_ci"))
charsets.add(Charset(77, "latin2", "latin2_bin"))
charsets.add(Charset(78, "latin5", "latin5_bin"))
charsets.add(Charset(79, "latin7", "latin7_bin"))
charsets.add(Charset(80, "cp850", "cp850_bin"))
charsets.add(Charset(81, "cp852", "cp852_bin"))
charsets.add(Charset(82, "swe7", "swe7_bin"))
charsets.add(Charset(83, "utf8mb3", "utf8mb3_bin"))
charsets.add(Charset(84, "big5", "big5_bin"))
charsets.add(Charset(85, "euckr", "euckr_bin"))
charsets.add(Charset(86, "gb2312", "gb2312_bin"))
charsets.add(Charset(87, "gbk", "gbk_bin"))
charsets.add(Charset(88, "sjis", "sjis_bin"))
charsets.add(Charset(89, "tis620", "tis620_bin"))
charsets.add(Charset(91, "ujis", "ujis_bin"))
charsets.add(Charset(92, "geostd8", "geostd8_general_ci", True))
charsets.add(Charset(93, "geostd8", "geostd8_bin"))
charsets.add(Charset(94, "latin1", "latin1_spanish_ci"))
charsets.add(Charset(95, "cp932", "cp932_japanese_ci", True))
charsets.add(Charset(96, "cp932", "cp932_bin"))
charsets.add(Charset(97, "eucjpms", "eucjpms_japanese_ci", True))
charsets.add(Charset(98, "eucjpms", "eucjpms_bin"))
charsets.add(Charset(99, "cp1250", "cp1250_polish_ci"))
charsets.add(Charset(192, "utf8mb3", "utf8mb3_unicode_ci"))
charsets.add(Charset(193, "utf8mb3", "utf8mb3_icelandic_ci"))
charsets.add(Charset(194, "utf8mb3", "utf8mb3_latvian_ci"))
charsets.add(Charset(195, "utf8mb3", "utf8mb3_romanian_ci"))
charsets.add(Charset(196, "utf8mb3", "utf8mb3_slovenian_ci"))
charsets.add(Charset(197, "utf8mb3", "utf8mb3_polish_ci"))
charsets.add(Charset(198, "utf8mb3", "utf8mb3_estonian_ci"))
charsets.add(Charset(199, "utf8mb3", "utf8mb3_spanish_ci"))
charsets.add(Charset(200, "utf8mb3", "utf8mb3_swedish_ci"))
charsets.add(Charset(201, "utf8mb3", "utf8mb3_turkish_ci"))
charsets.add(Charset(202, "utf8mb3", "utf8mb3_czech_ci"))
charsets.add(Charset(203, "utf8mb3", "utf8mb3_danish_ci"))
charsets.add(Charset(204, "utf8mb3", "utf8mb3_lithuanian_ci"))
charsets.add(Charset(205, "utf8mb3", "utf8mb3_slovak_ci"))
charsets.add(Charset(206, "utf8mb3", "utf8mb3_spanish2_ci"))
charsets.add(Charset(207, "utf8mb3", "utf8mb3_roman_ci"))
charsets.add(Charset(208, "utf8mb3", "utf8mb3_persian_ci"))
charsets.add(Charset(209, "utf8mb3", "utf8mb3_esperanto_ci"))
charsets.add(Charset(210, "utf8mb3", "utf8mb3_hungarian_ci"))
charsets.add(Charset(211, "utf8mb3", "utf8mb3_sinhala_ci"))
charsets.add(Charset(212, "utf8mb3", "utf8mb3_german2_ci"))
charsets.add(Charset(213, "utf8mb3", "utf8mb3_croatian_ci"))
charsets.add(Charset(214, "utf8mb3", "utf8mb3_unicode_520_ci"))
charsets.add(Charset(215, "utf8mb3", "utf8mb3_vietnamese_ci"))
charsets.add(Charset(223, "utf8mb3", "utf8mb3_general_mysql500_ci"))
charsets.add(Charset(224, "utf8mb4", "utf8mb4_unicode_ci"))
charsets.add(Charset(225, "utf8mb4", "utf8mb4_icelandic_ci"))
charsets.add(Charset(226, "utf8mb4", "utf8mb4_latvian_ci"))
charsets.add(Charset(227, "utf8mb4", "utf8mb4_romanian_ci"))
charsets.add(Charset(228, "utf8mb4", "utf8mb4_slovenian_ci"))
charsets.add(Charset(229, "utf8mb4", "utf8mb4_polish_ci"))
charsets.add(Charset(230, "utf8mb4", "utf8mb4_estonian_ci"))
charsets.add(Charset(231, "utf8mb4", "utf8mb4_spanish_ci"))
charsets.add(Charset(232, "utf8mb4", "utf8mb4_swedish_ci"))
charsets.add(Charset(233, "utf8mb4", "utf8mb4_turkish_ci"))
charsets.add(Charset(234, "utf8mb4", "utf8mb4_czech_ci"))
charsets.add(Charset(235, "utf8mb4", "utf8mb4_danish_ci"))
charsets.add(Charset(236, "utf8mb4", "utf8mb4_lithuanian_ci"))
charsets.add(Charset(237, "utf8mb4", "utf8mb4_slovak_ci"))
charsets.add(Charset(238, "utf8mb4", "utf8mb4_spanish2_ci"))
charsets.add(Charset(239, "utf8mb4", "utf8mb4_roman_ci"))
charsets.add(Charset(240, "utf8mb4", "utf8mb4_persian_ci"))
charsets.add(Charset(241, "utf8mb4", "utf8mb4_esperanto_ci"))
charsets.add(Charset(242, "utf8mb4", "utf8mb4_hungarian_ci"))
charsets.add(Charset(243, "utf8mb4", "utf8mb4_sinhala_ci"))
charsets.add(Charset(244, "utf8mb4", "utf8mb4_german2_ci"))
charsets.add(Charset(245, "utf8mb4", "utf8mb4_croatian_ci"))
charsets.add(Charset(246, "utf8mb4", "utf8mb4_unicode_520_ci"))
charsets.add(Charset(247, "utf8mb4", "utf8mb4_vietnamese_ci"))
charsets.add(Charset(248, "gb18030", "gb18030_chinese_ci", True))
charsets.add(Charset(249, "gb18030", "gb18030_bin"))
charsets.add(Charset(250, "gb18030", "gb18030_unicode_520_ci"))
charsets.add(Charset(255, "utf8mb4", "utf8mb4_0900_ai_ci"))
charsets.add(Charset(255, "utf8mb4", "utf8mb4_0900_as_cs"))


# Functions ==================================================================================
@cython.ccall
@cython.exceptval(check=False)
def charset_by_id(id: cython.int) -> Charset:
    """Get MySQL charset by id.

    :param id: `<int>` The id of the charset.
    :raise `KeyError`: If the charset is not found.
    :return: `<Charset>` The MySQL charset.
    """
    return charsets._get_by_id(id)


@cython.ccall
@cython.exceptval(check=False)
def charset_by_name(name: str) -> Charset:
    """Get MySQL charset by name.

    :param name: `<str>` The name of the charset.
    :raise `KeyError`: If the charset is not found.
    :return: `<Charset>` The MySQL charset.
    """
    return charsets._get_by_name(name)


@cython.ccall
@cython.exceptval(check=False)
def charset_by_collate(collate: str) -> Charset:
    """Get MySQL charset by collate.

    :param collate: `<str>` The collate of the charset.
    :raise `KeyError`: If the charset is not found.
    :return: `<Charset>` The MySQL charset.
    """
    return charsets._get_by_collate(collate)


@cython.ccall
@cython.exceptval(check=False)
def charset_by_name_and_collate(name: str, collate: str) -> Charset:
    """Get MySQL charset by name and collate.

    :param name: `<str>` The name of the charset.
    :param collate: `<str>` The collate of the charset.
    :raise `KeyError`: If the charset is not found.
    :return: `<Charset>` The MySQL charset.
    """
    return charsets._get_by_name_and_collate(name, collate)
