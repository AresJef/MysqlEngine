import os, numpy as np, platform
from setuptools import setup, Extension
from Cython.Build import cythonize

# Package name
__package__ = "mysqlengine"


# Create Extension
def extension(filename: str, include_np: bool, *extra_compile_args: str) -> Extension:
    # Extra arguments
    extra_args = list(extra_compile_args) if extra_compile_args else None
    # Name
    name: str = "%s.%s" % (__package__, filename.split(".")[0])
    source: str = os.path.join("src", __package__, filename)
    # Create extension
    if include_np:
        return Extension(
            name,
            sources=[source],
            extra_compile_args=extra_args,
            include_dirs=[np.get_include()],
            define_macros=[("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")],
        )
    else:
        return Extension(name, sources=[source], extra_compile_args=extra_args)


# Build Extensions
# fmt: off
if platform.system() == "Windows":
    extensions = [
        extension("charset.py", False),
        extension("column.py", False),
        extension("connection.py", False),
        extension("constant.py", False),
        extension("database.py", True),
        extension("dtype.py", True),
        extension("engine.py", False),
        extension("errors.py", False),
        extension("index.py", False),
        extension("protocol.py", False),
        extension("query.py", True),
        extension("regex.py", False),
        extension("settings.py", False),
        extension("transcode.py", True),
        extension("utils.py", True),
    ]
else:
    extensions = [
        extension("charset.py", False, "-Wno-unreachable-code"),
        extension("column.py", False, "-Wno-unreachable-code"),
        extension("connection.py", False, "-Wno-unreachable-code", "-Wno-incompatible-pointer-types"),
        extension("database.py", True, "-Wno-unreachable-code", "-Wno-incompatible-pointer-types"),
        extension("constant.py", False, "-Wno-unreachable-code"),
        extension("dtype.py", True, "-Wno-unreachable-code"),
        extension("engine.py", False, "-Wno-unreachable-code"),
        extension("errors.py", False, "-Wno-unreachable-code"),
        extension("index.py", False, "-Wno-unreachable-code"),
        extension("protocol.py", False, "-Wno-unreachable-code"),
        extension("query.py", True, "-Wno-unreachable-code"),
        extension("regex.py", False, "-Wno-unreachable-code"),
        extension("settings.py", False, "-Wno-unreachable-code"),
        extension("transcode.py", True, "-Wno-unreachable-code"),
        extension("utils.py", True, "-Wno-unreachable-code"),
    ]
# fmt: on

# Build
setup(
    ext_modules=cythonize(
        extensions,
        compiler_directives={"language_level": "3"},
        annotate=True,
    ),
)
