from importlib.metadata import PackageNotFoundError, version

try:
    VERSION = version("echodataflow")
except PackageNotFoundError:  # pragma: no cover
    VERSION = "0+local"
__version__ = VERSION

__all__ = ["__version__"]
