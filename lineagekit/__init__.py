"""
LineageKit — data lineage & impact analyzer for Python pipelines.
Public API re-exports live here to keep imports nice and short.
"""

from importlib.metadata import PackageNotFoundError, version

# Package version (works for editable installs and wheels)
try:
    __version__ = version("lineagekit")
except PackageNotFoundError:  # running from source without install
    __version__ = "0.0.0.dev0"

# Re-export the main things users need
from .dataset import dataset            # @dataset decorator
from .transform import transform        # @transform decorator
from .lineage_tracker import tracker    # global tracker instance

# Small helper so the CLI/UI can locate the Streamlit app file
def streamlit_app_path():
    from importlib.resources import files
    return files("lineagekit.ui") / "streamlit_app.py"

__all__ = [
    "dataset",
    "transform",
    "tracker",
    "__version__",
    "streamlit_app_path",
]
