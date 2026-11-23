import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "worker"))
sys.path.insert(0, ROOT)


# Legacy tests exercise the burst/scalp logic; basket mode (default ON in production) is tested in test_basket.py,
# which passes basket_enabled=True explicitly.
from rapidburst.config import EngineConfig as _EC  # noqa: E402

_orig_init = _EC.__init__


def _init(self, *a, **kw):
    kw.setdefault("basket_enabled", False)
    kw.setdefault("scalp_enabled", False)
    _orig_init(self, *a, **kw)


_EC.__init__ = _init
