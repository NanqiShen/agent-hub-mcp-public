"""Desktop agent read-only adapters."""

from .antigravity import AntigravityAdapter
from .cline import ClineAdapter
from .codex import CodexAdapter
from .dsh import DshAdapter

__all__ = [
    "CodexAdapter",
    "DshAdapter",
    "AntigravityAdapter",
    "ClineAdapter",
]
