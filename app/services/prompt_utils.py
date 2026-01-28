from __future__ import annotations

from typing import Optional


_REPLACEMENTS = {
    "\u2014": "-",  # em dash
    "\u2013": "-",  # en dash
}


def sanitize_prompt_text(text: Optional[str]) -> Optional[str]:
    """Replace problematic Unicode punctuation in prompt text with ASCII equivalents."""
    if text is None:
        return None
    return text.translate(str.maketrans(_REPLACEMENTS))
