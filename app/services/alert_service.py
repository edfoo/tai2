"""Alert service — send notifications through configured channels.

Currently supports Telegram. Additional channels (Slack, Discord, email, etc.)
can be added here without changing any callers; they all use ``send_alert()``.

Configuration (.env):
    TELEGRAM_BOT_TOKEN   — bot token from @BotFather
    TELEGRAM_CHAT_ID     — target chat/channel ID (can be negative for groups)

If credentials are absent the service logs a debug message and returns silently,
consistent with how Postgres/Redis degrade gracefully when unconfigured.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


async def send_alert(text: str, *, parse_mode: str = "HTML") -> None:
    """Send ``text`` through all configured alert channels.

    Args:
        text:        The message body. Supports HTML tags when using the default
                     ``parse_mode="HTML"`` (e.g. ``<b>bold</b>``, ``<code>…</code>``).
        parse_mode:  Telegram parse mode — ``"HTML"`` or ``"Markdown"``.

    Returns silently on any error so callers never need try/except.
    """
    await _send_telegram(text, parse_mode=parse_mode)


# ── Telegram ──────────────────────────────────────────────────────────────────

async def _send_telegram(text: str, *, parse_mode: str = "HTML") -> None:
    settings = get_settings()
    token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id

    if not token or not chat_id:
        logger.debug("Alert service: Telegram not configured (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID missing)")
        return

    url = _TELEGRAM_API.format(token=token)
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if not resp.is_success:
                logger.warning(
                    "Alert service: Telegram delivery failed (%s): %s",
                    resp.status_code,
                    resp.text[:200],
                )
            else:
                logger.debug("Alert service: Telegram message sent (%d chars)", len(text))
    except Exception as exc:
        logger.warning("Alert service: Telegram error — %s", exc)
