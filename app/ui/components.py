from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional

from nicegui import ui


class SnapshotStore:
    """Client-side helper to keep the latest snapshot in sync."""

    def __init__(self, fetcher: Callable[[], Any], interval: float = 5.0) -> None:
        self._fetcher = fetcher
        self.interval = interval
        self.snapshot: dict[str, Any] | None = None
        self._timer: Optional[ui.timer] = None
        self._listeners: list[Callable[[dict[str, Any] | None], None]] = []

    def start(self) -> None:
        if self._timer is not None:
            return

        async def refresh() -> None:
            await self.refresh_now()

        self._timer = ui.timer(self.interval, refresh)
        asyncio.create_task(refresh())

    def stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def subscribe(self, callback: Callable[[dict[str, Any] | None], None]) -> None:
        self._listeners.append(callback)

    async def refresh_now(self) -> None:
        try:
            self.snapshot = await self._fetcher()
            for listener in self._listeners:
                listener(self.snapshot)
        except Exception as exc:  # pragma: no cover - UI resilience
            ui.notify(f"Snapshot refresh failed: {exc}", color="negative")


def badge_stat(label: str, value: Any, color: str = "primary") -> ui.element:
    with ui.card().classes("items-center justify-center p-4") as card:
        ui.label(label).classes("text-xs text-gray-500")
        value_label = ui.label(value).classes(f"text-xl font-semibold text-{color}")
    card.value_label = value_label  # type: ignore[attr-defined]
    return card


__all__ = ["SnapshotStore", "badge_stat"]
