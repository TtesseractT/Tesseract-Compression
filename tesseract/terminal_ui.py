"""Terminal rendering helpers for the Tesseract CLI."""

from __future__ import annotations

import logging
import os
import sys
from typing import Iterable, Optional, Sequence, Tuple

logger = logging.getLogger("tesseract")

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
    from rich.table import Table

    HAS_RICH = True
except ImportError:
    Console = None
    Panel = None
    Progress = None
    SpinnerColumn = None
    TextColumn = None
    BarColumn = None
    TimeElapsedColumn = None
    TimeRemainingColumn = None
    Table = None
    HAS_RICH = False

try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    tqdm = None
    HAS_TQDM = False

SummaryRows = Sequence[Tuple[str, object]]


def _interactive_output() -> bool:
    """Return True when live terminal rendering is appropriate."""
    if os.environ.get("TESSERACT_PLAIN"):
        return False
    if os.environ.get("TERM", "").lower() == "dumb":
        return False
    return sys.stdout.isatty()


class ProgressTracker:
    """Bridge progress callbacks to rich, tqdm, or plain logging."""

    PHASE_LABELS = {
        "scanning": "Scanning files",
        "deduplicating": "Grouping by metadata",
        "partial_hashing": "Partial hashing (64KB)",
        "full_hashing": "Full BLAKE3 hashing",
        "hashing_unique": "Hashing unique files",
        "preflight": "Pre-flight verification",
        "staging": "Staging shards",
        "verifying_shards": "Verifying shards",
        "verifying_source": "Verifying source files",
        "assembling": "Assembling archive",
        "writing": "Writing archive",
        "verifying": "Verifying archive",
        "finalizing": "Finalizing",
        "recovery": "Recovery records",
        "reading_header": "Reading header",
        "reading_manifest": "Reading manifest",
        "extracting": "Extracting files",
        "restoring_duplicates": "Restoring duplicates",
        "verifying_extracted": "Verifying extracted files",
    }

    def __init__(self, console: Optional[Console] = None, use_tqdm: bool = True):
        self.console = console
        self.use_rich = HAS_RICH and console is not None and _interactive_output()
        self.use_tqdm = not self.use_rich and use_tqdm and HAS_TQDM and sys.stderr.isatty()
        self._bar = None
        self._progress = None
        self._progress_started = False
        self._task_id = None
        self._current_phase = ""
        self._current_label = ""
        self._current_total = 0
        self._current_completed = 0
        self._note = ""

        if self.use_rich:
            self._progress = Progress(
                SpinnerColumn(style="cyan"),
                TextColumn("[bold]{task.description}[/bold]", justify="left"),
                BarColumn(bar_width=None, complete_style="cyan", finished_style="green", pulse_style="cyan"),
                TextColumn("{task.fields[counts]}", style="dim", justify="right"),
                TextColumn("{task.fields[note]}", style="bright_black"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=self.console,
                transient=True,
                expand=True,
            )

    def __call__(self, event: str, value=None, total: int = 0):
        if event == "phase":
            self._set_phase(value, total)
        elif event == "step":
            self._advance(value if value else 1)
        elif event == "volume_written":
            self._set_note(f"wrote volume {int(value):03d}")
        elif event == "volume_read":
            self._set_note(f"read volume {int(value):03d}")

    def _set_phase(self, phase: str, total: int = 0):
        self._current_phase = phase
        self._current_label = self.PHASE_LABELS.get(phase, phase)
        self._current_total = total
        self._current_completed = 0
        self._note = "counting source tree" if total == -1 else ""

        if self.use_rich:
            self._ensure_task(reset=True)
            return

        if self._bar is not None:
            self._bar.close()
            self._bar = None

        if self.use_tqdm and total > 0:
            self._bar = tqdm(
                total=total,
                desc=self._current_label,
                unit="file",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            )
        elif self.use_tqdm and total == -1:
            self._bar = tqdm(
                desc=self._current_label,
                unit="file",
                bar_format="{desc}: {n_fmt} {unit} [{elapsed}]",
            )
        else:
            suffix = f" ({total} items)" if total > 0 else ""
            logger.info("%s...%s", self._current_label, suffix)

    def _advance(self, step: int):
        self._current_completed += step
        if self.use_rich:
            self._ensure_task(reset=False, advance=step)
            return
        if self._bar is not None:
            self._bar.update(step)

    def _set_note(self, note: str):
        self._note = note
        if self.use_rich:
            self._ensure_task(reset=False)

    def _counts_text(self) -> str:
        if self._current_total == -1:
            return f"{self._current_completed} files"
        if self._current_total > 0:
            return f"{self._current_completed}/{self._current_total} files"
        return "working"

    def _ensure_task(self, reset: bool, advance: int = 0):
        if not self.use_rich or self._progress is None:
            return

        if not self._progress_started:
            self._progress.start()
            self._progress_started = True

        rich_total = None if self._current_total <= 0 else self._current_total
        if self._task_id is None:
            self._task_id = self._progress.add_task(
                self._current_label,
                total=rich_total,
                counts=self._counts_text(),
                note=self._note,
            )
            return

        update_kwargs = {
            "description": self._current_label,
            "counts": self._counts_text(),
            "note": self._note,
        }

        if reset:
            update_kwargs["total"] = rich_total
            update_kwargs["completed"] = self._current_completed
        elif advance:
            update_kwargs["advance"] = advance
        else:
            update_kwargs["completed"] = self._current_completed

        self._progress.update(self._task_id, **update_kwargs)

    def close(self):
        if self._bar is not None:
            self._bar.close()
            self._bar = None
        if self._progress is not None and self._progress_started:
            self._progress.stop()
            self._progress_started = False
            self._task_id = None


class CommandUI:
    """High-level command rendering for polished CLI output."""

    def __init__(self, command: str, context: Optional[Iterable[Tuple[str, object]]] = None):
        self.command = command
        self.use_rich = HAS_RICH and _interactive_output()
        self.console = Console() if HAS_RICH else None
        self._print_header(context or [])

    def tracker(self) -> ProgressTracker:
        return ProgressTracker(console=self.console, use_tqdm=True)

    def print_summary(self, title: str, rows: SummaryRows, border_style: str = "green"):
        if self.use_rich and self.console is not None:
            table = Table.grid(padding=(0, 2))
            table.add_column(style="bold")
            table.add_column()
            for key, value in rows:
                table.add_row(str(key), str(value))
            self.console.print(Panel.fit(table, title=title, border_style=border_style))
            return

        print(f"\n{title}")
        for key, value in rows:
            print(f"  {key:<18} {value}")

    def print_message(self, message: str, border_style: str = "bright_black"):
        if self.use_rich and self.console is not None:
            self.console.print(Panel.fit(message, border_style=border_style))
            return
        print(message)

    def print_error(self, title: str, details: object):
        if self.use_rich and self.console is not None:
            self.console.print(Panel.fit(str(details), title=title, border_style="red"))
            return
        logger.error("%s: %s", title, details)

    def _print_header(self, context: Iterable[Tuple[str, object]]):
        if not self.use_rich or self.console is None:
            return

        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold cyan")
        table.add_column()
        table.add_row("tesseract", self.command)
        for key, value in context:
            table.add_row(f"[dim]{key}[/dim]", str(value))
        self.console.print(Panel.fit(table, border_style="bright_black"))