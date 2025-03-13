import os
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING, Literal, Optional, cast

from loguru import logger as logger
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.rule import Rule

from vp.utils.env import get_env_var

if TYPE_CHECKING:
    from loguru import Logger

os.environ["LOGURU_LEVEL"] = os.environ.get(
    "LOGURU_LEVEL", os.environ.get("LOG_LEVEL", "INFO")
)
LOG_LEVEL = cast(
    Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], os.environ["LOGURU_LEVEL"]
)


def _format_string(record):
    t = record["elapsed"]
    elapsed = f"{t.seconds // 60:02}:{t.seconds % 60:02}.{t.microseconds // 1000:03}"
    return (
        f"<green>{elapsed}</green> | "
        f"<level>{record['level']: <8}</level> | "
        "<level>{message}</level>"
        "\n"
    )


def _format_debug_string(record):
    t = record["elapsed"]
    elapsed = f"{t.seconds // 60:02}:{t.seconds % 60:02}.{t.microseconds // 1000}"
    return (
        f"<green>{elapsed}</green> | "
        f"<level>{record['level']: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
        "\n"
    )


def set_global_log_level(
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = LOG_LEVEL,
):
    from functools import partial

    from tqdm import tqdm

    os.environ["LOGURU_LEVEL"] = level
    if os.environ.get("NO_TQDM_WRITE", "0") == "1":
        sink = sys.stdout
    # elif os.environ.get("LOGURU_FILE", None) is not None:
    #     sink = os.environ["LOGURU_FILE"]
    else:
        sink = partial(tqdm.write, end="")

    logger.remove()
    logger.add(
        sink,
        format=_format_string,
        filter=lambda r: r["level"].name != "DEBUG",
        level=level,
        colorize=True,
    )
    logger.add(
        sink,
        format=_format_debug_string,
        filter=lambda r: r["level"].name == "DEBUG",
        level=level,
        colorize=True,
    )


@contextmanager
def log_progress(
    _logger: Optional["Logger"] = None,
    *,
    in_progress: str,
    complete: str,
    total: int | None = None,
):
    _logger = _logger or logger
    with Progress(
        TextColumn("{task.description}: "),
        TimeElapsedColumn(),
        SpinnerColumn(style="white"),
    ) as progress:
        task = progress.add_task(in_progress, total=total)
        yield
        progress.remove_task(task)

    _logger.opt(colors=True).info(complete)


def divider(title: str, console: Console | None = None):
    (console or Console()).print(Rule(title))


slack_oauth_warned = False


def slack_message(message: str, runtime_env: str | None = None):
    global slack_oauth_warned

    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    try:
        token = get_env_var("SLACK_OAUTH_TOKEN", runtime_env)
    except Exception:
        if not slack_oauth_warned:
            logger.warning("Slack OAuth token not found, skipping Slack message")
            slack_oauth_warned = True
        return

    client = WebClient(token=token)

    try:
        client.chat_postMessage(channel="jobs", text=message, username="jobs")
    except SlackApiError as e:
        logger.error(f"Error posting message to Slack: {e}")


def pretty_print_list(items: list[str], color: int = 32):
    pretty_items = f"\033[1m\033[0m, \033[1m\033[{color}m".join(items)
    pretty_items = f"\033[1m\033[{color}m{pretty_items}\033[0m"
    return pretty_items
