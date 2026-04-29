import logging
import structlog
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import List, Any
from structlog.stdlib import LoggerFactory

shared_configuration: List[Any] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", key="timestamp"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.CallsiteParameterAdder(
        {
            structlog.processors.CallsiteParameter.FILENAME,
            structlog.processors.CallsiteParameter.FUNC_NAME,
            structlog.processors.CallsiteParameter.LINENO,
            structlog.processors.CallsiteParameter.MODULE,
        }
    ),
    structlog.processors.UnicodeDecoder(),
]


class LogManager:
    def __init__(
        self,
        log_dir: str,
        app_name: str,
        log_level: str,
        backup_count: int = 5,
        console_output: bool = True,
        json_console: bool = True,
    ):
        self.log_dir = Path(log_dir)
        self.app_name = app_name
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.backup_count = backup_count
        self.console_output = console_output
        self.json_console = json_console

        self.log_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    def setup(self) -> structlog.stdlib.BoundLogger:
        handlers = []

        log_file_path = self.log_dir / "app.log"

        file_handler = TimedRotatingFileHandler(
            filename=log_file_path,
            when="midnight",
            interval=1,
            backupCount=self.backup_count,
            encoding="utf-8",
        )

        file_handler.setLevel(self.log_level)
        handlers.append(file_handler)

        if self.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            handlers.append(console_handler)

        logging.basicConfig(
            handlers=handlers,
            level=self.log_level,
            format="%(message)s",
        )

        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_configuration,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )

        for handler in handlers:
            handler.setFormatter(formatter)

        structlog.configure(
            processors=shared_configuration
            + [
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            logger_factory=LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        return structlog.get_logger()


def setup_logging(
    log_level: str,
    log_dir: str,
    app_name: str,
    backup_count: int = 5,
    silenced_modules: List[Any] = [],
) -> structlog.stdlib.BoundLogger:
    manager = LogManager(
        log_dir=log_dir,
        log_level=log_level,
        app_name=app_name,
        backup_count=backup_count,
    )

    logger = manager.setup()

    for module in silenced_modules:
        logging.getLogger(module).setLevel(logging.WARNING)

    return logger
