"""
loaders/base_loader.py

Abstract base class that every loader in this pipeline inherits from.
Enforces a consistent interface: each loader exposes a single `run()` method
and receives its logger and SnowflakeSession at construction time so
the pipeline orchestrator can manage connection lifecycle centrally.
"""

from abc import ABC, abstractmethod

from utils.db_connector import SnowflakeSession
from utils.logger import get_logger


class BaseLoader(ABC):
    """
    Abstract base for all extract, dimension, and fact loaders.

    Sub-classes must implement `run(sf)`.  Everything else (logging,
    timing, entry-point wrapper) is handled here.

    Parameters
    ----------
    name : str
        Human-readable loader name used for log messages, e.g. "country_loader".
    """

    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(name)

    @abstractmethod
    def run(self, sf: SnowflakeSession) -> None:
        """
        Execute the loader's full logic using the supplied *sf* session.

        Parameters
        ----------
        sf : SnowflakeSession
            An already-open Snowflake session provided by the caller.
            The loader must NOT open or close the connection itself.
        """

    def execute(self) -> None:
        """
        Convenience entry-point: opens its own SnowflakeSession and calls
        `run()`.  Useful when running a single loader standalone,
        e.g. ``python -m loaders.extract_loader``.
        """
        self.logger.info(f"=== {self.name.upper()} | START ===")
        try:
            with SnowflakeSession(self.logger) as sf:
                self.run(sf)
            self.logger.info(f"=== {self.name.upper()} | COMPLETE ===")
        except Exception as exc:
            self.logger.error(f"=== {self.name.upper()} | FAILED: {exc} ===")
            raise
