
from typing import Any, List, Optional
from . import _internal

class SqliteTableFactory:

    def __init__(self, path: str, mode: str, busy_timeout_s: float, attach_databases: Optional[List[str]] = None) -> None:
        # TODO: fix documentation
        """Create a new :py:class:`SessionConfig` with the given configuration options.

        Args:
            config_options: Configuration options.
        """
        self._raw = _internal.sqlite.RawSqliteTableFactory(path, mode, busy_timeout_s, attach_databases)
    
    def tables(self) -> List[str]:
        return self._raw.tables()
    
    def get_table(self, table_reference: str) -> Any:
        return self._raw.get_table(table_reference)
