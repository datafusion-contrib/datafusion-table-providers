from typing import Any, List, Optional
from . import _internal

class DuckDBTableFactory:

    def __init__(self, path: str, access_mode: str = "") -> None:
        # TODO: revise documentation
        """Create a DuckDB table factory. If creating an in-memory table factory,
        then specify path to be :memory: or none and don't specify access_mode.
        If creating a file-based table factory, then specify path and access_mode.

        Args:
            path: Memory or file location
            access_mode: Access mode configuration
        """
        # TODO: think about the interface, restrict invalid combination of input
        # arguments, for example, if path is memory, then access_mode should not be
        # specified.
        if path == ":memory:" or path == "":
            self._raw = _internal.duckdb.RawDuckDBTableFactory.new_memory()
        else:
            self._raw = _internal.duckdb.RawDuckDBTableFactory.new_file(path, access_mode)
    
    def tables(self) -> List[str]:
        return self._raw.tables()
    
    def get_table(self, table_reference: str) -> Any:
        return self._raw.get_table(table_reference)
