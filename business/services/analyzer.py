"""Main analyzer orchestrating the interlock analysis workflow."""
from datetime import datetime

from business.core.formatters import ResultFormatter, DictionaryResultFormatter
from business.core.tree_builder import InterlockTreeBuilder
from data.model.models import InterlockNode
from data.repositories.repository import InterlockRepository

class InterlockService:
    """Main analyzer orchestrating the interlock analysis workflow."""

    def __init__(
        self,
        repository: InterlockRepository | None = None,
        tree_builder: InterlockTreeBuilder | None = None
    ):
        self.repository = repository or InterlockRepository()
        self.tree_builder = tree_builder or InterlockTreeBuilder()

    def test_connection(self) -> bool:
        """Test database connection."""
        return self.repository.test_connection()

    def analyze_interlock(
            self,
            target_bsid: int | None = None,
            top_n: int | None = None,
            filter_timestamp_start: datetime | None = None,
            filter_timestamp_end: datetime | None = None,
            filter_condition_message: str | None = None,
            filter_plc: str | None = None,
            formatter: ResultFormatter | None = None
    ) -> list[InterlockNode]:
        """Perform complete interlock analysis."""
        if formatter is None:
            formatter = DictionaryResultFormatter()

        df = self.repository.get_interlock_chain(
            target_bsid=target_bsid,
            top_n=top_n,
            filter_timestamp_start=filter_timestamp_start,
            filter_timestamp_end=filter_timestamp_end,
            filter_condition_message=filter_condition_message,
            filter_plc=filter_plc
        )

        if df.empty:
            print(f"⚠️  No data found for interlock {target_bsid}")
            return []

        trees = self.tree_builder.build_from_dataframe(df)

        return trees