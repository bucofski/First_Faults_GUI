"""Main analyzer orchestrating the interlock analysis workflow."""

from typing import Any

from business.core.formatters import ResultFormatter, DictionaryResultFormatter
from business.core.tree_builder import InterlockTreeBuilder
from data.model.models import InterlockNode
from data.repositories.repository import InterlockRepository

class InterlockAnalyzer:
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
        interlock_number: int,
        limit: int = 1,
        formatter: ResultFormatter | None = None
    ) -> list[InterlockNode]:
        """Perform complete interlock analysis."""
        if formatter is None:
            formatter = DictionaryResultFormatter()

        df = self.repository.get_interlock_chain(interlock_number, limit)

        if df.empty:
            print(f"⚠️  No data found for interlock {interlock_number}")
            return [] if isinstance(formatter, DictionaryResultFormatter) else ""

        trees = self.tree_builder.build_from_dataframe(df)
        return trees