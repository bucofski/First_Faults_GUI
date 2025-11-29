from DB_Connection import engine
from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd
from typing import Optional, Protocol
from datetime import datetime
from dataclasses import dataclass, field
from abc import ABC, abstractmethod


# Interface Segregation Principle - Define protocols for different responsibilities
class DatabaseEngine(Protocol):
    """Protocol for database engine"""

    def connect(self): ...


class InterlockRepository(ABC):
    """Abstract base class for data access - Dependency Inversion Principle"""

    @abstractmethod
    def get_interlock_chain(self, interlock_number: int, limit: int) -> pd.DataFrame:
        """Fetch interlock chain data"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test database connection"""
        pass


@dataclass
class SQLAlchemyInterlockRepository(InterlockRepository):
    """
    Concrete implementation of InterlockRepository using SQLAlchemy.
    Single Responsibility: Handle database operations only
    """

    engine: object = field(default_factory=lambda: engine)

    def get_interlock_chain(self, interlock_number: int, limit: int = 1) -> pd.DataFrame:
        """
        Get the last N occurrences of an interlock with complete upstream/downstream chain trace.

        Args:
            interlock_number: The interlock number to search for (e.g., 121)
            limit: Number of most recent occurrences to retrieve (default: 1)

        Returns:
            pandas.DataFrame with the results
        """
        query_text = text("""
                          SELECT *
                          FROM dbo.fn_InterlockChainByDate(:interlock_number, :limit)
                          ORDER BY Date DESC, TIMESTAMP DESC, Level;
                          """)

        with Session(self.engine) as session:
            result = session.execute(
                query_text,
                {"interlock_number": interlock_number, "limit": limit}
            )
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df

    def test_connection(self) -> bool:
        """
        Test the SQL Server connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with Session(self.engine) as session:
                result = session.execute(text("SELECT @@VERSION AS Version, DB_NAME() AS CurrentDatabase"))
                row = result.fetchone()
                print("✓ Connection successful!")
                print(f"Database: {row.CurrentDatabase}")
                print(f"Version: {row.Version}")
                return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False


class ResultFormatter(ABC):
    """Abstract base class for formatting results - Open/Closed Principle"""

    @abstractmethod
    def format(self, df: pd.DataFrame, interlock_number: int) -> None:
        """Format and display results"""
        pass


@dataclass
class ConsoleResultFormatter(ResultFormatter):
    """
    Single Responsibility: Format and print results to console
    """

    def format(self, df: pd.DataFrame, interlock_number: int) -> None:
        """
        Pretty print the root cause analysis results.

        Args:
            df: DataFrame with analysis results
            interlock_number: The interlock number analyzed
        """
        if df.empty:
            print(f"No occurrences found for interlock {interlock_number}")
            return

        print(f"Found {len(df)} rows")
        print("\n" + "=" * 80)
        print(f"ROOT CAUSE ANALYSIS - Interlock {interlock_number}")
        print("=" * 80 + "\n")

        for date in sorted(df['Date'].unique(), reverse=True):
            date_data = df[df['Date'] == date]

            print(f"\n{'=' * 80}")
            print(f"DATE: {date}")
            print(f"{'=' * 80}")

            # Group by unique interlock occurrences
            for interlock_id in sorted(date_data['Interlock_Log_ID'].unique()):
                interlock_data = date_data[date_data['Interlock_Log_ID'] == interlock_id]

                print(f"\n--- Interlock Log ID: {interlock_id} ---")

                for level in sorted(interlock_data['Level'].unique(), reverse=True):
                    level_data = interlock_data[interlock_data['Level'] == level]
                    first_row = level_data.iloc[0]

                    indent = "  " * (1 + abs(level))
                    print(f"\n{indent}Level {level}: {first_row['Direction']} - Interlock {first_row['BSID']}")
                    print(f"{indent}PLC: {first_row['PLC']}")
                    print(f"{indent}Time: {first_row['TIMESTAMP']}")
                    print(f"{indent}Message: {first_row['Interlock_Message']}")
                    if first_row['Status']:
                        print(f"{indent}Status: {first_row['Status']}")

                    conditions = level_data[level_data['Condition_Message'].notna()]
                    if not conditions.empty:
                        print(f"\n{indent}Active Conditions:")
                        for _, cond in conditions.iterrows():
                            print(
                                f"{indent}  - Type {cond['TYPE']}, Bit {cond['BIT_INDEX']}: {cond['Condition_Message']}")


@dataclass
class DictionaryResultFormatter(ResultFormatter):
    """
    Single Responsibility: Format results as dictionary/JSON structure
    Open/Closed: Can add new formatters without modifying existing ones
    """

    output_dir: Optional[str] = None

    def format(self, df: pd.DataFrame, interlock_number: int) -> dict:
        """
        Return results as a formatted object.

        Args:
            df: DataFrame with results
            interlock_number: The interlock number

        Returns:
            dict: Results as a structured dictionary object
        """
        results_object = {
            "interlock_number": interlock_number,
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "total_records": len(df),
            "data": df.to_dict(orient='records')
        }

        return results_object


@dataclass
class InterlockAnalyzer:
    """
    Main analyzer class following SOLID principles:
    - Single Responsibility: Orchestrate analysis workflow
    - Open/Closed: Can extend with new formatters/repositories without modification
    - Liskov Substitution: Can substitute any InterlockRepository or ResultFormatter
    - Interface Segregation: Depends only on abstract interfaces
    - Dependency Inversion: Depends on abstractions, not concrete implementations
    """

    repository: InterlockRepository = field(default_factory=SQLAlchemyInterlockRepository)
    console_formatter: ResultFormatter = field(default_factory=ConsoleResultFormatter)
    dict_formatter: ResultFormatter = field(default_factory=DictionaryResultFormatter)

    def test_connection(self) -> bool:
        """Test the database connection"""
        return self.repository.test_connection()

    def analyze_interlock(self, interlock_number: int, limit: int = 1,
                          save_to_file: bool = True, output_dir: Optional[str] = None) -> pd.DataFrame:
        """
        Complete analysis workflow: fetch data, print, and optionally save.

        Args:
            interlock_number: The interlock number to analyze
            limit: Number of most recent occurrences to retrieve
            save_to_file: Whether to save results to files
            output_dir: Output directory for saved files

        Returns:
            pandas.DataFrame with analysis results
        """
        print(f"\n{'=' * 80}")
        print(f"Root Cause Analysis for Interlock {interlock_number}")
        print(f"{'=' * 80}")

        results = self.repository.get_interlock_chain(interlock_number, limit)

        if results.empty:
            print(f"⚠️  No data found for interlock {interlock_number}")
            return results

        # Use console formatter
        self.console_formatter.format(results, interlock_number)

        if save_to_file:
            # Use dictionary formatter
            if isinstance(self.dict_formatter, DictionaryResultFormatter):
                self.dict_formatter.output_dir = output_dir
            formatted_results = self.dict_formatter.format(results, interlock_number)

        return results


# ============================================================================
# Usage Example
# ============================================================================

if __name__ == "__main__":
    try:
        start = datetime.now()

        # Initialize analyzer with dependency injection (SOLID principles)
        analyzer = InterlockAnalyzer(
            repository=SQLAlchemyInterlockRepository(),
            console_formatter=ConsoleResultFormatter(),
            dict_formatter=DictionaryResultFormatter(output_dir='./output')
        )

        # Test connection
        print("Testing SQL Server connection...")
        if not analyzer.test_connection():
            exit(1)

        # Analyze specific interlock
        results = analyzer.analyze_interlock(
            interlock_number=11222,
            limit=5,
            save_to_file=True,
            output_dir='./output'
        )

        end = datetime.now()
        print(f"\n✓ Execution time: {end - start}")

    except FileNotFoundError as e:
        print(f"❌ Configuration Error: {e}")
        print("Please ensure Connection.yaml exists in the config directory")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()