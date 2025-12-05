import json
from datetime import datetime

from interlock_search import InterlockAnalyzer, SQLAlchemyInterlockRepository, DictionaryResultFormatter


def save_interlock_results_to_file(interlock_number: int, limit: int = 1, output_file: str = "interlock_results.txt"):
    """
    Analyze interlock and save hierarchical results to a text file.

    Args:
        interlock_number: The interlock number to analyze
        limit: Number of most recent occurrences to retrieve
        output_file: Path to the output text file
    """
    # Initialize analyzer
    analyzer = InterlockAnalyzer(
        repository=SQLAlchemyInterlockRepository()
    )

    # Test connection
    print("Testing SQL Server connection...")
    if not analyzer.test_connection():
        print("Connection failed!")
        return

    # Get dictionary results
    results = analyzer.analyze_interlock(
        interlock_number=interlock_number,
        limit=limit,
        formatter=DictionaryResultFormatter()
    )

    # Write to file
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(results, indent=2, default=str))

    print(f"✓ Results saved to {output_file}")


if __name__ == "__main__":
    start_time = datetime.now()
    INTERLOCK_NUMBER = 11222
    LIMIT = 5
    OUTPUT_FILE = "interlock_results.txt"

    save_interlock_results_to_file(INTERLOCK_NUMBER, LIMIT, OUTPUT_FILE)
    end_time = datetime.now()
    print(f"Execution time: {end_time - start_time}")