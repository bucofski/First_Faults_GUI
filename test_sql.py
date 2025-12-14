"""
Test script to diagnose where the interlock analysis is failing.
This script tests each layer: SQL Server → SQLAlchemy → Repository → Analyzer
"""

import pyodbc
import sqlalchemy as sa
from sqlalchemy import text
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.repositories.DB_Connection import get_engine, load_db_config, _build_odbc_connect_string

# ============================================================================
# TEST 1: Direct pyodbc connection
# ============================================================================
def test_pyodbc_connection():
    """Test direct connection to SQL Server using pyodbc."""
    print("\n" + "="*80)
    print("TEST 1: Direct pyodbc Connection")
    print("="*80)

    try:
        cfg = load_db_config()
        conn_str = _build_odbc_connect_string(cfg)

        print(f"   Server: {cfg['server']}")
        print(f"   Database: {cfg['database']}")
        print(f"   Driver: {cfg['driver']}")

        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()

        # Test simple query
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        print(f"✅ Connection successful!")
        print(f"   SQL Server Version: {version[0][:50]}...")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ pyodbc connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# TEST 2: Direct SQL query execution
# ============================================================================
def test_direct_sql_query():
    """Test executing the function directly via pyodbc."""
    print("\n" + "="*80)
    print("TEST 2: Direct SQL Query Execution")
    print("="*80)

    try:
        cfg = load_db_config()
        conn_str = _build_odbc_connect_string(cfg)

        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()

        # Test WITHOUT filter first (should be fast)
        print("\n📊 Testing query WITHOUT filter (baseline test)...")
        query_no_filter = """
        SELECT TOP 5 
            AnchorReference,
            Date,
            Level,
            Direction,
            Interlock_Log_ID,
            TIMESTAMP,
            PLC,
            BSID,
            Condition_Message
        FROM dbo.fn_InterlockChain(NULL, 5, NULL, NULL, NULL, NULL, NULL)
        ORDER BY TIMESTAMP DESC, Date DESC, Level
        """

        start_time = datetime.now()
        cursor.execute(query_no_filter)
        rows = cursor.fetchall()
        elapsed = (datetime.now() - start_time).total_seconds()

        print(f"✅ Query WITHOUT filter executed in {elapsed:.2f} seconds")
        print(f"   Rows returned: {len(rows)}")

        # Now test WITH 'test' filter
        print("\n📊 Testing query WITH filter_condition_message='test'...")
        query_with_filter = """
        SELECT TOP 5 
            AnchorReference,
            Date,
            Level,
            Direction,
            Interlock_Log_ID,
            TIMESTAMP,
            PLC,
            BSID,
            Condition_Message
        FROM dbo.fn_InterlockChain(NULL, 5, NULL, NULL, NULL, 'test', NULL)
        ORDER BY TIMESTAMP DESC, Date DESC, Level
        """

        print(f"   Query timeout set to: 30 seconds")
        print(f"   If this hangs, press Ctrl+C after 30 seconds...")

        start_time = datetime.now()
        cursor.execute(query_with_filter)
        rows = cursor.fetchall()
        elapsed = (datetime.now() - start_time).total_seconds()

        print(f"✅ Query WITH filter executed in {elapsed:.2f} seconds")
        print(f"   Rows returned: {len(rows)}")

        if rows:
            print("\n   Sample row:")
            cols = [col[0] for col in cursor.description]
            for i, col in enumerate(cols[:5]):  # First 5 columns
                print(f"      {col}: {rows[0][i]}")

        cursor.close()
        conn.close()
        return True

    except pyodbc.OperationalError as e:
        if "timeout" in str(e).lower():
            print(f"❌ Query TIMEOUT - This is the infinite loop issue!")
            print(f"   The SQL function itself is hanging.")
            print(f"   Error: {e}")
        else:
            print(f"❌ SQL execution failed: {e}")
        return False
    except KeyboardInterrupt:
        print(f"\n❌ Query interrupted by user (Ctrl+C)")
        print(f"   This indicates the query was hanging - infinite loop confirmed!")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# TEST 3: SQLAlchemy engine test
# ============================================================================
def test_sqlalchemy_engine():
    """Test SQLAlchemy engine from your database module."""
    print("\n" + "="*80)
    print("TEST 3: SQLAlchemy Engine")
    print("="*80)

    try:
        engine = get_engine()

        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION"))
            version = result.fetchone()
            print(f"✅ SQLAlchemy engine created successfully!")
            print(f"   Database: {engine.url.database or 'from config'}")
            print(f"   Driver: {engine.url.drivername}")
            print(f"   Pool size: 5")
            print(f"   Pool recycle: 3600s")

        return engine

    except Exception as e:
        print(f"❌ SQLAlchemy engine creation failed: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# TEST 4: SQLAlchemy query with timeout
# ============================================================================
def test_sqlalchemy_query(engine):
    """Test query execution through SQLAlchemy with timeout."""
    print("\n" + "="*80)
    print("TEST 4: SQLAlchemy Query with Timeout")
    print("="*80)

    if engine is None:
        print("⚠️  Skipping - no engine available")
        return False

    try:
        query = text("""
        SELECT TOP 5 
            AnchorReference,
            Date,
            Level,
            Direction,
            Interlock_Log_ID,
            TIMESTAMP,
            PLC,
            BSID,
            Condition_Message
        FROM dbo.fn_InterlockChain(
            :target_bsid, 
            :top_n, 
            :filter_date, 
            :filter_timestamp_start, 
            :filter_timestamp_end, 
            :filter_condition_message, 
            :filter_plc
        )
        ORDER BY TIMESTAMP DESC, Date DESC, Level
        """)

        params = {
            'target_bsid': None,
            'top_n': 5,
            'filter_date': None,
            'filter_timestamp_start': None,
            'filter_timestamp_end': None,
            'filter_condition_message': 'test',
            'filter_plc': None
        }

        print(f"\n📊 Executing query with params:")
        print(f"   top_n: {params['top_n']}")
        print(f"   filter_condition_message: '{params['filter_condition_message']}'")
        print(f"   Timeout: 30 seconds")

        start_time = datetime.now()

        # Execute with timeout
        with engine.connect().execution_options(timeout=30) as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ Query executed successfully in {elapsed:.2f} seconds")
        print(f"   Rows returned: {len(rows)}")

        if rows:
            print("\n   Sample row:")
            keys = list(result.keys())
            for i, col in enumerate(keys[:5]):
                print(f"      {col}: {rows[0][i]}")

        return True

    except sa.exc.DBAPIError as e:
        if "timeout" in str(e).lower():
            print(f"❌ Query TIMEOUT via SQLAlchemy!")
            print(f"   This confirms the issue is in the SQL function itself")
        else:
            print(f"❌ SQLAlchemy query failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# TEST 5: pandas read_sql with timeout
# ============================================================================
def test_pandas_read_sql(engine):
    """Test reading data into pandas DataFrame."""
    print("\n" + "="*80)
    print("TEST 5: Pandas read_sql")
    print("="*80)

    if engine is None:
        print("⚠️  Skipping - no engine available")
        return None

    try:
        query = """
        SELECT TOP 5 
            AnchorReference,
            Date,
            Level,
            Direction,
            Interlock_Log_ID,
            TIMESTAMP,
            PLC,
            BSID,
            Condition_Message
        FROM dbo.fn_InterlockChain(NULL, 5, NULL, NULL, NULL, 'test', NULL)
        ORDER BY TIMESTAMP DESC, Date DESC, Level
        """

        print(f"\n📊 Reading data into DataFrame with filter='test'...")
        start_time = datetime.now()

        df = pd.read_sql(query, engine)

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ DataFrame created successfully in {elapsed:.2f} seconds")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")

        if not df.empty:
            print("\n   First row:")
            print(df.head(1).to_string())

        return df

    except Exception as e:
        print(f"❌ Pandas read_sql failed: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# TEST 6: Repository layer test
# ============================================================================
def test_repository_layer():
    """Test the InterlockRepository directly."""
    print("\n" + "="*80)
    print("TEST 6: Repository Layer")
    print("="*80)

    try:
        from data.repositories.repository import InterlockRepository

        repo = InterlockRepository()

        # Test connection
        print("\n📊 Testing repository connection...")
        if repo.test_connection():
            print("✅ Repository connection successful")
        else:
            print("❌ Repository connection failed")
            return None

        # Test query WITHOUT filter first
        print("\n📊 Testing repository query WITHOUT filter...")
        start_time = datetime.now()
        df_no_filter = repo.get_interlock_chain(target_bsid=None, top_n=5)
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ Query WITHOUT filter completed in {elapsed:.2f} seconds")
        print(f"   DataFrame shape: {df_no_filter.shape}")

        # Test query WITH filter
        print("\n📊 Testing repository query WITH filter='test'...")
        start_time = datetime.now()
        df_with_filter = repo.get_interlock_chain(
            target_bsid=None,
            top_n=5,
            filter_condition_message='test'
        )
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ Query WITH filter completed in {elapsed:.2f} seconds")
        print(f"   DataFrame shape: {df_with_filter.shape}")
        print(f"   Empty: {df_with_filter.empty}")

        return df_with_filter

    except ImportError as e:
        print(f"⚠️  Could not import repository: {e}")
        print("   Make sure you're running from the correct directory")
        return None
    except Exception as e:
        print(f"❌ Repository test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# TEST 7: Full analyzer test
# ============================================================================
def test_full_analyzer():
    """Test the complete InterlockAnalyzer."""
    print("\n" + "="*80)
    print("TEST 7: Full Analyzer")
    print("="*80)

    try:
        from business.core.analyzer import InterlockAnalyzer

        analyzer = InterlockAnalyzer()

        print("\n📊 Running full analysis with filter_condition_message='test'...")
        start_time = datetime.now()

        trees = analyzer.analyze_interlock(
            target_bsid=None,
            top_n=5,
            filter_condition_message='test'
        )

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ Analysis completed in {elapsed:.2f} seconds")
        print(f"   Trees created: {len(trees)}")

        if trees:
            print("\n   First tree:")
            print(f"      Root BSID: {trees[0].bsid}")
            print(f"      Level: {trees[0].level}")
            print(f"      Direction: {trees[0].direction}")

        return trees

    except ImportError as e:
        print(f"⚠️  Could not import analyzer: {e}")
        return None
    except Exception as e:
        print(f"❌ Analyzer test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# MAIN TEST RUNNER
# ============================================================================
def main():
    """Run all tests in sequence."""
    print("\n" + "="*80)
    print("INTERLOCK ANALYSIS DIAGNOSTIC TEST SUITE")
    print("="*80)
    print("\nThis script will test each layer of the interlock analysis system")
    print("to identify where the infinite loop/timeout is occurring.\n")

    print("Configuration:")
    try:
        cfg = load_db_config()
        print(f"  Server: {cfg['server']}")
        print(f"  Database: {cfg['database']}")
        print(f"  Driver: {cfg['driver']}")
    except Exception as e:
        print(f"  ⚠️  Could not load config: {e}")

    print("\n" + "="*80)
    input("Press Enter to start tests...")

    results = {}

    # Run tests
    results['pyodbc'] = test_pyodbc_connection()

    if results['pyodbc']:
        results['direct_sql'] = test_direct_sql_query()
    else:
        print("\n⚠️  Skipping remaining tests - no database connection")
        return

    engine = test_sqlalchemy_engine()
    results['sqlalchemy_engine'] = engine is not None

    if engine:
        results['sqlalchemy_query'] = test_sqlalchemy_query(engine)
        df = test_pandas_read_sql(engine)
        results['pandas'] = df is not None
    else:
        results['sqlalchemy_query'] = False
        results['pandas'] = False

    results['repository'] = test_repository_layer() is not None
    results['analyzer'] = test_full_analyzer() is not None

    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name}")

    print("\n" + "="*80)
    print("DIAGNOSIS")
    print("="*80)

    if not results.get('direct_sql', False):
        print("🔍 The issue is in the SQL FUNCTION itself!")
        print("   The function times out even with direct SQL execution.")
        print("   ")
        print("   SOLUTION:")
        print("   1. Deploy the updated SQL function (without DISTINCT)")
        print("   2. Run: DROP FUNCTION dbo.fn_InterlockChain")
        print("   3. Run the new CREATE FUNCTION script")
        print("   4. Verify by running this test again")
    elif not results.get('sqlalchemy_query', False):
        print("🔍 The issue is in the SQLAlchemy layer!")
        print("   Direct SQL works but SQLAlchemy times out.")
        print("   Solution: Check parameter passing and timeout settings")
    elif not results.get('repository', False):
        print("🔍 The issue is in the Repository layer!")
        print("   SQLAlchemy works but repository fails.")
        print("   Solution: Check repository query construction")
    elif not results.get('analyzer', False):
        print("🔍 The issue is in the Analyzer layer!")
        print("   Repository works but analyzer fails.")
        print("   Solution: Check tree building logic")
    else:
        print("✅ All tests passed! The system is working correctly.")
        print("   The SQL function fix has resolved the infinite loop issue.")


if __name__ == "__main__":
    main()