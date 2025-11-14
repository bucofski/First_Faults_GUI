import yaml
import pyodbc
import os


class DBConnection:
    def __init__(self):
        # Determine path to Connection.yaml (one level up, then config/)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "..", "config", "Connection.yaml")

        # Load the YAML file
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        # Build connection string
        conn_config = config["DBconnection"]
        self.connection_string = (
            f"DRIVER={{{conn_config['driver']}}};"
            f"SERVER={conn_config['server']},{conn_config['port']};"
            f"DATABASE={conn_config['database']};"
            f"UID={conn_config['username']};"
            f"PWD={conn_config['password']};"
            f"Encrypt={'yes' if conn_config['encrypt'] else 'no'};"
            f"TrustServerCertificate="
            f"{'yes' if conn_config['trust_server_certificate'] else 'no'};"
        )

        self.conn = None
        self.cursor = None

    def connect(self):
        """Open the database connection."""
        if self.conn is None:
            self.conn = pyodbc.connect(self.connection_string)
            self.cursor = self.conn.cursor()

    def close(self):
        """Close cursor and connection."""
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def execute(self, query, params=None):
        """Execute a query and return the cursor."""
        if self.conn is None:
            self.connect()
        if params is None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, params)
        return self.cursor

    def fetchone(self):
        return self.cursor.fetchone() if self.cursor is not None else None

    def fetchall(self):
        return self.cursor.fetchall() if self.cursor is not None else None

    def commit(self):
        if self.conn is not None:
            self.conn.commit()

    def __enter__(self):
        """Support with-statement usage."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Example usage inside this file (you can call similar code from other functions)
if __name__ == "__main__":
    query = """
            SELECT TOP 1
        i.[TIMESTAMP],
        i.[MESSAGE] AS BS_comment,
        i.[PLC],
        i.[NUMBER] AS BSID,
        c.[BIT_INDEX] AS VW_index,
        c.[MESSAGE] AS VW_comment,
        c.[UPSTREAM_INTERLOCK_REF],
        c.[TYPE],
        c.[MNEMONIC] AS VW_mnemonic,
        i.[ID] AS interlock_id
            FROM [TD2].[dbo].[FF_INTERLOCK_LOG] i
                LEFT JOIN [TD2].[dbo].[FF_CONDITION_LOG] c \
            ON i.ID = c.INTERLOCK_REF
            WHERE i.[NUMBER] = 2355
            ORDER BY i.TIMESTAMP DESC, i.ORDER_LOG DESC, c.BIT_INDEX ASC \
            """
    with DBConnection() as db:
        cursor = db.execute(query)
        result = cursor.fetchone()
        print(f"FAULT: {result}")