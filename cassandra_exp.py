from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
import psutil
import csv
from datetime import datetime
import os

# Configurations
KEYSPACE = "energytest"
TABLE = "reviews"
CSV_LOG = "cassandra_results.csv"

# Connect to Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

# Setup keyspace and table
def setup_database():
    session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
    session.execute(f"""
        CREATE KEYSPACE {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
    """)
    session.set_keyspace(KEYSPACE)
    session.execute(f"""
        CREATE TABLE {TABLE} (
            id UUID PRIMARY KEY,
            reviewerID TEXT,
            asin TEXT,
            reviewText TEXT,
            overall INT,
            summary TEXT,
            unixReviewTime BIGINT
        )
    """)

# Load sample data
def load_data():
    from uuid import uuid4
    insert_stmt = session.prepare(f"""
        INSERT INTO {TABLE} (id, reviewerID, asin, reviewText, overall, summary, unixReviewTime)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    for _ in range(10000):
        session.execute(insert_stmt, [uuid4(), "A2SUAM1J3GNN3B", "0000013714", "Great book for kids.", 5, "Good", 1382659200])

# Utility to log results
def log_results(operation, duration, cpu, memory):
    file_exists = os.path.isfile(CSV_LOG)
    with open(CSV_LOG, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["Timestamp", "Operation", "Duration(s)", "CPU(%)", "Memory(MB)"])
        writer.writerow([datetime.now(), operation, duration, cpu, memory])

# Measure and run operation
def run_operation(name, func):
    start = time.time()
    func()
    end = time.time()
    cpu = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory().used / (1024 * 1024)
    log_results(name, end - start, cpu, memory)

# Workload operations
def read_intensive():
    session.execute(f"SELECT COUNT(*) FROM {TABLE}")

def write_intensive():
    from uuid import uuid4
    insert_stmt = session.prepare(f"""
        INSERT INTO {TABLE} (id, reviewerID, asin, reviewText, overall, summary, unixReviewTime)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    for _ in range(1000):
        session.execute(insert_stmt, [uuid4(), "TEST", "TEST", "Write test.", 4, "Test", 1234567890])

def indexing():
    # Cassandra doesn't support CREATE INDEX on primary key
    session.execute(f"CREATE INDEX IF NOT EXISTS ON {TABLE}(overall)")

def aggregation():
    # Aggregations are limited in Cassandra; use COUNT per value as an example
    session.execute(f"SELECT overall FROM {TABLE} LIMIT 1000")

def mixed_operations():
    from uuid import uuid4
    temp_id = uuid4()
    session.execute(f"""
        INSERT INTO {TABLE} (id, reviewerID, asin, reviewText, overall, summary, unixReviewTime)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, [temp_id, "mixed", "mixed", "test", 3, "mixed", 1111111111])
    session.execute(f"SELECT * FROM {TABLE} WHERE id = %s", [temp_id])
    # Cassandra doesn't support traditional UPDATE/DELETE by default on non-primary fields

if __name__ == "__main__":
    setup_database()
    load_data()
    run_operation("Read-Intensive", read_intensive)
    run_operation("Write-Intensive", write_intensive)
    run_operation("Indexing", indexing)
    run_operation("Aggregation", aggregation)
    run_operation("Mixed", mixed_operations)
    print(f"Results logged to {CSV_LOG}")

    cluster.shutdown()
