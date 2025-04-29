import mysql.connector
import time
import psutil
import csv
from datetime import datetime
import os

# Configurations
DB_NAME = "energytest"
USER = "root"
PASSWORD = "your_password"
HOST = "localhost"
CSV_LOG = "mysql_results.csv"

# Connect to MySQL
conn = mysql.connector.connect(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DB_NAME
)
cursor = conn.cursor()

# Create test table
def setup_database():
    cursor.execute("DROP TABLE IF EXISTS reviews")
    cursor.execute("""
        CREATE TABLE reviews (
            reviewerID VARCHAR(255),
            asin VARCHAR(255),
            reviewText TEXT,
            overall INT,
            summary VARCHAR(255),
            unixReviewTime BIGINT
        )
    """)
    conn.commit()

# Load sample data
def load_data():
    sample_data = [
        ("A2SUAM1J3GNN3B", "0000013714", "Great book for kids.", 5, "Good", 1382659200)
        for _ in range(10000)
    ]
    cursor.executemany("""
        INSERT INTO reviews (reviewerID, asin, reviewText, overall, summary, unixReviewTime)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, sample_data)
    conn.commit()

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
    cursor.execute("SELECT COUNT(*) FROM reviews")
    cursor.fetchone()

def write_intensive():
    for _ in range(1000):
        cursor.execute("""
            INSERT INTO reviews (reviewerID, asin, reviewText, overall, summary, unixReviewTime)
            VALUES ('TEST', 'TEST', 'Write test.', 4, 'Test', 1234567890)
        """)
    conn.commit()

def indexing():
    cursor.execute("CREATE INDEX idx_overall ON reviews(overall)")
    conn.commit()

def aggregation():
    cursor.execute("SELECT overall, COUNT(*) FROM reviews GROUP BY overall")
    cursor.fetchall()

def mixed_operations():
    for _ in range(500):
        cursor.execute("INSERT INTO reviews (reviewerID, asin) VALUES ('mixed', 'mixed')")
    cursor.execute("SELECT * FROM reviews LIMIT 1")
    cursor.fetchone()   # ✅ Fetch the result to clear buffer
    cursor.execute("UPDATE reviews SET summary = 'updated' WHERE reviewerID = 'mixed'")
    cursor.execute("DELETE FROM reviews WHERE reviewerID = 'mixed'")
    conn.commit()

if __name__ == "__main__":
    setup_database()
    load_data()
    run_operation("Read-Intensive", read_intensive)
    run_operation("Write-Intensive", write_intensive)
    run_operation("Indexing", indexing)
    run_operation("Aggregation", aggregation)
    run_operation("Mixed", mixed_operations)
    print(f"Results logged to {CSV_LOG}")

    cursor.close()
    conn.close()
