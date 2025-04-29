
import pymongo
import time
import psutil
import csv
from datetime import datetime
import os
from bson.objectid import ObjectId  # ADD THIS
from copy import deepcopy


# Configurations
DB_NAME = "energy_test_db"
COLLECTION_NAME = "reviews"
CSV_LOG = "mongodb_results.csv"

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Load sample data (mock small sample for testing)
def load_data():
    sample_doc = {
        "reviewerID": "A2SUAM1J3GNN3B",
        "asin": "0000013714",
        "reviewText": "Great book for kids.",
        "overall": 5,
        "summary": "Good",
        "unixReviewTime": 1382659200
    }
    collection.delete_many({})
    
    docs = []
    for _ in range(10000):
        doc = deepcopy(sample_doc)
        doc["_id"] = ObjectId() 
        docs.append(doc)
    
    collection.insert_many(docs)

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
    collection.count_documents({})

def write_intensive():
    docs = []
    for _ in range(1000):
        doc = {
            "reviewerID": "TEST",
            "asin": "TEST",
            "reviewText": "Write test.",
            "overall": 4,
            "_id": ObjectId()  # Force fresh _id
        }
        docs.append(doc)
    
    collection.insert_many(docs)


def indexing():
    collection.create_index("overall")

def aggregation():
    pipeline = [{"$group": {"_id": "$overall", "count": {"$sum": 1}}}]
    list(collection.aggregate(pipeline))

def mixed_operations():
    for _ in range(500):
        collection.insert_one({"mixed": True})
    collection.find_one({})
    collection.update_many({"mixed": True}, {"$set": {"updated": True}})
    collection.delete_many({"mixed": True})

if __name__ == "__main__":
    load_data()
    run_operation("Read-Intensive", read_intensive)
    run_operation("Write-Intensive", write_intensive)
    run_operation("Indexing", indexing)
    run_operation("Aggregation", aggregation)
    run_operation("Mixed", mixed_operations)
    print(f"Results logged to {CSV_LOG}")
