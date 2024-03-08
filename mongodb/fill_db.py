import time
import random
import functools
import datetime

import pymongo

def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function '{func.__name__}' executed in {(end_time - start_time):.2f} seconds")
        return result
    return wrapper

@timer
def create_test_data(collection):
    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2022, 12, 31)

    delta = end_date - start_date
    for i in range(delta.days + 1):
        current_date = start_date + datetime.timedelta(days=i)
        for device_item_id in range(1, 11):
            for _ in range(17_280):
                doc = {
                    "device_id": 1,
                    "device_item_id": device_item_id,
                    "created_at": datetime.datetime.now(),
                    "undated_at": datetime.datetime.now(),
                    "timestamp": current_date + datetime.timedelta(seconds=random.randint(0, 86399)),
                    "value": random.random() * 100.0
                }
                collection.insert_one(doc)

@timer
def count_created_documents(collection):
    return collection.count_documents({})


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["test-mongodb"]
    collection = db["test_collection"]

    create_test_data(collection)

    print("Total documents:", count_created_documents(collection))
    client.close()
