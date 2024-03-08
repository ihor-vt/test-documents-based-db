import time
import random
import functools
import datetime

import mysql.connector
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

@timer
def create_test_data_mongodb():
    client = pymongo.MongoClient("mongodb://localhost:27017/", compressors='zstd')
    db = client["test2"]
    collection = db["machine_log"]

    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2022, 12, 31)

    timestamp_range = []
    current_time = datetime.datetime(2022, 1, 1, 0, 0, 0)
    end_time = start_date + datetime.timedelta(days=1)
    while current_time < end_time:
        timestamp_range.append(f"T{current_time.strftime('%H:%M:%S.%fZ')}")
        current_time += datetime.timedelta(seconds=5)

    delta = end_date - start_date
    for i in range(delta.days + 1):
        for index in range(1, 11):
            current_date = start_date + datetime.timedelta(days=i, hours=index)
            current_date_str = current_date.strftime("%Y-%m-%d")
            for number in range(17_280):
                timestamp = current_date_str + timestamp_range[number]
                str_number = str(number)
                doc = {
                    "partij_id": str_number + " G " + current_date_str,
                    "partij_omschrijving": str_number + timestamp + 'T' + str_number,
                    "herkomst": current_date_str + str_number,
                    "maat": str_number + ' | ' + str_number,
                    "container": "C_" + str_number,
                    "inhaaldatum": current_date_str,
                    "locatie": str_number + '%&' + timestamp + str_number,
                    "timestamp": timestamp,
                    "value": number
                }
                collection.insert_one(doc)

    client.close()

def create_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="username",
        password="password",
        database="test_db"
    )
    cursor = mydb.cursor()

    sql = """CREATE TABLE IF NOT EXISTS machine_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                partij_id VARCHAR(255),
                partij_omschrijving TEXT,
                herkomst VARCHAR(255),
                maat VARCHAR(255),
                container VARCHAR(255),
                inhaaldatum DATETIME,
                locatie VARCHAR(255),
                timestamp DATETIME,
                value INT
            )"""

    cursor.execute(sql)

    mydb.commit()

    cursor.close()
    mydb.close()

def create_test_data_mysql():
    mydb = mysql.connector.connect(
        host="localhost",
        user="username",
        password="password",
        database="test_db"
    )
    cursor = mydb.cursor()

    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2022, 12, 31)

    timestamp_range = []
    current_time = datetime.datetime(2022, 1, 1, 0, 0, 0)
    end_time = start_date + datetime.timedelta(days=1)
    while current_time < end_time:
        timestamp_range.append(f"{current_time.strftime('%H:%M:%S')}")
        current_time += datetime.timedelta(seconds=5)

    delta = end_date - start_date
    for i in range(delta.days + 1):
        for index in range(1, 11):
            current_date = start_date + datetime.timedelta(days=i, hours=index)
            current_date_str = current_date.strftime("%Y-%m-%d")
            for number in range(17_280):
                timestamp = current_date_str + " " + timestamp_range[number]
                str_number = str(number)
                sql = "INSERT INTO machine_log (partij_id, partij_omschrijving, herkomst, maat, container, inhaaldatum, locatie, timestamp, value) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (str_number + " G " + current_date_str,
                    str_number + timestamp + 'T' + str_number,
                    current_date_str + str_number,
                    str_number + ' | ' + str_number,
                    "C_" + str_number,
                    current_date_str,
                    str_number + '%&' + timestamp + str_number,
                    timestamp,
                    number)
                cursor.execute(sql, val)

    mydb.commit()
    print(cursor.rowcount, "records inserted.")

    cursor.close()
    mydb.close()

if __name__ == "__main__":
    create_table()
    create_test_data_mysql()

    # create_test_data_mongodb()
