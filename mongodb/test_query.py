import calendar
import time
import datetime
import functools
import json

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
def count_documents(collection):
    return collection.count_documents({})

@timer
def find_by_device_item_id(collection, device_item_id):
    return list(collection.find({"device_item_id": device_item_id}))


@timer
def find_by_device_item_id_aggregate(collection, device_item_id):
    pipeline = [
        {"$match": {"device_item_id": device_item_id}},
    ]
    result = list(collection.aggregate(pipeline))
    return result


@timer
def find_by_time_range(collection, start_time, end_time):
    return list(collection.find({"timestamp": {"$gte": start_time, "$lte": end_time}}))

@timer
def calculate_monthly_sum(collection):
    start_date = datetime.datetime(2022, 1, 1)

    result = {}

    for month in range(1, 13):
        start_of_month = datetime.datetime(start_date.year, month, 1)
        _, days_in_month = calendar.monthrange(start_of_month.year, start_of_month.month)
        end_of_month = start_of_month.replace(day=days_in_month)

        query = {
            "device_id": 1,
            "timestamp": {"$gte": start_of_month, "$lte": end_of_month}
        }

        monthly_sum = {1: 0, 2: 0, 3: 0}

        documents = collection.find(query)

        for doc in documents:
            monthly_sum[doc["device_item_id"]] += doc["value"]

        result[start_of_month.strftime("%Y-%m")] = monthly_sum

    return result


@timer
def calculate_monthly_avarage(collection):
    start_date = datetime.datetime(2022, 1, 1)

    result = {}

    for month in range(1, 13):
        start_of_month = datetime.datetime(start_date.year, month, 1)
        _, days_in_month = calendar.monthrange(start_of_month.year, start_of_month.month)
        end_of_month = start_of_month.replace(day=days_in_month)

        query = {
            "device_id": 1,
            "timestamp": {"$gte": start_of_month, "$lte": end_of_month}
        }

        monthly_sum = {1: 0, 2: 0, 3: 0}
        monthly_count = {1: 0, 2: 0, 3: 0}

        documents = collection.find(query)

        for doc in documents:
            monthly_sum[doc["device_item_id"]] += doc["value"]
            monthly_count[doc["device_item_id"]] += 1

        monthly_average = {item_id: monthly_sum[item_id] / monthly_count[item_id] if monthly_count[item_id] != 0 else 0
                        for item_id in monthly_sum}

        result[start_of_month.strftime("%Y-%m")] = monthly_average

    return result


@timer
def calculate_daily_average(collection, device_item_ids):
    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2023, 12, 31)

    result = {}

    current_date = start_date

    while current_date <= end_date:
        start_of_day = datetime.datetime(current_date.year, current_date.month, current_date.day)
        end_of_day = start_of_day + datetime.timedelta(days=1)

        query = {
            "device_id": 1,
            "timestamp": {"$gte": start_of_day, "$lt": end_of_day},
            "device_item_id": {"$in": device_item_ids}
        }

        documents = collection.find(query)

        daily_data = {}

        for doc in documents:
            date_key = doc["timestamp"].strftime("%Y-%m-%d")
            if date_key not in daily_data:
                daily_data[date_key] = {"sum": {item_id: 0 for item_id in device_item_ids},
                                        "count": {item_id: 0 for item_id in device_item_ids}}
            daily_data[date_key]["sum"][doc["device_item_id"]] += doc["value"]
            daily_data[date_key]["count"][doc["device_item_id"]] += 1

        daily_average = {}

        for date_key, data in daily_data.items():
            daily_average[date_key] = {item_id: data["sum"][item_id] / data["count"][item_id] if data["count"][item_id] != 0 else 0
                                        for item_id in device_item_ids}

        result.update(daily_average)

        current_date += datetime.timedelta(days=1)

    return result


@timer
def aggregate_calculate_daily_average(collection, device_item_ids):
    pipeline = [
        {
            "$match": {
                "device_id": 1,
                "device_item_id": {"$in": device_item_ids},
                "timestamp": {
                    "$gte": datetime.datetime(2022, 1, 1),
                    "$lte": datetime.datetime(2023, 12, 31)
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$timestamp"},
                    "month": {"$month": "$timestamp"},
                    "day": {"$dayOfMonth": "$timestamp"},
                    "device_item_id": "$device_item_id"
                },
                "total_value": {"$sum": "$value"},
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": {
                    "$dateFromParts": {
                        "year": "$_id.year",
                        "month": "$_id.month",
                        "day": "$_id.day"
                    }
                },
                "device_item_id": "$_id.device_item_id",
                "average_value": {"$divide": ["$total_value", "$count"]}
            }
        },
        {
            "$sort": {
                "date": 1,
                "device_item_id": 1
            }
        }
    ]

    result = {}
    cursor = collection.aggregate(pipeline)

    for doc in cursor:
        date_key = doc["date"].strftime("%Y-%m-%d")
        if date_key not in result:
            result[date_key] = {}
        result[date_key][doc["device_item_id"]] = doc["average_value"]

    return result


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["test-mongodb"]
    collection = db["test_collection"]

    # count = count_documents(collection)
    # print(f"Total documents: {count}")

    # If you want add index
    # collection.create_index([("device_item_id", pymongo.ASCENDING)])

    device_item_id = 3
    # results = find_by_device_item_id(collection, device_item_id)
    # print(f"Found {len(results)} documents with device_item_id={device_item_id}")

    results = find_by_device_item_id_aggregate(collection, device_item_id)
    print(f"Aggregate found {len(results)} documents with device_item_id={device_item_id}")


    # start_time = datetime.datetime(2022, 6, 1)
    # end_time = datetime.datetime(2022, 6, 30)
    # results = find_by_time_range(collection, start_time, end_time)
    # print(f"Found {len(results)} documents between {start_time} and {end_time}")

    # result = calculate_monthly_sum(collection)
    # for month, data in result.items():
    #     print(f"Month: {month}")
    #     for device_item_id, value_sum in data.items():
    #         print(f"    Device Item ID: {device_item_id}, Sum of Values: {value_sum}")

    # result = calculate_monthly_avarage(collection)
    # for month, data in result.items():
    #     print(f"Month: {month}")
    #     for device_item_id, value_sum in data.items():
    #         print(f"    Device Item ID: {device_item_id}, Avarage of Values: {value_sum}")

    # result1 = calculate_daily_average(collection, [1, 2, 3])
    # # for date, data in result.items():
    # #     print(f"Date: {date}")
    # #     for item_id, value in data.items():
    # #         print(f"Device Item: {item_id}, Average Value: {value:.2f}")

    # result2 = aggregate_calculate_daily_average(collection, [3])
    # # for date, data in result2.items():
    # #     print(f"Date: {date}")
    # #     for item_id, value in data.items():
    # #         print(f"Device Item: {item_id}, Average Value: {value:.2f}")

    # with open('result1.json', 'w', encoding='utf-8') as file:
    #     json.dump(result1, file, indent=4)
    # with open('result2.json', 'w', encoding='utf-8') as file:
    #     json.dump(result2, file, indent=4)

    client.close()
