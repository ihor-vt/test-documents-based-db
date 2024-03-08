**Mongo DB**


What can be used with Django:

Write your own validators for the created models.
1. pymongo - Not supported.
2. MongoEngine - Not supported.
3. Djongo - Not supported. You will need to use a paid subscription. Many bugs are open 329 bugs on github.


Docker parameters:
- CPU limit: 8
- Memory limit: 2 GB
- Swap: 1 GB



Python library for using MongoDB:
```
pip install pymongo
```

A command to fill your own test database with ~6.3 million documents:
```
python3 mongodb/fill_db.py
```

A command to run test:
```
python3 mongodb/test_query.py
```

1. Test case 1 | Total documents: 55_057
    - Function 'create_test_data' executed in 11.85 seconds

    - Function 'count_created_documents' executed in 0.01 seconds
    Total documents: 55057

    - Function 'find_by_device_id' executed in 0.11 seconds
    Found 55057 documents with device_id=1

    - Function 'find_by_time_range' executed in 0.02 seconds
    Found 2258 documents between 2022-06-01 00:00:00 and 2022-06-30 00:00:00

    - Function 'calculate_monthly_data' executed in 5.37 seconds with 3 device_item_id

    - Function 'calculate_monthly_avarage' executed in 0.34 seconds with 3 device_item_id

    - Function 'calculate_daily_average' executed in 13.74 seconds with 1 device_item_id

    - Function 'calculate_daily_average' executed in 14.46 seconds with 2 device_item_id

    - Function 'calculate_daily_average' executed in 14.99 seconds with 3 device_item_id

    - Function 'aggregate_calculate_daily_average' executed in 0.04 seconds with 2 device_item_id
    - Function 'aggregate_calculate_daily_average' executed in 0.11 seconds with 3 device_item_id


2. Test case 2 | Total documents: 77_921_678

    - DB parameters:
        * Sorage size: 2.64 GB (Uncompressed data size: 10.21 GB)
        * Avg.document size: 131.0 B
        * Indexes: 2
        * Total index size: 1.07 GB

    - Function 'create_test_data' takes more than 2h (Python + used 5 different function calls for 1 document. "Bad decision")

    - Function 'count_documents' executed in 17.89 seconds

    - Create index takes 48,33 seconds

    Before idexing:

        Function 'find_by_device_item_id' executed in 114.32 seconds
        Found 7793280 documents with device_item_id=3
    After:

        1) First time:
            Function 'find_by_device_item_id' executed in 90.09 seconds
            Found 7793280 documents with device_item_id=3

        2) Second time:
            Function 'find_by_device_item_id' executed in 79.93 seconds
            Found 7793280 documents with device_item_id=3

        3) With aggregate:
            1. Function 'find_by_device_item_id_aggregate' executed in 78.84 seconds
                Aggregate found 7793280 documents with device_item_id=3
            2. Function 'find_by_device_item_id_aggregate' executed in 77.55 seconds
                Aggregate found 7793280 documents with device_item_id=3



    - Function 'aggregate_calculate_daily_average' executed in 9.56 seconds (With indexes device_item_id)
    - Function 'aggregate_calculate_daily_average' executed in 38.07 seconds with 3 device_item_id
    - Function 'aggregate_calculate_daily_average' executed in 56.41 seconds with 10 device_item_id
