import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import time

def generate_emails():
    user= ''.join(np.random.choice(list('abcdefghijklmnopqrstuvwxyz'), size=6))
    return f"{user}@example.com"

num_records = 50000        #num of records in each file
id1=1
fake = Faker('en_US')
base_timestamp = datetime(1987, 1, 3, 9, 0, 0)
file_num=1

subscription_schema = pa.schema([
    ('first name', pa.string()),
    ('last name', pa.string()),
    ('timestamp', pa.timestamp('ms')),          #name and type
    ('id', pa.int32()),
    ('email', pa.string()),
    ('phone_num', pa.int64()),
    ('location', pa.string()),
    ('version', pa.string()),
    ('subscription_status', pa.string()),
    ('premium subscription', pa.bool_())
])

print(f'Generating parquet files')

# total_files = 3
# for file_index in range(total_files):

while True:

    f_names = [fake.first_name() for _ in range(num_records)]
    first_names= pa.array(f_names, pa.string())

    l_names = [fake.last_name() for _ in range(num_records)]
    last_names= pa.array(l_names, pa.string())

    timestamps = pa.array([base_timestamp + timedelta(minutes=i*5) for i in range(num_records)], type=pa.timestamp('ms'))

    ids = pa.array([id1+i for i in range(num_records)], type = pa.int32())      #array of 3 ids

    random_emails = [generate_emails() for _ in range(num_records)]
    emails = pa.array(random_emails, type=pa.string())

    numbers = np.random.randint(1000000000, 9999999999, num_records, dtype=np.int64) 
    phone_num = pa.array(numbers, type=pa.int64())

    cities = ['New York', 'Los Angeles', 'Chicago', 'San Francisco', 'Miami', 'Boston', 'Seattle', 'Dallas', 'Atlanta', 'Denver']
    locations= pa.array(np.random.choice(cities, size=num_records), pa.string())

    sub_status = ['active', 'unsubscribed']
    sub_status_array = pa.array(np.random.choice(sub_status, size=num_records))

    premium_sub = np.random.choice([True, False], size=num_records)
    premium_subscription = pa.array(premium_sub, type=pa.bool_())

    version = ['Red', 'Blue', 'Green']
    sub_version= pa.array(np.random.choice(version, size=num_records), pa.string())

    batch = pa.RecordBatch.from_arrays(
        [first_names, last_names, timestamps, ids, emails, phone_num, locations, sub_version, sub_status_array, premium_subscription], 
        schema=subscription_schema      #each array is a column
    )

    table = pa.Table.from_batches([batch])

    filename= f'sub{file_num}.parquet'
    pq.write_table(table, f'files/{filename}')
    time.sleep(7)

    file_num += 1
    id1 += num_records
    base_timestamp = timestamps[-1].as_py() + timedelta(minutes=5) 

    if (file_num%10)==0:
        time.sleep(30)