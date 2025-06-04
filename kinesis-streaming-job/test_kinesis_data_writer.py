import os

import boto3
import json
import time
from faker import Faker

fake = Faker()

KINESIS_STREAM_NAME = "test3"
KINESIS_REGION = "us-east-2"


def kinesis_write():
    kinesis_client = boto3.client('kinesis', region_name=KINESIS_REGION)

    def put_to_stream(index):
        data = {
            'index': index,
            'name': fake.name(),
            'address': fake.address(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'job': "job4",
            'bank': {
                'bank_country': fake.country(),
                'bank_swift': fake.swift(),
                'bank_iban': fake.iban(),
                'bank_account_number': fake.bban(),
                'bank_currency': fake.currency_code(),
                'bank_currency_name': fake.currency_name(),
                'bank_currency_symbol': fake.currency_symbol(),
            }
        }

        result = kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(data),
            PartitionKey=data['name']
        )

        print(result["ShardId"], result["SequenceNumber"], index)

    index = 0
    while True:
        put_to_stream(index)
        index += 1
        time.sleep(1)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    kinesis_write()
