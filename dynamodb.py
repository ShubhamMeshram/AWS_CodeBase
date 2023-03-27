import time
import boto3

def create_tbl_on_dynamodb(table_name):
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    table = dynamodb.create_table (
        TableName = table_name,
        KeySchema = [
            {
                'AttributeName': 'Name',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Email',
                'KeyType': 'RANGE'
            }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName': 'Name',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName':'Email',
                    'AttributeType': 'S'
                }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits':1,
                    'WriteCapacityUnits':1
                }

        )
    print(f"\n\nTable created {table_name}")
    time.sleep(10)

def migrate(source, target):
    dynamo_client = boto3.client('dynamodb', region_name="us-east-1")
    dynamo_target_client = boto3.client('dynamodb', region_name="us-east-1")

    dynamo_paginator = dynamo_client.get_paginator('scan')
    dynamo_response = dynamo_paginator.paginate(
        TableName=source,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True
    )
    for page in dynamo_response:
        for item in page['Items']:
            dynamo_target_client.put_item(
                TableName=target,
                Item=item
            )

    print(f"\n\nMigrated data from {source} to {target}")
    time.sleep(30)


def delete_dynamodb_table(table_name):
    client = boto3.client('dynamodb', region_name="us-east-1")
    #tbl_to_delete="Employees_test_copy"
    response = client.delete_table(
        TableName=table_name
    )
    print(f"\n\nTable deleted {table_name}")


if __name__ == '__main__':
    create_tbl_on_dynamodb('Employees_test_copy')
    migrate('Employees_test', 'Employees_test_copy')
    delete_dynamodb_table('Employees_test_copy')
