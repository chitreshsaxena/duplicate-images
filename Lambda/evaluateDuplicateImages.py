import json
import os
import boto3
import uuid
from urllib.parse import unquote_plus
from datetime import datetime


client = boto3.client('lambda')
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):

    for record in event['Records']:
        inputParams = {
            "bucket"   : record['s3']['bucket']['name'],
            "key"      : record['s3']['object']['key']
        }
        uploadImageName = record['s3']['bucket']['name'] + '/' + unquote_plus(record['s3']['object']['key'])
        response = client.invoke(
            FunctionName='arn:aws:lambda:us-west-2:298566585559:function:findDuplicateImages',
            InvocationType='RequestResponse',
            Payload= json.dumps(inputParams)
        )

        responseFromLambda = json.load(response['Payload'])
        results = list(filter(lambda item: item['score'] < 200, responseFromLambda))
        print(results)

        for item in results:
            dynamodb.put_item(TableName='DuplicateImagesLog',
                Item={'upload_image_name':{'S':uploadImageName},
                    'existing_image_name':{'S':item['file']},
                    'user_name':{'S':'demo user'},
                    'upload_date_time':{'S':datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
            })

    return {
        'statusCode': 200,
        'body': json.dumps('Duplicate Evaluation completed - please check DuplicateImagesLog')
    }
