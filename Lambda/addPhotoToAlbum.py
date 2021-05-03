import json
import boto3

def detect_labels(photo, bucket):

    client=boto3.client('rekognition')
    s3_resource = boto3.resource('s3')
    lst  = photo.split('/')
    strCount = len(lst)
    fileName = photo.split('/')[strCount-1]

    itemLabels = {}

    apiResponse = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},
        MaxLabels=10)

    #print('Detected labels for ' + photo)
    #print()
    outputS3Prefix = ''
    itemLabels = {}
    subjectLabels = {}
    objectLabels = {}
    itemIds = {}
    recommendedItems = {}

    dynamodb = boto3.resource('dynamodb')
    personalizeRt = boto3.client('personalize-runtime')
    table = dynamodb.Table('PhotoLabels')
    dbResponse = table.scan()
    data = dbResponse['Items']

    while 'LastEvaluateKey' in dbResponse:
        dbResponse = table.scan(ExclusiveStartKey=response['LastEvaluateKey'])
        data.extend(dbResponse['Items'])

    for apiLabel in apiResponse['Labels']:
        print ("Label: " + apiLabel['Name'])
        print ("Confidence: " + str(apiLabel['Confidence']))
        for d in data:
            if apiLabel['Name'] == d['label_name'] and d['label_name'] not in itemLabels:
                itemLabels[d['label_name']] = apiLabel['Confidence'] > d['confidence_score']

            if apiLabel['Name'] == d['label_name'] and  apiLabel['Confidence'] > d['confidence_score'] :
                if d['subject'] == True:
                    if d['label_name'] not in subjectLabels:
                        subjectLabels[d['label_name']] =  apiLabel['Name']
                else:
                    if d['label_name'] not in objectLabels:
                        objectLabels[d['label_name']] = apiLabel['Name']


    recommendationResponse = personalizeRt.get_recommendations(
    campaignArn = 'arn:aws:personalize:us-west-2:298566585559:campaign/photo-user-campaign-v2', userId = 'DemoUser')

    for item in recommendationResponse['itemList']:
        ItemIdRec, ScoreRec = item['itemId'], item['score']
        recommendedItems[ItemIdRec] = ScoreRec

    for subj in subjectLabels:
        for obj in objectLabels:
            itemId = (subj + '-' + obj).lower()
            print(itemId)
            default = subj.lower() + '-default'
            print(default)
            if itemId in recommendedItems and default in recommendedItems:
                if recommendedItems[itemId] > recommendedItems[default]:
                    outputS3Prefix = 'output-photo-books/' + subj.lower() + '/' + obj.lower() + '/'
                else:
                    outputS3Prefix = 'output-photo-books/' + subj.lower() + '/default/'

    if(outputS3Prefix == ''):
        s3_resource.Object(bucket, 'output-photo-books/default/' + fileName).copy_from(CopySource= bucket + '/' + photo)
    else:
        s3_resource.Object(bucket, outputS3Prefix + fileName).copy_from(CopySource= bucket + '/' + photo)

    return len(apiResponse['Labels'])


def lambda_handler(event, context):

    s3_client = boto3.client('s3')
    rekognition_client = boto3.client('rekognition')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        label_count=detect_labels(key, bucket)

    return {
        'statusCode': 200,
        'body': json.dumps(label_count)
    }
