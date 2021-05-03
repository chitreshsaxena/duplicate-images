import boto3
import json
import csv
import os
import sys
import uuid
from urllib.parse import unquote_plus
from PIL import Image
import numpy as np
import glob

s3_client = boto3.client('s3')
endpoint_name = 'mxnet-inference-2021-03-08-07-45-17-278'
root_dir = '/tmp/'

# Encoder for converting numpy to json
class NumPyArangeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist() # or map(int, obj)
        return json.JSONEncoder.default(self, obj)

def get_visual_data(img, endpoint_name):
    #print(img)
    #print(endpoint_name)
    sm = boto3.client('sagemaker-runtime')
    response = sm.invoke_endpoint(
        EndpointName=endpoint_name,
        Body=json.dumps(img, cls=NumPyArangeEncoder)
    )
    #print(response['Body'])
    response = json.loads(response["Body"].read())
    #print(response)
    return response

def get_image(img_name):
    img = Image.open(img_name)
    img = img.resize((224, 224))
    img = np.transpose(img, (2,0,1))
    return img

# Calculate the Hamming distance between two bit strings
def hamming2(s1, s2):
    assert len(s1) == len(s2)
    return sum(c1 != c2 for c1, c2 in zip(s1, s2))

def get_metadata_existing_images(bucket, prefix):
    #print(prefix)
    metadata = []

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        MaxKeys=20,
        Prefix=prefix
    )
    s3 = boto3.resource('s3')
    sourceBucket = s3.Bucket(bucket)
    for obj in sourceBucket.objects.filter(Delimiter='/', Prefix=prefix):
        path, fileName = os.path.split(obj.key)
        if fileName.strip()  != '' :
            #try:
            s3_client.download_file(Bucket=bucket, Key=obj.key, Filename=root_dir + fileName)
            #except Exception as e:
                #print(e)

    for file in os.listdir(root_dir):
        if file.endswith(".jpg"):
            fl = os.path.join(root_dir, file)
            try:
                image21 = get_image(fl)
                str = {'data': get_visual_data(image21, endpoint_name), 'image_path': fl}
                #print(str)
                metadata.append(str)
            except Exception as e:
                print(e)
                break
    return metadata
            #try:
            #print(fl)
            #os.remove(fl)
            #except Exception as e:
                #print(e)
               # break
# Search in the metadata list the most similar objects
def search_local_base(metadata, endpoint_name, file_name=None):
    try:
        image = get_image(file_name)
    except Exception as e:
        print(e)
        return

    item_hash = get_visual_data(image, endpoint_name)

    # get the item categories
    #categories = list(map( lambda x: ( object_classes[x[0]], x[1]), item_hash['categories']))
    #print(categories)
    # measure the distance to each item
    dist = {}
    cnt = 0
    for meta in metadata:
        #dist[meta['id']] = hamming2( item_hash['hash'], meta['hash'] )
        #print(meta['hash'])
        dist[cnt] = hamming2( item_hash['hash'], meta['data']['hash'] )
        cnt = cnt + 1

    result = []
    for w in sorted(dist, key=dist.get, reverse=False):
        result.append( (dist[w], metadata[w][ 'image_path']) )

    return result

def clear_tmp_files():
    files = glob.glob('/tmp/*')
    for f in files:
        print(f)
        os.remove(f)

def lambda_handler(event, context):

    #for record in event['Records']:
        clear_tmp_files()

        metadata = []
        bucket = event['bucket'] #record['s3']['bucket']['name']
        key = unquote_plus(event['key']) #unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        upload_path = '/tmp/resized-{}'.format(tmpkey)
        s3_client.download_file(bucket, key, download_path)
        path, fileName = os.path.split(key)
        metadata = get_metadata_existing_images(bucket, path + '/')

        result = search_local_base(metadata, endpoint_name, download_path)

        result2 = []
        for item in result:
            ignoreFileMaybe = item[1].split('/')[len(item[1].split('/'))-1]
            if(ignoreFileMaybe != fileName):
                x = item[0]
                y = item[1].replace('/tmp', bucket + '/' + path)
                result2.append({'file': y, 'score':x})

        clear_tmp_files()

        print(result2)
        return result2
