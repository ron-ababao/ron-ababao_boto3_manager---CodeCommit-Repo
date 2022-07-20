import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
level=logging.INFO,
format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',)
log = logging.getLogger()

def create_bucket(name, region=None):
    region = region or 'us-east-2'
    client = boto3.client('s3', region_name=region)
    params = {
        'Bucket': name,
        'CreateBucketConfiguration': {
            'LocationConstraint': region,
            }
        }
        
    try:
        client.create_bucket(**params)
        return True
    except ClientError as err:
        log.error(f'{err} - Params {params}')
        return False
        
tusk=create_bucket("testingawesomestuffron")
log.info(tusk)

def list_buckets():
    s3 = boto3.resource('s3')
    count = 0
    for bucket in s3.buckets.all():
        print(bucket.name)
        count += 1
    print(f'Found {count} buckets!')
    
list_buckets()

def get_bucket(name, create=False, region=None):
    client = boto3.resource('s3')
    bucket = client.Bucket(name=name)
    if bucket.creation_date:
        return bucket
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f'Bucket {name} does not exist!')
            return

getting_buck=get_bucket('testawesomeproject1').creation_date
log.info(getting_buck)

def create_tempfile(file_name=None, content=None, size=300):
    """Create a temporary text file"""
    filename = f'{file_name or uuid.uuid4().hex}.txt'
    with open(filename, 'w') as f:
        f.write(f'{(content or "0") * size}')
    return filename

temporary=tmp_file = create_tempfile()
log.info(f" temporary file {temporary} has been made")

def create_bucket_object(bucket_name, file_path, key_prefix=None):

    bucket = get_bucket(bucket_name)
    dest = f'{key_prefix or ""}{file_path}'
    bucket_object = bucket.Object(dest)
    bucket_object.upload_file(Filename=file_path)
    return bucket_object

b_obj = create_bucket_object('testawesomeproject1', tmp_file, key_prefix='temp/')
log.info(f"Object created {b_obj} with key {b_obj.key}")

def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):

    bucket = get_bucket(bucket_name)
    params = {'key': object_key}
    if version_id:
        params['VersionId'] = version_id
    bucket_object = bucket.Object(**params)
    dest = Path(f'{dest or ""}')
    file_path = dest.joinpath(PosixPath(object_key).name)
    bucket_object.download_file(f'{file_path}')
    return bucket_object, file_path

bucket_obj_key = b_obj.key
b_obj, tmp_file = get_bucket_object('testawesomeproject1',bucket_obj_key)
log.info(f"bucket object key is {b_obj} and temporary file is {tmp_file}")

def enable_bucket_versioning(bucket_name):
    """Enable bucket versioning for the given bucket_name
    """
    bucket = get_bucket(bucket_name)
    versioned = bucket.Versioning()
    versioned.enable()
    return versioned.status

enablement=enable_bucket_versioning('testawesomeproject1')
log.info(f"it is {enablement}")
tmp_file.open().read()
tmp_file.open(mode='w').write('10' * 500)
tmp_file.open().read()

create_bucket_object('testawesomeproject1', tmp_file.name, key_prefix='temp/')
print(list(get_bucket('testawesomeproject1').objects.all()))

for _ in range(3):
    obj = create_bucket_object('testawesomeproject1',file_path=create_tempfile(),key_prefix='others/')
print(f'Object {obj.key} created!')


show_all_latest=list(get_bucket('testawesomeproject1').objects.all())
print(show_all_latest)
show_all_latest_temp=list(get_bucket('testawesomeproject1').objects.filter(Prefix='temp/'))
print(show_all_latest_temp)

def delete_bucket_objects(bucket_name, key_prefix=None):


    bucket = get_bucket(bucket_name)
    objects = bucket.object_versions
    if key_prefix:
        objects = objects.filter(Prefix=key_prefix)
    else:
        objects = objects.iterator()
    targets = [] # This should be a max of 1000
    for obj in objects:
        targets.append({'Key': obj.object_key,'VersionId': obj.version_id,})
    bucket.delete_objects(Delete={'Objects': targets,'Quiet': True,})
    return len(targets)


del_buck_obj=delete_bucket_objects('testawesomeproject1', key_prefix='temp/')
del_buck_obj_no=delete_bucket_objects('testawesomeproject1')
print(del_buck_obj,del_buck_obj_no)

def delete_buckets(name=None):
    count = 0
    if name:
        bucket = get_bucket(name)
        if bucket:
            bucket.delete()
            bucket.wait_until_not_exists()
            count += 1
    else:
        count = 0
        client = boto3.resource('s3')
        for bucket in client.buckets.iterator():
            try:
                bucket.delete()
                bucket.wait_until_not_exists()
                count += 1
            except ClientError as err:
                log.warning(f'Bucket {bucket.name}: {err}')
    
    return count

delete_buckets()
