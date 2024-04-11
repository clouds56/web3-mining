# %%
# load env
import os
import boto3.s3
from dotenv import load_dotenv
while not os.path.exists("Cargo.toml"):
  os.chdir("../")
load_dotenv(".env", override=True)
S3_BUCKET = os.environ['S3_BUCKET']
print(S3_BUCKET)
# %%
import boto3
s3 = boto3.client('s3', endpoint_url="https://"+S3_BUCKET, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
s3.list_buckets()

# %%
# upload to s3
from tqdm import tqdm
def s3_upload(s3: "boto3.S3Client", local_filename, *, Bucket, Key):
  # get length from local file
  total_length = os.path.getsize(local_filename)
  with tqdm(total=total_length, desc=f's3://{Bucket}/{Key}', bar_format="{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}",  unit='B', unit_scale=True, unit_divisor=1024) as pbar:
    s3.upload_file(local_filename, Bucket=Bucket, Key=Key, Callback=pbar.update)

s3_upload(s3, "./data.zip", Bucket='reth', Key="data.zip")

# %%
versions = s3.list_object_versions(Bucket='reth')

# %%
versions['Versions']

# %%
