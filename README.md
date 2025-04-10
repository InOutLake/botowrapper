# Botowrapper

Botowrapper is a Python library that simplifies interactions with Amazon S3 storage. It provides an intuitive interface for common S3 operations such as uploading, downloading, listing, copying, moving, and deleting files. Built on top of Boto3, the AWS SDK for Python, Botowrapper streamlines S3 interactions, making it easier to manage your cloud storage.

## Features

- **Upload Files and Streams**: Upload local files or binary streams to your S3 bucket.
- **Download Files**: Retrieve files from your S3 bucket to local storage.
- **List Files**: Enumerate files in your S3 bucket with support for pagination.
- **Copy and Move Files**: Duplicate or relocate files within your S3 storage.
- **Delete Files**: Remove files from your S3 bucket.
- **Check File Existence**: Verify the presence of files in your S3 bucket.
- **Count and Size Files**: Obtain the number and sizes of files under a specific prefix.
- **Generate Pre-signed URLs**: Create time-limited URLs for secure access to your S3 files.

## Installation

```bash
pip install -i https://test.pypi.org/simple/ botowrapper==0.1.2
```

## Usage
### Sync
```python
from botowrapper import SyncS3Client

# Initialize the Botowrapper client
# Config could be set up in .aws/config
s3_client = SyncS3Client(
    bucketname='your-bucket-name',
    region_name='your-region',
    endpoint_url='https://s3.amazonaws.com',
    aws_access_key_id='your-access-key-id',
    aws_secret_access_key='your-secret-access-key'
)

# Upload a local file to S3
s3_client.upload_file('path/to/local/file.txt', key='folder/file.txt')

# Upload a binary stream to S3
with open('path/to/local/file.txt', 'rb') as file_stream:
    s3_client.upload_stream(file_stream, key='folder/file.txt')

# List files under a specific prefix
for file in s3_client.ls_files(prefix='folder/'):
    print(file['Key'])

# Download files from S3 to a local directory
s3_client.download(prefix='folder/', destination='path/to/local/directory')

# Copy files within S3
s3_client.copy(prefix='folder/', destination_prefix='new_folder/')

# Move files within S3
s3_client.move(prefix='folder/', new_prefix='new_folder/')

# Delete files from S3
s3_client.remove(prefix='folder/')

# Check if a file exists in S3
exists = s3_client.check_exist('folder/file.txt')

# Count the number of files under a prefix
file_count = s3_client.count_files(prefix='folder/')

# Get the sizes of files under a prefix
file_sizes = s3_client.get_sizes(prefix='folder/')

# Generate pre-signed URLs for files under a prefix
urls = s3_client.get_urls(prefix='folder/', expires_in=3600)
```

### Async
Async version has the same interface as sync version
```python
from botowrapper import AsyncS3Client
from pathlib import Path

async def main():
        s3_client = AsyncS3Client(
        bucketname='your-bucket-name',
        region_name='your-region',
        endpoint_url='https://s3.amazonaws.com',
        aws_access_key_id='your-access-key-id',
        aws_secret_access_key='your-secret-access-key'
    )
    tasks = []
    for f in Path("some/path").iterdir():
        tasks.append(s3_client.upload_file(f, key=f'destinatioin/{f.name}'))
    await asyncio.gather(tasks)
    upload_results = await s3_client.download(prefix="destination")

if __name__ == '__main__':
    asyncio.run(main())
```
