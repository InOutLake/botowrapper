import boto3
from botowrapper.helpers import check_bucket_selected
from typing import BinaryIO, Any
from pathlib import Path
from os import PathLike
from typing import Iterable


class SyncS3Client:
    """Handles managing files in S3"""

    def __init__(self, bucketname: str = None, **session_params: dict[str, Any]) -> Any:
        """
        Initializes the S3 client wrapper.

        :param bucket_name: Optional. The bucket to use for operations.
        :param session_params: Additional parameters passed to boto3.client().
            Example:
                {
                    endpoint_url='http://host:9090',
                    aws_access_key_id='key',
                    aws_secret_access_key='password',
                    aws_session_token=None,
                    config=boto3.session.Config(signature_version='s3v4'),
                    verify=False
                }
        """
        self._client = boto3.client(service_name="s3", **session_params)
        self.create_bucket(bucketname)
        self._selected_bucket = bucketname

    def ls_buckets(self) -> list[str]:
        """
        Returns a list of all available S3 buckets.

        :return: List of bucket names.
        """
        return [b["Name"] for b in self._client.list_buckets()["Buckets"]]

    def create_bucket(self, bucketname: str) -> None:
        """
        Creates a bucket for client if such does not exist.

        :param bucketname: Bucket to be created name
        """
        if bucketname not in self.ls_buckets():
            self._client.create_bucket(Bucket=bucketname)

    @property
    def selected_bucket(self) -> str | None:
        """
        Gets the currently selected bucket.

        :return: Bucket name or None.
        """
        return self._selected_bucket

    @selected_bucket.setter
    def set_selected_bucket(self, bucketname: str) -> None:
        """
        Sets the bucket to use for S3 operations.

        :param bucketname: Name of the bucket.
        """
        available_buckets = self._client.get_buckets()
        if bucketname not in available_buckets:
            raise Exception(f"Bucket named {bucketname} is unavailable")
        self._selected_bucket = bucketname

    @check_bucket_selected
    def upload_file(self, file_path: str, key: str | None = None, overwrite: bool = False, **kwargs) -> None:
        """
        Uploads a local file to the S3 bucket.

        :param file_path: Path to the local file.
        :param key: S3 key name. If not specified, uses file name.
        :param kwargs: Additional arguments passed to upload_file().
        :param overwrite: Safety flag to prevent unintended overwrites.
        :return: True if upload succeeds.
        """
        if key is None:
            key = Path(file_path).name
        if self.check_exist(key) and not overwrite:
            raise FileExistsError(f"You are trying to overwrite {key}. Use overwrite=True flag if intended")
        self._client.upload_file(Bucket=self._selected_bucket, Filename=file_path, Key=key, **kwargs)

    @check_bucket_selected
    def upload_stream(self, stream: BinaryIO, key: str, **kwargs) -> None:
        """
        Uploads a binary stream to S3.

        :param stream: Binary stream to upload.
        :param key: S3 key name.
        :param kwargs: Additional arguments passed to put_object().
        :return: True if upload succeeds.
        """
        self._client.put_object(Bucket=self._selected_bucket, Key=key, Body=stream, **kwargs)

    @check_bucket_selected
    def ls_files_paged(self, prefix: str, page_len: int = 100) -> Iterable:
        """
        Lists files under a prefix with pagination.

        :param prefix: Prefix to search under.
        :param page_len: Number of files per page.
        :yield: Lists of objects per page.
        """
        paginator = self._client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self._selected_bucket,
            Prefix=prefix,
            PaginationConfig={"PageSize": page_len},
        )
        for page in page_iterator:
            yield page.get("Contents", [])

    @check_bucket_selected
    def ls_files(self, prefix: str) -> Iterable:
        """
        Lists all files under a prefix.

        :param prefix: Prefix to search under.
        :yield: File object metadata.
        """
        pages = self.ls_files_paged(prefix, 100)
        for page in pages:
            for obj in page:
                yield obj

    @check_bucket_selected
    def download(self, prefix: str, destination: PathLike, overwrite: bool = False) -> list[str]:
        """
        Downloads files from S3 to a local destination folder.

        :param prefix: Prefix or key of the file(s).
        :param destination: Local folder to save the files.
        :param overwrite: Safety flag to prevent unintended overwrites.
        :return: List of downloaded file paths.
        """
        downloaded_files = []
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        for obj in self.ls_files(prefix):
            key = obj["Key"]
            relative_path = Path(key).relative_to(prefix) if prefix and key != prefix else Path(key).name
            local_path = destination / relative_path
            # make sure user does not overwrite any local files unintentionaly
            if local_path.exists() and not overwrite:
                raise FileExistsError(f"You are trying to overwrite {local_path}. Use overwrite=True flag if intended")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self._client.download_file(Bucket=self._selected_bucket, Key=key, Filename=str(local_path))
            downloaded_files.append(str(local_path))
        return downloaded_files

    @check_bucket_selected
    def download_by_chunks(self, key: str, chunk_size: int = 1024 * 1024) -> Iterable:
        """
        Downloads a file in chunks (generator).

        :param key: S3 key.
        :param chunk_size: Chunk size in bytes.
        :yield: File content in chunks.
        """
        response = self._client.get_object(Bucket=self._selected_bucket, Key=key)
        body = response["Body"]
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                return
            yield chunk

    @check_bucket_selected
    def copy(self, prefix: str, destination_prefix: str, overwrite=False) -> None:
        """
        Copies files under a prefix to a new prefix.

        :param prefix: Original prefix.
        :param destination_prefix: New prefix.
        :param overwrite: File will be skipped if already exists in destination. Set True to overwrite them.
        """
        for obj in self.ls_files(prefix):
            source_key = obj["Key"]
            destination_key = source_key.replace(prefix, destination_prefix, 1)
            if not self.check_exist(destination_key) or overwrite:
                copy_source = {"Bucket": self._selected_bucket, "Key": source_key}
                self._client.copy_object(Bucket=self._selected_bucket, CopySource=copy_source, Key=destination_key)

    @check_bucket_selected
    def move(self, prefix: str, new_prefix: str, overwrite: bool) -> None:
        """
        Moves files from one prefix to another.

        :param prefix: Original prefix.
        :param new_prefix: New prefix.
        :param overwrite: File will be skipped if already exists in destination. Set True to overwrite it.
        """
        original_keys = [obj["Key"] for obj in self.ls_files(prefix)]

        for key in original_keys:
            dest_key = key.replace(prefix, new_prefix, 1)
            if not self.check_exist(dest_key) or overwrite:
                copy_source = {"Bucket": self._selected_bucket, "Key": key}
                self._client.copy_object(Bucket=self._selected_bucket, CopySource=copy_source, Key=dest_key)

        for i in range(0, len(original_keys), 1000):
            chunk = original_keys[i : i + 1000]
            self._client.delete_objects(Bucket=self._selected_bucket, Delete={"Objects": [{"Key": key} for key in chunk]})

    @check_bucket_selected
    def remove(self, prefix: str):
        """
        Deletes files under a given prefix.

        :param prefix: Prefix to delete.
        """
        # to minimize amount of requests to the S3 files are deleted in batches.
        # 1000 is the max amount of files per request
        keys_to_delete = [{"Key": f["Key"]} for f in self.ls_files(prefix)]
        for i in range(0, len(keys_to_delete), 1000):
            chunk = keys_to_delete[i : i + 1000]
            self._client.delete_objects(Bucket=self._selected_bucket, Delete={"Objects": chunk})

    @check_bucket_selected
    def check_exist(self, prefix: str) -> bool:
        """
        Checks if any file exists under a prefix.

        :param prefix: Prefix to check.
        :return: True if any file exists.
        """
        pages = self.ls_files_paged(prefix)
        return bool(next(pages, None))

    @check_bucket_selected
    def count_files(self, prefix: str) -> int:
        """
        Counts number of files under a prefix.

        :param prefix: Prefix to count.
        :return: Total number of files.
        """
        return sum(1 for _ in self.ls_files(prefix))

    @check_bucket_selected
    def get_sizes(self, prefix: str) -> dict[str, int]:
        """
        Gets size of each file under a prefix.

        :param prefix: Prefix to check.
        :return: Dictionary of key to size in bytes.
        """
        return {f["Key"]: f["Size"] for f in self.ls_files(prefix)}

    @check_bucket_selected
    def get_urls(self, prefix: str, expires_in: int = 3600) -> dict[str, str]:
        """
        Generates pre-signed URLs for files under a prefix.

        :param prefix: Prefix to list files.
        :param expires_in: Expiry time in seconds for each URL.
        :return: Dictionary of key to URL.
        """
        files = self.ls_files(prefix)
        urls = {}
        for obj in files:
            key = obj["Key"]
            url = self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._selected_bucket, "Key": key},
                ExpiresIn=expires_in,
            )
            urls[key] = url
        return urls
