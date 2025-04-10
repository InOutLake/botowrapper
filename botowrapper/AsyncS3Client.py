import aioboto3
import asyncio
from os import PathLike
from typing import AsyncGenerator, Any, BinaryIO
from pathlib import Path
from contextlib import asynccontextmanager


class AsyncS3Client:
    def __init__(self, bucketname: str = None, max_concurency: int = 5, **session_params: dict[str, Any]) -> None:
        self._session = aioboto3.Session()
        self._session_params = session_params
        self._selected_bucket = bucketname
        self._semaphore = asyncio.Semaphore(5)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.close()

    @asynccontextmanager
    async def get_client(self) -> AsyncGenerator:
        async with self._session.client("s3", **self._session_params) as client:
            yield client

    @property
    def selected_bucket(self) -> str:
        if self._selected_bucket is None:
            raise ValueError("Bucket is not selected")
        return self._selected_bucket

    @selected_bucket.setter
    async def set_selected_bucket(self, bucketname: str) -> None:
        if bucketname not in await self.ls_buckets():
            # TODO search for fitting exception in botocore.exceptions
            raise Exception(f"Bucket named {bucketname} is unavailable")
        #! Bug: impossible to select bucket if bucket is none by default
        self._selected_bucket = bucketname

    async def ls_buckets(self) -> list[str]:
        async with self.get_client() as client:
            response = await client.list_buckets()
            return [bucket["Name"] for bucket in response["Buckets"]]

    async def create_bucket(self, bucketname: str) -> None:
        if bucketname not in await self.ls_buckets():
            async with self.get_client() as client:
                await client.create_bucket(Bucket=bucketname)

    async def upload_file(self, file_path: str, key: str | None = None, overwrite: bool = False, **kwargs) -> None:
        async with self._semaphore:
            if key is None:
                key = Path(file_path).name

            if not overwrite and await self.check_exist(key):
                raise FileExistsError(f"You are trying to overwrite {key}. Use overwrite=True flag if intended")

            async with self.get_client() as client:
                await client.upload_file(Bucket=self._selected_bucket, Filename=file_path, Key=key, **kwargs)

    async def upload_stream(self, stream: BinaryIO, key: str, **kwargs):
        """
        Uploads a binary stream to S3.

        :param stream: Binary stream to upload.
        :param key: S3 key name.
        :param kwargs: Additional arguments passed to put_object().
        """
        try:
            async with self._semaphore:
                data = await stream.read()
                async with self.get_client() as client:
                    await client.put_object(Bucket=self._selected_bucket, Key=key, Body=data, **kwargs)
        except Exception as e:
            await stream.seek(0)
            raise e

    async def ls_files_paged(self, prefix: str, page_len: int = 100) -> AsyncGenerator:
        async with self.get_client() as client:
            continuation_token = None
            while True:
                list_kwargs = {
                    "Bucket": self._selected_bucket,
                    "Prefix": prefix,
                    "MaxKeys": page_len,
                }
                if continuation_token:
                    list_kwargs["ContinuationToken"] = continuation_token

                response = await client.list_objects_v2(**list_kwargs)
                contents = response.get("Contents", [])
                if contents:
                    yield contents

                if not response.get("IsTruncated"):
                    break
                continuation_token = response.get("NextContinuationToken")

    async def ls_files(self, prefix: str = "") -> AsyncGenerator:
        """
        Lists all files under a prefix.

        :param prefix: Prefix to search under.
        :yield: File object metadata.
        """
        async for page in self.ls_files_paged(prefix):
            for obj in page:
                yield obj

    async def download(self, prefix: str, destination: PathLike, overwrite: bool = False) -> list[tuple[str, Exception | None]]:
        """
        Downloads files from S3 to a local destination folder.

        :param prefix: Prefix or key of the file(s).
        :param destination: Local folder to save the files.
        :param overwrite: Safety flag to prevent unintended overwrites.
        :return: List of tuples (local file path, exception if any)
        """
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)

        tasks_data: list[tuple[str, Path]] = []

        async with self.get_client() as client:
            async for obj in self.ls_files(prefix):
                key = obj["Key"]
                relative_path = Path(key).relative_to(prefix) if prefix and key != prefix else Path(key).name
                local_path = destination / relative_path

                if local_path.exists() and not overwrite:
                    tasks_data.append((key, local_path, FileExistsError(f"{local_path} already exists and overwrite is False")))
                else:
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    tasks_data.append((key, local_path, None))

            async def download_file(key: str, path: Path, precheck_error: Exception | None):
                if precheck_error is not None:
                    return str(path), precheck_error

                async with self._semaphore:
                    try:
                        await client.download_file(
                            Bucket=self._selected_bucket,
                            Key=key,
                            Filename=str(path),
                        )
                        return str(path), None
                    except Exception as e:
                        return str(path), e

            tasks = [asyncio.create_task(download_file(key, path, error)) for key, path, error in tasks_data]
            return await asyncio.gather(*tasks)

    # ? This one was tricky. Still not sure if implemented right, and if I should have used as_completed here.
    # ? It may be usefull in some cases, but usualy you would work with chunks coherently, so I stick to gather
    async def download_by_chunks(self, key: str, chunk_size: int = 1024 * 1024) -> AsyncGenerator[bytes, None]:
        async with self.get_client() as client:
            head_response = await client.head_object(Bucket=self._selected_bucket, Key=key)
            total_size = int(head_response["ContentLength"])
            num_chunks = (total_size + chunk_size - 1) // chunk_size

            async def download_chunk(index: int):
                async with self._semaphore:
                    start_range = index * chunk_size
                    end_range = min(start_range + chunk_size - 1, total_size - 1)
                    range_header = f"bytes={start_range}-{end_range}"
                    response = await client.get_object(Bucket=self._selected_bucket, Key=key, Range=range_header)
                    data = await response["Body"].read()
                    return index, data

            tasks = [asyncio.create_task(download_chunk(i) for i in range(num_chunks))]

            # ? chunks = [None] * num_chunks
            # ? for future in asyncio.as_completed(*tasks):
            # ?     try:
            # ?         index, data = await future
            # ?         chunks[index] = data
            # ?     except Exception as e:
            # ?         raise e

            chunks = await asyncio.gather(*tasks)
            for chunk in chunks:
                yield chunk

    async def copy(self, prefix: str, destination_prefix: str, overwrite: bool = False) -> list[tuple[str, Exception | None]]:
        """
        Copies files under a prefix to a new prefix.
        :param prefix: Original prefix.
        :param destination_prefix: New prefix.
        :param overwrite: File will be skipped if already exists in destination. Set True to overwrite them.
        """
        async with self.get_client() as client:

            async def copy_task(source_key, destination_key, overwrite):
                result = None
                async with self._semaphore:
                    try:
                        if not await self.check_exist(destination_key) or overwrite:
                            copy_source = {"Bucket": self._selected_bucket, "Key": source_key}
                            await client.copy_object(Bucket=self._selected_bucket, CopySource=copy_source, Key=destination_key)
                        else:
                            result = FileExistsError(f"You are trying to overwrite {destination_key}. Use overwrite=True flag if intended")
                    except Exception as e:
                        result = e
                    finally:
                        return (destination_key, result)

            tasks = []
            async for obj in self.ls_files(prefix):
                source_key = obj["Key"]
                destination_key = source_key.replace(prefix, destination_prefix, 1)
                tasks.append(asyncio.create_task(copy_task(source_key, destination_key, overwrite)))
            return await asyncio.gather(*tasks)

    async def move(self, prefix: str, new_prefix: str, overwrite: bool = False) -> list[tuple[str, Exception | None]]:
        """
        Moves files from one prefix to another.
        :param prefix: Original prefix.
        :param new_prefix: New prefix.
        :param overwrite: File will be skipped if already exists in destination. Set True to overwrite it.
        """
        async with self.get_client() as client:

            async def move_task(source_key, destination_key):
                result = None
                async with self._semaphore:
                    try:
                        if overwrite or not await self.check_exist(destination_key):
                            copy_source = {"Bucket": self._selected_bucket, "Key": source_key}
                            # could use self.copy instead
                            await client.copy_object(Bucket=self._selected_bucket, CopySource=copy_source, Key=destination_key)
                        else:
                            result = FileExistsError(f"{destination_key} exists. Use overwrite=True to replace.")
                    except Exception as e:
                        result = e
                    return destination_key, result

            tasks = []
            original_keys = [obj["Key"] async for obj in self.ls_files(prefix)]

            for key in original_keys:
                dest_key = key.replace(prefix, new_prefix, 1)
                tasks.append(move_task(key, dest_key))

            results = await asyncio.gather(*tasks)

            for i in range(0, len(original_keys), 1000):
                chunk = original_keys[i : i + 1000]
                async with self.get_client() as client:
                    await client.delete_objects(Bucket=self._selected_bucket, Delete={"Objects": [{"Key": k} for k in chunk]})

            return results

    async def check_exist(self, prefix: str) -> bool:
        """
        Checks if any file exists under a prefix.

        :param prefix: Prefix to check.
        :return: True if any file exists.
        """
        pages = self.ls_files_paged(prefix)
        return bool(await anext(pages, None))

    async def remove(self, prefix: str) -> None:
        """
        Deletes files under a given prefix.
        :param prefix: Prefix to delete.
        """
        keys_to_delete = [{"Key": obj["Key"]} async for obj in self.ls_files(prefix)]

        async def delete_chunk(chunk):
            async with self._semaphore:
                async with self.get_client() as client:
                    await client.delete_objects(Bucket=self._selected_bucket, Delete={"Objects": chunk})

        tasks = []
        for i in range(0, len(keys_to_delete), 1000):
            chunk = keys_to_delete[i : i + 1000]
            tasks.append(delete_chunk(chunk))

        await asyncio.gather(*tasks)

    async def get_urls(self, prefix: str, expires_in: int = 3600) -> dict[str, str]:
        """
        Generates pre-signed URLs for files under a prefix.
        :param prefix: Prefix to list files.
        :param expires_in: Expiry time in seconds for each URL.
        :return: Dictionary of key to URL.
        """

        async def url_task(key):
            async with self._semaphore:
                async with self.get_client() as client:
                    url = await client.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": self._selected_bucket, "Key": key},
                        ExpiresIn=expires_in,
                    )
                return key, url

        tasks = []
        async for obj in self.ls_files(prefix):
            key = obj["Key"]
            tasks.append(url_task(key))

        results = await asyncio.gather(*tasks)
        return dict(results)

    async def count_files(self, prefix: str) -> int:
        """
        Counts number of files under a prefix.

        :param prefix: Prefix to count.
        :return: Total number of files.
        """
        return sum([1 async for _ in self.ls_files(prefix)])

    async def get_sizes(self, prefix: str) -> dict[str, int]:
        """
        Gets size of each file under a prefix.

        :param prefix: Prefix to check.
        :return: Dictionary of key to size in bytes.
        """
        return {f["Key"]: f["Size"] async for f in self.ls_files(prefix)}
