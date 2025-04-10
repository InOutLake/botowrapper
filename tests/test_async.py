import unittest
import configparser
import logging
import docker
from pathlib import Path
import time
import os
from botowrapper import AsyncS3Client
import aiofiles

logging.getLogger("asyncio").setLevel(logging.CRITICAL)


class TestAsyncS3Client(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        config = configparser.ConfigParser()
        config.read(str(Path(__file__).parent) + "\\.cfg")

        minio_config = config["minio"]
        cls.access_key = minio_config.get("access_key")
        cls.secret_key = minio_config.get("secret_key")
        cls.bucket_name = minio_config.get("bucket_name")
        cls.host = minio_config.get("host")
        cls.endpoint_port = minio_config.get("endpoint_port")
        cls.console_port = minio_config.get("console_port")

        cls.docker_client = docker.from_env()
        cls.docker_client.images.pull("minio/minio")
        try:
            cls.container = cls.docker_client.containers.run(
                "minio/minio",
                command="server /data --address 0.0.0.0:9000 --console-address 0.0.0.0:9001",
                ports={"9000/tcp": cls.endpoint_port, "9001/tcp": cls.console_port},
                environment={"MINIO_ROOT_USER": cls.access_key, "MINIO_ROOT_PASSWORD": cls.secret_key},
                detach=True,
                name="test-bucket",
                healthcheck={
                    "Test": ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"],
                    "Interval": 30000000000,
                    "Timeout": 20000000000,
                    "Retries": 3,
                },
            )
        except Exception as _:
            cls.container = cls.docker_client.containers.get("test-bucket")
            cls.container.restart()
        time.sleep(5)

        cls.testfiles = Path(__file__).parent / "testfiles"
        cls.testdownloaded = Path(__file__).parent / "testdownloaded"
        cls.testfiles.mkdir(parents=True, exist_ok=True)
        cls.testdownloaded.mkdir(parents=True, exist_ok=True)

        for i in range(100):
            with open(cls.testfiles / f"test{i}.txt", "w") as f:
                f.write(f"Test file {i}")

    @classmethod
    def tearDownClass(cls):
        cls.container.stop()
        cls.container.remove(force=True)
        cls.docker_client.close()

    async def asyncSetUp(self):
        self.async_client = AsyncS3Client(
            bucketname=self.bucket_name,
            max_concurency=50,
            region_name="local",
            endpoint_url=f"http://{self.host}:{self.endpoint_port}",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        await self.async_client.create_bucket(self.bucket_name)

    async def asyncTearDown(self):
        await self.delete_all_objects()
        for f in self.testdownloaded.iterdir():
            os.remove(f)

    async def delete_all_objects(self):
        async with self.async_client.get_client() as client:
            response = await client.list_objects_v2(Bucket=self.bucket_name)
            objects_to_delete = [{"Key": obj["Key"]} for obj in response.get("Contents", [])]
            if objects_to_delete:
                await client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": objects_to_delete, "Quiet": True})

    async def upload_all(self):
        async with self.async_client.get_client() as client:
            for f in self.testfiles.iterdir():
                await client.upload_file(
                    Bucket=self.bucket_name,
                    Filename=f,
                    Key="prefix/" + f.name,
                )

    async def test_upload_stream(self):
        async with aiofiles.open(self.testfiles / "test1.txt", "rb") as stream:
            await self.async_client.upload_stream(stream, "test1_stream.txt")
        async with self.async_client.get_client() as client:
            objects = await client.list_objects_v2(Bucket=self.bucket_name)
        self.assertIn("test1_stream.txt", [obj["Key"] for obj in objects.get("Contents", [])])

    async def test_upload_file(self):
        await self.async_client.upload_file(self.testfiles / "test1.txt")
        async with self.async_client.get_client() as client:
            objects = await client.list_objects_v2(Bucket=self.bucket_name)
        self.assertIn("test1.txt", [obj["Key"] for obj in objects.get("Contents", [])])

    async def test_upload_file_with_overwrite(self):
        await self.async_client.upload_file(self.testfiles / "test1.txt")
        await self.async_client.upload_file(self.testfiles / "test1.txt", overwrite=True)
        async with self.async_client.get_client() as client:
            objects = await client.list_objects_v2(Bucket=self.bucket_name)
        self.assertIn("test1.txt", [obj["Key"] for obj in objects.get("Contents", [])])

    async def test_ls_files(self):
        await self.upload_all()
        files = [obj["Key"] async for obj in self.async_client.ls_files("prefix")]
        self.assertEqual(len(files), 100)
        self.assertIn("prefix/test1.txt", files)

    async def test_download_file(self):
        await self.upload_all()
        await self.async_client.download("prefix", destination=self.testdownloaded)
        with open(self.testdownloaded / "test1.txt", "r") as f:
            content = f.read()
        self.assertEqual(content, "Test file 1")
        self.assertEqual(len(list(self.testdownloaded.iterdir())), 100)

    async def test_copy(self):
        await self.upload_all()
        await self.async_client.copy("prefix", "newprefix", overwrite=True)
        async with self.async_client.get_client() as client:
            objects = await client.list_objects_v2(Bucket=self.bucket_name, Prefix="newprefix")
        self.assertEqual(len(objects.get("Contents", [])), 100)

    async def test_move(self):
        await self.upload_all()
        await self.async_client.move("prefix", "newprefix", overwrite=True)
        async with self.async_client.get_client() as client:
            objects = await client.list_objects_v2(Bucket=self.bucket_name, Prefix="newprefix")
        self.assertEqual(len(objects.get("Contents", [])), 100)
        async with self.async_client.get_client() as client:
            objects = await client.list_objects_v2(Bucket=self.bucket_name, Prefix="prefix")
        self.assertEqual(len(objects.get("Contents", [])), 0)

    async def test_remove(self):
        await self.upload_all()
        await self.async_client.remove("prefix")
        async with self.async_client.get_client() as client:
            objects = await client.list_objects_v2(Bucket=self.bucket_name, Prefix="prefix")
        self.assertEqual(len(objects.get("Contents", [])), 0)

    async def test_count_files(self):
        await self.upload_all()
        count = await self.async_client.count_files("prefix")
        self.assertEqual(count, 100)

    async def test_get_sizes(self):
        await self.upload_all()
        sizes = await self.async_client.get_sizes("prefix")
        self.assertEqual(len(sizes), 100)
        self.assertGreater(sizes["prefix/test1.txt"], 0)

    async def test_get_urls(self):
        await self.upload_all()
        urls = await self.async_client.get_urls("prefix")
        self.assertEqual(len(urls), 100)
        self.assertTrue(urls["prefix/test1.txt"].startswith("http"))


if __name__ == "__main__":
    unittest.main()
