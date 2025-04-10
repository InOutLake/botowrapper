import unittest
from pathlib import Path
import configparser
from botowrapper import SyncS3Client
import docker
import time
from shutil import rmtree
import os


# ? Only main test cases covered,
# ? Comprehensive tests were too much to maintain for a test assignment
class TestS3Client(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = configparser.ConfigParser()
        config.read(str(Path(__file__).parent) + "\\.cfg")

        minio_config = config["minio"]
        access_key = minio_config.get("access_key")
        secret_key = minio_config.get("secret_key")
        cls.bucket_name = minio_config.get("bucket_name")
        host = minio_config.get("host")
        endpoint_port = minio_config.get("endpoint_port")
        console_port = minio_config.get("console_port")

        cls.docker_client = docker.from_env()
        cls.docker_client.images.pull("minio/minio")
        try:
            cls.container = cls.docker_client.containers.run(
                "minio/minio",
                command="server /data --address 0.0.0.0:9000 --console-address 0.0.0.0:9001",
                ports={"9000/tcp": endpoint_port, "9001/tcp": console_port},
                environment={"MINIO_ROOT_USER": access_key, "MINIO_ROOT_PASSWORD": secret_key},
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
        cls.sync_client = SyncS3Client(
            bucketname=cls.bucket_name,
            region_name="local",
            endpoint_url=f"http://{host}:{endpoint_port}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        cls.sync_client.create_bucket(cls.bucket_name)
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
        rmtree(cls.testfiles)
        rmtree(cls.testdownloaded)

    def delete_all_objects(self):
        s3_client = self.sync_client._client
        response = s3_client.list_objects_v2(Bucket=self.bucket_name)

        objects_to_delete = [{"Key": obj["Key"]} for obj in response.get("Contents", [])]

        if objects_to_delete:
            response = s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": objects_to_delete, "Quiet": True})

    def upload_all(self):
        for f in self.testfiles.iterdir():
            self.sync_client._client.upload_file(Filename=f, Key="prefix/" + f.name, Bucket=self.bucket_name)

    def tearDown(self):
        self.delete_all_objects()
        for f in self.testdownloaded.iterdir():
            os.remove(f)

    def test_upload_file(self):
        self.sync_client.upload_file(self.testfiles / "test1.txt")
        response = self.sync_client._client.list_objects_v2(Bucket=self.bucket_name)
        self.assertIn("test1.txt", [obj["Key"] for obj in response.get("Contents", [])])

    def test_upload_file_with_overwrite(self):
        self.sync_client.upload_file(self.testfiles / "test1.txt")
        self.sync_client.upload_file(self.testfiles / "test1.txt", overwrite=True)
        response = self.sync_client._client.list_objects_v2(Bucket=self.bucket_name)
        self.assertIn("test1.txt", [obj["Key"] for obj in response.get("Contents", [])])

    def test_upload_stream(self):
        with open(self.testfiles / "test1.txt", "rb") as stream:
            self.sync_client.upload_stream(stream, "test1_stream.txt")
        response = self.sync_client._client.list_objects_v2(Bucket=self.bucket_name)
        self.assertIn("test1_stream.txt", [obj["Key"] for obj in response.get("Contents", [])])

    def test_ls_files(self):
        self.upload_all()
        files = list(self.sync_client.ls_files("prefix"))
        self.assertEqual(len(files), 100)
        self.assertIn("prefix/test1.txt", [file["Key"] for file in files])

    def test_ls_files_paged(self):
        self.upload_all()
        pages = list(self.sync_client.ls_files_paged("prefix", page_len=50))
        self.assertEqual(len(pages), 2)
        self.assertIn("prefix/test1.txt", [file["Key"] for file in pages[0]])

    def test_download_file(self):
        self.upload_all()
        self.sync_client.download("prefix", destination=self.testdownloaded)
        with open(self.testdownloaded / "test1.txt", "r") as f:
            content = f.read()
        self.assertEqual(content, "Test file 1")
        self.assertEqual(len(list(self.testdownloaded.iterdir())), 100)

    def test_download_by_chunks(self):
        self.sync_client.upload_file(self.testfiles / "test1.txt", "prefix/test1.txt")
        chunks = list(self.sync_client.download_by_chunks("prefix/test1.txt", chunk_size=10))
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(isinstance(chunk, bytes) for chunk in chunks))

    def test_copy(self):
        self.upload_all()
        self.sync_client.copy("prefix", "newprefix", overwrite=True)
        response = self.sync_client._client.list_objects_v2(Bucket=self.bucket_name, Prefix="newprefix")
        self.assertEqual(len(response.get("Contents", [])), 100)

    def test_move(self):
        self.upload_all()
        self.sync_client.move("prefix", "newprefix", overwrite=True)
        response = self.sync_client._client.list_objects_v2(Bucket=self.bucket_name, Prefix="newprefix")
        self.assertEqual(len(response.get("Contents", [])), 100)
        response = self.sync_client._client.list_objects_v2(Bucket=self.bucket_name, Prefix="prefix")
        self.assertEqual(len(response.get("Contents", [])), 0)

    def test_remove(self):
        self.upload_all()
        self.sync_client.remove("prefix")
        response = self.sync_client._client.list_objects_v2(Bucket=self.bucket_name, Prefix="prefix")
        self.assertEqual(len(response.get("Contents", [])), 0)

    def test_check_exist(self):
        self.sync_client.upload_file(self.testfiles / "test1.txt", "prefix/test1.txt")
        self.assertTrue(self.sync_client.check_exist("prefix"))
        self.assertFalse(self.sync_client.check_exist("nonexistentprefix"))

    def test_count_files(self):
        self.upload_all()
        count = self.sync_client.count_files("prefix")
        self.assertEqual(count, 100)

    def test_get_sizes(self):
        self.upload_all()
        sizes = self.sync_client.get_sizes("prefix")
        self.assertEqual(len(sizes), 100)
        self.assertGreater(sizes["prefix/test1.txt"], 0)

    def test_get_urls(self):
        self.upload_all()
        urls = self.sync_client.get_urls("prefix")
        self.assertEqual(len(urls), 100)
        self.assertTrue(urls["prefix/test1.txt"].startswith("http"))


if __name__ == "__main__":
    unittest.main()
