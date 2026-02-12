import boto3
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os


load_dotenv()

logging.basicConfig(level=logging.INFO)


class S3Client:
    """
        Each S3 client connects to one bucket.
        entrypoints:
            - safely_upload() -> validate the env , check the file existance, upload
            - upload() -> only upload without check for existance, checks the env content
            - check_file_exists() -> onlly check file existance , checks the env content.
    """

    def __init__(self, file_path):

        self.file_path = file_path
        self.endpoint_url = os.getenv("ENDPOINT_URL")
        self.access_key = os.getenv("ACCESS_KEY_ID")
        self.secret_key = os.getenv("SECRET_KEY")
        self.bucket_name = os.getenv("BUCKET_NAME")

        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        
    def safely_upload(self):
        """
            Validate, check existence, and upload.
        """
        # Step 1: Validate
        if not self._pre_validation():
            return False

        # Step 2: Check if file exists
        if self._check_file_exists():
            logging.info("File already exists in S3. Skipping upload.")
            return False

        # Step 3: Upload
        logging.info("File not found in S3. Uploading...")
        return self._upload()

    def _get_object_name(self):
        return os.path.basename(self.file_path)

    def _pre_validation(self):

        if not all([
            self.endpoint_url,
            self.access_key,
            self.secret_key,
            self.bucket_name
        ]):
            logging.error("Missing environment variables")
            return False

        if not os.path.exists(self.file_path):
            logging.error(f"File not found: {self.file_path}")
            return False

        return True
    
    def check_file_exists(self):
        status = self._pre_validation()
        if status:
            return self._check_file_exists()
        else:
            logging.error("prevalidation not passed.")
            return False
        
    def _check_file_exists(self):

        try:
            self.s3.head_object(
                Bucket=self.bucket_name,
                Key=self._get_object_name()
            )
            return True

        except ClientError as e:

            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            logging.error(e)
            return False

    def upload(self):
        status = self._pre_validation()
        if status:
            return self._upload()
        else:
            logging.error("prevalidation not passed.")
            return False
    
    def _upload(self):

        try:

            with open(self.file_path, "rb") as file:
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=self._get_object_name(),
                    Body=file,
                    ACL="private"
                )

            logging.info("Upload successful!")
            return True

        except ClientError as e:
            logging.error(e)
            return False

        except Exception as e:
            logging.error(e)
            return False


def main():
    file_path = "webhighlights-backup-20260206-094850.json"
    client = S3Client(file_path)
    client.safely_upload()

if __name__ == "__main__":
    main()