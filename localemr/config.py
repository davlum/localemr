import boto3
import os


class Configuration:
    def __init__(self, args):
        fetch_from_s3 = os.environ.get('FETCH_FROM_S3', args.fetch_from_s3)
        self.fetch_from_s3 = fetch_from_s3 if isinstance(fetch_from_s3, bool) else fetch_from_s3 == 'True'
        convert_s3_to_local = os.environ.get('CONVERT_S3_TO_LOCAL', args.convert_s3_to_local)
        self.convert_s3_to_local = convert_s3_to_local if isinstance(convert_s3_to_local, bool) else convert_s3_to_local == 'True'
        # If local_dir not specified temporary directories will be used
        self.local_dir = os.environ.get('LOCAL_DIR', args.local_dir)
        self.livy_host = os.environ.get('LIVY_HOST', args.livy_host)
        self.max_fetch_from_s3 = os.environ.get('MAX_FETCH_FROM_S3', args.max_fetch_from_s3)
        self.livy_log_file_lines = os.environ.get('MAX_FETCH_FROM_S3', args.livy_log_file_lines)
        if self.fetch_from_s3:
            self.s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_HOST', args.s3_host))
