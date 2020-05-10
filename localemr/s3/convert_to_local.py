import pathlib
import os
import itertools
from functools import reduce
from botocore.errorfactory import ClientError
from typing import List, Tuple
import re


def extract_s3_parts(s3_path: str) -> (str, str):
    res = re.findall('s3://([^/]+)/(.+)', s3_path)
    if len(res) != 1 or len(res[0]) != 2:
        raise ValueError("Couldn't extract bucket and key from S3 bucket %s.", s3_path)
    return res[0][0], res[0][1]


def replace_with_local_fs(local_dir_name: str, s: str) -> str:
    return s.replace('s3://', local_dir_name + '/')


def replace_with_spark_fs(local_dir_name: str, s: str) -> str:
    return s.replace('s3://', 'file://' + local_dir_name + '/')


def remove_chars_pre_s3_path(s: str) -> str:
    index = s.find('s3://')
    if index == -1:
        raise ValueError("No `s3://` in string; %s", s)
    return s[index:]


def clean_and_replace_with_local_fs(local_dir_name: str, s3_path: str) -> (str, str):
    cleaned = remove_chars_pre_s3_path(s3_path)
    return cleaned, replace_with_local_fs(local_dir_name, cleaned)


def extract_s3_paths_from_step(emr_step: List[str]) -> List[Tuple[str, str]]:
    return [extract_s3_parts(remove_chars_pre_s3_path(v)) for v in emr_step if 's3://' in v]


def convert_s3_to_local_path(local_dir_name: str, emr_step: List[str]) -> List[str]:
    return [replace_with_spark_fs(local_dir_name, v) for v in emr_step]


class S3Response:
    def __init__(self, key, last_modified, size):
        self.key = key
        self.last_modified = last_modified
        self.size = size

    @staticmethod
    def from_response(d):
        return S3Response(d['Key'], d['LastModified'], d['Size'])


def is_s3_file(s3, bucket, key) -> bool:
    """
    Parameters
    ----------
    s3 : s3 client
    bucket : target s3 bucket
    key : target s3 key

    Returns
    -------

    a boolean of whether the key exists. The idea here being that if the key doesn't exist, then that is an output.
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404' and e.response['Error']['Message'] == 'Not Found':
            return False
        raise e


def get_files_from_s3(s3, local_dir_name: str, args: List[str]):
    s3_paths = extract_s3_paths_from_step(args)
    subdirs = [(b, k) for b, k in s3_paths if is_nonempty_s3_subdir(s3, b, k)]
    select_s3_files = process_s3_subdirs(s3, subdirs)
    s3_files = [(b, k) for b, k in s3_paths if is_s3_file(s3, b, k)]
    for b, k in s3_files + select_s3_files:
        process_s3_file(s3, local_dir_name, b, k)


def process_s3_subdirs(s3, subdirs: List[Tuple[str, str]]):
    return reduce(lambda x, y: x + process_s3_subdir(s3, y[0], y[1]), subdirs, [])


def process_s3_subdir(s3, bucket, key):
    keys = ls_s3(s3, bucket, key)
    return sample_list(bucket, keys)


def sample_list(bucket: str, s3_file_list: List[S3Response]) -> List[Tuple[str, str]]:
    """
    Parameters
    ----------
    bucket : The S3 bucket the s3_file_list came from
    s3_file_list : A list of S3 Keys

    Returns
    -------
    A sampled version of the list of S3 keys which has evenly distributed
    `updated_at` and the total size of all the files does not exceed
    configured size.

    TODO: This function
    """
    return [(bucket, r.key) for r in s3_file_list]


def process_s3_file(s3, local_dir_prefix: str, bucket: str, key: str):
    local_path = os.path.join(local_dir_prefix, bucket, key)
    pathlib.Path(os.path.dirname(local_path)).mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, local_path)


def is_nonempty_s3_subdir(s3, bucket: str, key: str) -> bool:
    if '*' in key:
        # If '*' in path then just return True as further processing needed
        return True
    key_ls = s3.list_objects(Bucket=bucket, Prefix=key)
    if 'Contents' not in key_ls:
        return False
    contents = key_ls['Contents']
    if len(contents) == 0:
        return False
    if len(contents) == 1 and contents[0]['Key'] == key:
        return False
    return True


def ls_s3(s3, bucket: str, key: str, delimiter='/') -> List[S3Response]:
    """
    Parameters
    ----------
    s3 : S3 client
    bucket : S3 bucket
    key : S3 Key
    delimiter : Delimiter between the paths

    Returns
    -------
    Returns a list of S3 keys that match the path. Simulates ls with wildcards for S3.
    """
    if '*' in key:
        subdirs = key.split(delimiter)
        prefix = delimiter.join(list(itertools.takewhile(lambda s: '*' not in s, subdirs)))
        keys = s3.list_objects(Bucket=bucket, Prefix=prefix)['Contents']
        return [S3Response.from_response(r) for r in keys if matches_prefix(subdirs, r['Key'].split(delimiter))]
    else:
        return [S3Response.from_response(r) for r in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']]


def matches_prefix(patterns: List[str], matchers: List[str]) -> bool:
    return all([wildcard_eq(pattern, match) for pattern, match in zip(patterns, matchers)])


def wildcard_eq(pattern: str, match: str) -> bool:
    if pattern == '*':
        return True
    pattern = re.escape(pattern).replace('\\*', '.*')
    return re.match(pattern, match) is not None
