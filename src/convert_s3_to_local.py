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


def extract_s3_files_from_step(local_dir_name: str, emr_step: List[str]) -> List[Tuple[str, str]]:
    return [clean_and_replace_with_local_fs(local_dir_name, v) for v in emr_step if 's3://' in v]


def convert_s3_to_local_path(local_dir_name: str, emr_step: List[str]) -> List[str]:
    return [replace_with_spark_fs(local_dir_name, v) for v in emr_step]

