from localemr.common import get_emr_version, LocalFakeStep


def test_clean_for_local_run():
    assert LocalFakeStep.clean_for_local_run([
        '/usr/bin/spark-submit',
        '--master', 'yarn',
        '--deploy-mode', 'cluster',
        '--num-executors', '256',
        '--executor-memory', '16G',
        '--class', 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
        '--name', 'test',
        '--conf', 'spark.driver.cores=1',
        '--conf', 'spark.yarn.maxAppAttempts=1',
        '--conf', 'spark.shuffle.service.enabled=true',
        's3://bucket/tmp/localemr/wc-spark.jar',
        's3n://bucket/key/2020-05/03/*/*.txt',
        's3a://bucket/tmp/localemr/output',
    ]) == [
        '/usr/bin/spark-submit',
        '--class', 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
        '--name', 'test',
        '--conf', 'spark.driver.cores=1',
        '--conf', 'spark.yarn.maxAppAttempts=1',
        '--conf', 'spark.shuffle.service.enabled=true',
        's3a://bucket/tmp/localemr/wc-spark.jar',
        's3a://bucket/key/2020-05/03/*/*.txt',
        's3a://bucket/tmp/localemr/output',
    ]


def test_get_emr_version():
    assert get_emr_version('emr-5.26.0') == '5.25.0'
    assert get_emr_version('emr-0.0.0') == '5.0.0'
    assert get_emr_version('emr-7.0.0') == '6.0.0'
