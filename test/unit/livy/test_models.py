from localemr.exec.livy.backend import transform_emr_step_to_livy_req
from localemr.common import LocalFakeStep


def test_clean_for_local_run():
    args = [
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
        '--conf', 'spark.driver.extraJavaOptions=-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC',
        '--conf', 'spark.executor.extraJavaOptions=-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC',
        's3://bucket/tmp/localemr/wc-spark.jar',
        's3n://bucket/key/2020-05/03/*/*.txt',
        's3a://bucket/tmp/localemr/output',
    ]
    step = LocalFakeStep('test', 'test', 'test', state='RUNNING', jar='command-runner.jar', args=args)
    assert transform_emr_step_to_livy_req(step).to_dict() == {
        'className': 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
        'conf': {
            'spark.app.name': 'test',
            'spark.yarn.maxAppAttempts': '1',
            'spark.shuffle.service.enabled': 'true',
        },
        'file': 's3a://bucket/tmp/localemr/wc-spark.jar',
        'args': [
            's3a://bucket/key/2020-05/03/*/*.txt',
            's3a://bucket/tmp/localemr/output',
        ],
    }
