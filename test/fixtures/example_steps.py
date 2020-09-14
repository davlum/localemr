MAX_WAIT = 30

EXAMPLE_STEP = {
    'Name': 'EMR Job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/bin/spark-submit',
            '--deploy-mode', 'cluster',
            '--master', 'yarn',
            '--class', 'com.company.org.Jar',
            '--name', 'test',
            '--num-executors', '256',
            '--driver-memory', '4G',
            '--executor-memory', '30G',
            '--conf', 'spark.driver.cores=1',
            '--conf', 'spark.yarn.maxAppAttempts=1',
            '/tmp/example-bucket/artifacts/jar-with-dependencies.jar',
            '--output-path=/ccpa/delete',
            '--partitions=512',
            '--final-output-concurrency=256',
            '--graphite-port=2003',
            '--max-age-in-days=180',
        ],
    },
}

S3_STEP = {
    'Name': 'EMR Job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/bin/spark-submit',
            '--master', 'yarn',
            '--class', 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
            '--name', 'test',
            '--conf', 'spark.driver.cores=1',
            '--conf', 'spark.yarn.maxAppAttempts=1',
            '--conf', 'spark.shuffle.service.enabled=true',
            's3a://bucket/tmp/localemr/wc-spark.jar',
            's3a://bucket/key/2020-05/03/*/*.txt',
            's3a://bucket/tmp/localemr/output',
        ],
    },
}

MAPREDUCE_STEP = {
    'Name': 'Test Jar step',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': '/opt/hadoop/wc-mapreduce.jar',
        'Args': ['s3a://bucket/user/joe/wordcount/input', 's3a://bucket/user/joe/wordcount/output'],
        'MainClass': 'org.myorg.WordCount',
    },
}
