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
        ]
    }
}

WORKING_STEP = {
    'Name': 'EMR Job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/bin/spark-submit',
            '--deploy-mode', 'cluster',
            '--master', 'yarn',
            '--class', 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
            '--name', 'test',
            '--conf', 'spark.driver.cores=1',
            '--conf', 'spark.yarn.maxAppAttempts=1',
            'file:///tmp/localemr/word-count.jar',
            '/tmp/localemr/input.txt',
            '/tmp/localemr/output',
        ]
    }
}


S3_STEP = {
    'Name': 'EMR Job',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            '/usr/bin/spark-submit',
            '--deploy-mode', 'cluster',
            '--master', 'yarn',
            '--class', 'com.oreilly.learningsparkexamples.mini.scala.WordCount',
            '--name', 'test',
            '--conf', 'spark.driver.cores=1',
            '--conf', 'spark.yarn.maxAppAttempts=1',
            # '--conf', 'spark.jars.packages=org.apache.hadoop:hadoop-aws:2.8.5',
            # '--jars', "/root/.ivy2/jars/org.apache.hadoop_hadoop-aws-2.8.5.jar",
            # '--jars', 'spark.jars.packages=com.amazonaws:aws-java-sdk:1.10.34',
            # '--conf', 'spark.hadoop.fs.s3.access.key=TESTING',
            # '--conf', 'spark.hadoop.fs.s3.secret.key=TESTING',
            # '--conf', 'spark.hadoop.fs.s3.endpoint=http://s3:2000',
            # '--conf', 'spark.hadoop.fs.s3.connection.ssl.enabled=false',
            's3://bucket/tmp/localemr/word-count.jar',
            's3://bucket/key/2020-05/03/*/*.txt',
            's3://bucket/tmp/localemr/output',
        ]
    }
}
