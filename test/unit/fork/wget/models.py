from localemr.fork.wget.models import change_spark_in_path


def test_change_spark_in_path():
    result = change_spark_in_path(
        '/opt/spark-2.3.1-bin-without-hadoop',
        '/opt/hadoop/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/spark-2.4.4-bin-without-hadoop'
    )

    assert result == '/opt/hadoop/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/spark-2.3.1-bin-without-hadoop'

    result = change_spark_in_path(
        '/opt/spark-2.4.4-bin-without-hadoop',
        '/opt/hadoop/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
    )

    assert result == '/opt/hadoop/bin:/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/spark-2.4.4-bin-without-hadoop'
