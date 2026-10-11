import pytest

from spark.common.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("pytest")
    yield s
    s.stop()
