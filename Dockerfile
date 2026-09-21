FROM iqdamshidqi/airflow-pyspark:2.8.1
USER airflow
RUN python -m pip install --no-cache-dir openpyxl