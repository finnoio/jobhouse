FROM apache/airflow:2.10.5-python3.11

WORKDIR /opt/airflow
ADD ./requirements.txt /opt/airflow/requirements.txt

RUN python -m pip install --upgrade pip wheel
USER airflow

RUN pip install -r requirements.txt
ENV PYTHONPATH="src:${PYTHONPATH}"