FROM apache/airflow:2.3.4

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y vim default-jdk

COPY requirements.txt /opt/airflow/requirements.txt
RUN chown -R airflow:root /opt/airflow/requirements.txt

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

RUN pip install -r requirements.txt
