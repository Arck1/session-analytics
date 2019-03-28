FROM python:3.7

ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY requirements.txt requirements.txt
ADD ./*.json ./
COPY main.py main.py
COPY init.sql init.sql
COPY init.sh init.sh
#COPY fixjson.py fixjson.py
#RUN apt -y -f install mysql-client mysql-server
RUN apt-get update && apt-get install -y mysql-client
RUN pip install --no-cache-dir -r requirements.txt



