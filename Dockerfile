FROM python:3.8-slim

USER root

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir /home/app

WORKDIR /home/app

COPY . .

RUN mkdir /tmp/localemr && \
    mv test/fixtures/word-count.jar /tmp/localemr/ && \
    mv test/fixtures/input.txt /tmp/localemr/

CMD ["python", "main.py"]
