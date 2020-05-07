FROM python:3.8

USER root

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir /home/app

WORKDIR /home/app

COPY . .

RUN mkdir /tmp/files && \
    mv test/fixtures/word-count.jar /tmp/files/ && \
    mv test/fixtures/input.txt /tmp/files/

CMD ["python", "main.py"]
