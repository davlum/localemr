FROM python:3.8

USER root

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir /home/app

WORKDIR /home/app

COPY . .

CMD ["python", "src/emr.py"]