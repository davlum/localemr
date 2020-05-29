FROM python:3.8-slim

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir /opt/localemr

WORKDIR /opt/localemr

COPY . .

CMD ["python", "main.py"]
