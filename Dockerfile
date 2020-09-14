FROM python:3.7-slim as app

RUN mkdir /opt/localemr && cd /opt/localemr

WORKDIR /opt/localemr

COPY Pipfile .
COPY Pipfile.lock .

RUN pip install --no-cache-dir pipenv && pipenv install

COPY . .

CMD ["pipenv", "run", "python", "main.py"]

FROM app AS test

RUN pipenv install --dev
