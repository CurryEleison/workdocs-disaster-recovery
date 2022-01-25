FROM python:3.9-slim

LABEL maintainer="curryeleison@gmail.com"

COPY workdocs_dr /app/workdocs_dr
COPY main.py /app
COPY restore.py /app
COPY LICENSE /app
COPY README.md /app
COPY Pipfile /app
COPY Pipfile.lock /app

WORKDIR /app

RUN pip install pipenv \
    && pipenv install --system --deploy

CMD ["python", "main.py"]
