FROM python:3.9-slim

LABEL maintainer="curryeleison@gmail.com"

COPY . /app
WORKDIR /app

RUN pip install pipenv \
    && pipenv install --system --deploy

CMD ["python", "main.py"]
