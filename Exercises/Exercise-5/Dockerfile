FROM python:3.12-alpine

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN python3 -m pip install -r /app/requirements.txt

COPY . /app
