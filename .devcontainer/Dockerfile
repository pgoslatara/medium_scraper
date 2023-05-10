FROM python:3.11

RUN apt update && \
    apt upgrade -y && \
    apt install git sudo -y

RUN pip install --upgrade pip

COPY . .
RUN pip install -e .
# RUN pre-commit install-hooks
