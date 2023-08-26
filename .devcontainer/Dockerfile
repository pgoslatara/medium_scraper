FROM python:3.11

# Set working directory
RUN mkdir -p /data
WORKDIR /data

RUN apt update && \
    apt upgrade -y && \
    apt install git sudo -y

RUN pip install --upgrade pip

COPY . .
RUN pip install -e .
RUN spacy download en_core_web_sm
# RUN pre-commit install-hooks
