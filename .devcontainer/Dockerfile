FROM python:3.10.2

RUN apt update && \
    apt upgrade -y && \
    apt install -y \
        git \
        sudo

RUN pip install --upgrade pip

ARG USERNAME=nonroot
RUN groupadd --gid 1000 $USERNAME &&  useradd --uid 1000 --gid 1000 -m $USERNAME

ENV PATH="/home/${USERNAME}/.local/bin:${PATH}"
USER $USERNAME

COPY --chown=nonroot:1000 requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt
    
