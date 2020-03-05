FROM ubuntu:latest

# TODO
RUN apt-get update -y && apt-get install python3 -y && \
    apt-get install curl python3-distutils -y && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py && \
    python3 -m pip install sklearn pandas
