###
# USAGE: docker build -t central_aligner .
# RUN: docker run -it --env-file .env  -v "$(pwd)/out:/srv"  central_aligner
FROM alpine:3.15

RUN apk update --no-cache && \
    apk upgrade --no-cache && \
    apk add --no-cache --upgrade postgresql14 py3-pip

COPY requirements.txt /root
RUN pip3 install -r /root/requirements.txt
COPY align_local_central.py /root
WORKDIR /root/
CMD [ "python3", "/root/align_local_central.py" ]