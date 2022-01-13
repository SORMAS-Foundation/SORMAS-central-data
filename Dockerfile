FROM alpine:3.15

RUN apk update --no-cache && \
    apk upgrade --no-cache && \
    apk add --no-cache --upgrade postgresql14 postgresql14-dev py3-pip

COPY requirements.txt /root
RUN pip3 install -r /root/requirements.txt
COPY align_local_central.py /root
WORKDIR /root/
CMD [ "python3", "/root/align_local_central.py", "-H", "${host}", "-d", "${dbname}", "-u", "${username}", "-p", "${password}"]