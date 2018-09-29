FROM ubuntu:latest
WORKDIR /app
RUN apt-get update -y && \
    apt-get install -y mongodb && \
    apt-get install -y python3
COPY backup.py  .
ENTRYPOINT ["python", "backup.py"]