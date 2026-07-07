FROM python:3.9-alpine

ENV LANG=C.UTF-8
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt influxdb

COPY main.py /app/main.py
COPY run.sh /app/run.sh

RUN chmod +x /app/run.sh

CMD ["/app/run.sh"]
