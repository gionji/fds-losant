FROM python:alpine
RUN mkdir /telemetry
RUN mkdir -p /libs/utils
RUN pip install paho-mqtt losant-mqtt
COPY ../libs/utils /libs/utils
WORKDIR /telemetry
