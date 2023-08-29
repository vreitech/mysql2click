FROM docker.io/python:3.11.5-slim-bullseye
WORKDIR /usr/src/myapp
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt
COPY . .

