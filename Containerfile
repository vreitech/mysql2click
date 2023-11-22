FROM docker.io/python:3.11.5-slim-bullseye as builder
WORKDIR /usr/src/myapp
COPY requirements.txt datetimecolumn.py.patch ./
RUN apt-get -yq update && apt-get -yq install patch
RUN pip3 install --no-cache-dir -r requirements.txt && patch /usr/local/lib/python3.11/site-packages/asynch/proto/columns/datetimecolumn.py ./datetimecolumn.py.patch

FROM builder
WORKDIR /usr/src/myapp
COPY mysql2click.py ./

