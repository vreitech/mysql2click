FROM docker.io/python:3.11.5-slim-bullseye as builder
WORKDIR /usr/src/myapp
COPY requirements.txt datetimecolumn.py.patch ./
RUN apt-get -yqq update \
  && DEBIAN_FRONTEND=noninteractive apt-get -yqq install patch \
  && pip3 install --no-cache-dir -r requirements.txt \
  && patch /usr/local/lib/python3.11/site-packages/asynch/proto/columns/datetimecolumn.py ./datetimecolumn.py.patch

FROM docker.io/python:3.11.5-slim-bullseye
WORKDIR /usr/src/myapp
COPY requirements.txt ./
RUN apt-get -yqq update \
  && DEBIAN_FRONTEND=noninteractive apt-get -yqq install tzdata \
  && pip3 install --no-cache-dir -r requirements.txt
COPY --from=builder /usr/local/lib/python3.11/site-packages/asynch/proto/columns/datetimecolumn.py /usr/local/lib/python3.11/site-packages/asynch/proto/columns/datetimecolumn.py
COPY mysql2click.py ./

