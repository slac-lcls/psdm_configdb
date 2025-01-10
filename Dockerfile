FROM docker.io/python:3.12.4

RUN apt-get update

# A lot of this logic is time based; make sure we have the right time zone.
ENV TZ="America/Los_Angeles"

RUN mkdir /work

WORKDIR /work

COPY requirements.txt /work

RUN pip install --no-cache-dir --upgrade -r /work/requirements.txt

# Get typedjson from the LCLS2 DAQ code
RUN git clone "https://github.com/slac-lcls/lcls2/" && \
    mkdir typed_json && \
    cd lcls2 && \
    git checkout 4.1.3 && \
    cp psdaq/psdaq/configdb/typed_json.py ../typed_json/ && \
    cd .. && \
    rm -rf lcls2

COPY ./src /work/


EXPOSE 5000

ENTRYPOINT ["./runscripts/run.sh"]