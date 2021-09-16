FROM osgeo/gdal:ubuntu-small-3.3.0

RUN apt-get update && apt-get -y install gnupg lsb-release

RUN export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s` \
    && echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | tee /etc/apt/sources.list.d/gcsfuse.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

RUN apt-get update && apt-get -y install gcsfuse python3-pip parallel

COPY requirements.txt /root
RUN python3 -m pip install -r /root/requirements.txt

RUN mkdir -p /usr/share/gcs/data

ENV PATH $PATH:/root/scripts

COPY ./scripts/export_to_gcs.sh ./scripts/geotif_to_bqparquet.py /root/scripts/

WORKDIR /root/scripts