FROM python:3.8-slim-buster

COPY etl-requirements.txt ./etl-requirements.txt
RUN pip3 install --upgrade pip==20.1.*
RUN pip3 install -r etl-requirements.txt
RUN python3 -m pip install awscli --upgrade

COPY ./hailtable-etl /hailtable-etl
WORKDIR /hailtable-etl

# output logs while running job
ENV PYTHONUNBUFFERED=1

CMD [ "python3", "/hailtabl-etl/hail_to_es.py" ]
