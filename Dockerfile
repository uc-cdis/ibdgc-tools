FROM python:3.8-slim-buster

COPY etl-requirements.txt ./etl-requirements.txt
RUN pip3 install --upgrade pip==20.1.*
RUN pip3 install -r etl-requirements.txt

# aws cli v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && /bin/rm -rf awscliv2.zip ./aws

COPY ./hailtable-etl /hailtable-etl
WORKDIR /hailtable-etl

# output logs while running job
ENV PYTHONUNBUFFERED=1

CMD [ "python3", "/covid19-etl/main.py" ]
