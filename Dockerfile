FROM python:3.9
WORKDIR /parent
COPY ./requirements.txt /parent/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /parent/requirements.txt
COPY ./mlmetrics /parent/mlmetrics
RUN chmod u+x mlmetrics/exporter.py
ENTRYPOINT ["/parent/mlmetrics/exporter.py"]
