FROM datamechanics/spark:3.1-latest
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

ADD . /EPICMIXGUESTGPSDATA
RUN pip install .