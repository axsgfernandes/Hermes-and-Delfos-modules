FROM amancevice/pandas:1.3.5
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
 && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN mkdir /app
ADD . /app
WORKDIR /app
RUN pip3 install -r requirements.txt

EXPOSE 5000
CMD ["python3", "app.py"]