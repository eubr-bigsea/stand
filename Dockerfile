FROM ubuntu:16.04
MAINTAINER Vinicius Dias <viniciusvdias@dcc.ufmg.br>

ENV STAND_HOME /usr/local/stand
ENV STAND_CONFIG $STAND_HOME/conf/stand-config.yaml

RUN apt-get update && apt-get install -y  \
     python-pip \
   && rm -rf /var/lib/apt/lists/*

WORKDIR $STAND_HOME
COPY . $STAND_HOME
RUN pip install -r $STAND_HOME/requirements.txt

CMD ["/usr/local/stand/sbin/stand-daemon.sh", "startf"]
