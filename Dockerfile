FROM python:3.7.3-alpine3.9 as base

FROM base as pip_builder
RUN apk add --no-cache gcc musl-dev
COPY requirements.txt /
RUN pip install -r /requirements.txt

FROM base
LABEL maintainer="Vinicius Dias <viniciusvdias@dcc.ufmg.br>, Guilherme Maluf <guimaluf@dcc.ufmg.br>"

ENV STAND_HOME /usr/local/stand
ENV STAND_CONFIG $STAND_HOME/conf/stand-config.yaml

COPY --from=pip_builder /usr/local /usr/local

WORKDIR $STAND_HOME
COPY . $STAND_HOME

RUN pybabel compile -d $STAND_HOME/stand/i18n/locales

CMD ["/usr/local/stand/sbin/stand-daemon.sh", "docker"]
