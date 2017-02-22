FROM python:2.7-onbuild
MAINTAINER Guilherme Maluf <guimalufb@gmail.com>

EXPOSE 3321
ENV STAND_CONFIG="./stand.yaml"
ENV PYTHONPATH="."
RUN chmod a+x "./run.sh"

CMD [ "./run.sh" ]
