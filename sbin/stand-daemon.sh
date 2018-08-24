#!/usr/bin/env sh

# This script controls the stand server daemon initialization, status reporting
# and termination
# TODO: rotate logs

usage="Usage: stand-daemon.sh (start|docker|stop|status)"

# this sript requires the command parameter
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# parameter option
cmd_option=$1

# if unset set stand_home to stand root dir, without ./sbin
export STAND_HOME=${STAND_HOME:-$(cd $(dirname $0)/..; pwd)}
echo ${STAND_HOME}

# get log directory
export STAND_LOG_DIR=${STAND_LOG_DIR:-${STAND_HOME}/logs}

# get pid directory
export STAND_PID_DIR=${STAND_PID_DIR:-/var/run/}

mkdir -p ${STAND_PID_DIR} ${STAND_LOG_DIR}

# log and pid files
log=${STAND_LOG_DIR}/stand-server-${USER}-${HOSTNAME}.out
pid=${STAND_PID_DIR}/stand-server-${USER}.pid

case $cmd_option in

  (start)
    # set python path
    PYTHONPATH=${STAND_HOME}:${PYTHONPATH} \
      python ${STAND_HOME}/stand/manage.py \
      db upgrade

    PYTHONPATH=${STAND_HOME}:${PYTHONPATH} nohup -- \
      python ${STAND_HOME}/stand/runner/stand_server.py \
      -c ${STAND_HOME}/conf/stand-config.yaml \
      >> $log 2>&1 < /dev/null &
    stand_server_pid=$!

    # persist the pid
    echo $stand_server_pid > $pid

    echo "Stand server started, logging to $log (pid=$stand_server_pid)"
    ;;

  (docker)
    trap "$0 stop" SIGINT SIGTERM
    # set python path
    PYTHONPATH=${STAND_HOME}:${PYTHONPATH} \
      python ${STAND_HOME}/stand/manage.py \
      db upgrade
    # check if the db migration was successful
    if [ $? -eq 0 ]
    then
      echo "DB migration: successful"
    else
      echo "Error on DB migration"
      exit 1
    fi

    PYTHONPATH=${STAND_HOME}:${PYTHONPATH} \
      python ${STAND_HOME}/stand/runner/stand_server.py \
      -c ${STAND_HOME}/conf/stand-config.yaml &
    stand_server_pid=$!

    # persist the pid
    echo $stand_server_pid > $pid

    echo "Stand server started, logging to $log (pid=$stand_server_pid)"
    wait
    ;;

  (stop)
    if [ -f $pid ]; then
      TARGET_ID=$(cat $pid)
      if [[ $(ps -p ${TARGET_ID} -o comm=) =~ "python" ]]; then
        echo "stopping stand server, user=${USER}, hostname=${HOSTNAME}"
        (pkill -SIGTERM -P ${TARGET_ID} && \
          kill -SIGTERM ${TARGET_ID} && \
          rm -f $pid)
      else
        echo "no stand server to stop"
      fi
    else
      echo "no stand server to stop"
    fi
    ;;

  (status)
    if [ -f $pid ]; then
      TARGET_ID=$(cat $pid)
      if [[ $(ps -p ${TARGET_ID} -o comm=) =~ "python" ]]; then
        echo "stand server is running (pid=${TARGET_ID})"
        exit 0
      else
        echo "$pid file is present (pid=${TARGET_ID}) but stand server not running"
        exit 1
      fi
    else
      echo stand server not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
