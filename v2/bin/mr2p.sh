#!/bin/bash
# Description:  start or stop the spiderservice
# Usage:        spiderservice [start|stop|reload|restart]
# Author:       intsig
root_dir=$(cd "$(dirname "$0")";pwd)
pid_path="${root_dir}/../data/mr2p.pid"
pro_path="${root_dir}/../mr2p.py"

case "$1" in

start)
    echo "Starting spiderservice service..."
    python $pro_path >/tmp/mr2p_push.log 2>&1&
    echo "spiderservice started..."
;;
stop)
    echo "Stoping spiderservice service..."
    while read line
    do
    echo "killing the process of id:"$line
    kill -9 $line
    done < $pid_path
    rm -rf $pid_path
    echo "spiderservice stoped..."
;;
reload|restart)
    $0 stop
    $0 start
;;
*)
    echo "Usage: spiderservice [start|stop|reload|restart]"
    exit 1
esac
exit 0



