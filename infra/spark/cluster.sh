#!/usr/bin/env bash
# Control the two-node Spark standalone cluster. Run on spark-vm.
#
#   infra/spark/cluster.sh deploy   # install config + systemd units on both nodes (enables start at boot)
#   infra/spark/cluster.sh start | stop | restart
#   infra/spark/cluster.sh status   # service state on both nodes + workers the master sees
set -euo pipefail

VM=192.168.26.92
CALC=192.168.26.96
HERE=$(cd "$(dirname "$0")" && pwd)
calc() { ssh -o BatchMode=yes ubuntu@"$CALC" "$@"; }

case "${1:-status}" in
  deploy)
    sudo install -m 644 "$HERE/spark-env.vm.sh" /opt/spark/conf/spark-env.sh
    sudo install -m 644 "$HERE"/systemd/spark-master.service "$HERE"/systemd/spark-worker.service /etc/systemd/system/
    sudo systemctl daemon-reload && sudo systemctl enable spark-master spark-worker
    scp -q -o BatchMode=yes "$HERE/spark-env.calc.sh" "$HERE/systemd/spark-worker.service" ubuntu@"$CALC":/tmp/
    calc 'sudo install -m 644 /tmp/spark-env.calc.sh /opt/spark/conf/spark-env.sh &&
          sudo install -m 644 /tmp/spark-worker.service /etc/systemd/system/ &&
          sudo systemctl daemon-reload && sudo systemctl enable spark-worker'
    echo "deployed; run: $0 start"
    ;;
  start)
    sudo systemctl start spark-master
    sudo systemctl start spark-worker
    calc 'sudo systemctl start spark-worker'
    ;;
  stop)
    calc 'sudo systemctl stop spark-worker'
    sudo systemctl stop spark-worker spark-master
    ;;
  restart) "$0" stop; "$0" start ;;
  status)
    echo "spark-vm  master=$(systemctl is-active spark-master)  worker=$(systemctl is-active spark-worker)"
    echo "calc      worker=$(calc 'systemctl is-active spark-worker')"
    curl -s --max-time 5 "http://$VM:8080/json/" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print("master", d["status"], d["url"])
for w in d["workers"]:
    print("  worker", w["host"], w["cores"], "cores", w["memory"], "MB", w["state"])' \
      || echo "master not reachable on $VM:8080"
    ;;
  *) echo "usage: $0 deploy|start|stop|restart|status" >&2; exit 2 ;;
esac
