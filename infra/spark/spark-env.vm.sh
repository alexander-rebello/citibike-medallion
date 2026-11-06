# Citi Bike standalone cluster (spark-vm = master + worker). Addresses are pinned: hostnames broke after Tailscale.
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_MASTER_HOST=192.168.26.92
export SPARK_MASTER_PORT=7077
export SPARK_LOCAL_IP=192.168.26.92
export SPARK_WORKER_CORES=8
export SPARK_WORKER_MEMORY=8g
