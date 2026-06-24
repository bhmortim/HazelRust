#!/bin/bash
set -e
cd /home/ec2-user
LIC=$(cat /home/ec2-user/hz/license.key)
for i in 1 2 3; do
  port=$((5700 + i))
  cat > hz/hz$i.yaml <<YAML
hazelcast:
  license-key: <REDACTED-read-from-hz/license.key>
  cluster-name: dev
  network:
    port:
      auto-increment: false
      port: $port
    join:
      auto-detection:
        enabled: false
      multicast:
        enabled: false
      tcp-ip:
        enabled: true
        member-list:
          - 127.0.0.1:5701
          - 127.0.0.1:5702
          - 127.0.0.1:5703
    public-address: 127.0.0.1:$port
  cp-subsystem:
    cp-member-count: 3
    group-size: 3
  serialization:
    data-serializable-factories:
      - factory-id: 1
        class-name: com.hazelcast.test.JavaParityFactory
  map:
    "java_parity_test_load_all":
      map-store:
        enabled: true
        class-name: com.hazelcast.test.JavaParityMapStore
        initial-mode: LAZY
    "java_parity_test_load_all_keys":
      map-store:
        enabled: true
        class-name: com.hazelcast.test.JavaParityMapStore
        initial-mode: LAZY
    "java_parity_test_*":
      statistics-enabled: true
      per-entry-stats-enabled: true
YAML
  sudo docker rm -f hz$i >/dev/null 2>&1 || true
  sudo docker run -d --name hz$i --network host \
    -v /home/ec2-user/hz/hz$i.yaml:/opt/hazelcast/hz.yaml \
    -v /home/ec2-user/hz/a4classes.jar:/opt/hazelcast/lib/a4classes.jar \
    -e JAVA_OPTS="-Dhazelcast.config=/opt/hazelcast/hz.yaml -Xms512m -Xmx1g" \
    hazelcast/hazelcast-enterprise:5.7.0 >/dev/null
  echo "started hz$i :$port"
done
