#!/bin/bash
echo "[CAOS] Removendo barreiras de rede e restaurando a comunicação..."

docker exec --privileged semaforo_1 iptables -F
docker exec --privileged semaforo_2 iptables -F
docker exec --privileged semaforo_3 iptables -F
docker exec --privileged semaforo_4 iptables -F

echo "[SUCESSO] Rede unificada novamente."
