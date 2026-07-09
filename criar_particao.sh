#!/bin/bash
echo "========================================================="
echo "  INICIANDO PARTIÇÃO DE REDE: 50% VS 50% (SPLIT-BRAIN)   "
echo "========================================================="

# Bloqueia comunicação vinda de semaforo_3 e semaforo_4 dentro de 1 e 2
docker exec --privileged semaforo_1 iptables -A INPUT -s 192.168.140.13 -j DROP
docker exec --privileged semaforo_1 iptables -A INPUT -s 192.168.140.14 -j DROP
docker exec --privileged semaforo_2 iptables -A INPUT -s 192.168.140.13 -j DROP
docker exec --privileged semaforo_2 iptables -A INPUT -s 192.168.140.14 -j DROP

# Bloqueia comunicação vinda de semaforo_1 e semaforo_2 dentro de 3 e 4
docker exec --privileged semaforo_3 iptables -A INPUT -s 192.168.140.11 -j DROP
docker exec --privileged semaforo_3 iptables -A INPUT -s 192.168.140.12 -j DROP
docker exec --privileged semaforo_4 iptables -A INPUT -s 192.168.140.11 -j DROP
docker exec --privileged semaforo_4 iptables -A INPUT -s 192.168.140.12 -j DROP

echo "[SUCESSO] Partição criada!"
