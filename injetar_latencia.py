import subprocess
import time

def executar_comando_docker(container, comando):
    cmd = f"docker exec --user root {container} {comando}"
    try:
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"[OK] Comando executado em {container}")
    except subprocess.CalledProcessError as e:
        print(f"[ERRO] Falha ao executar em {container}: {e.stderr.decode().strip()}")

def aplicar_caos_rede(containers):
    print("\n INICIANDO INJEÇÃO DE CAOS NA REDE")
    for c in containers:
        # Limpa qualquer regra anterior para não acumular
        executar_comando_docker(c, "tc qdisc del dev eth0 root")
        
        # Adiciona 2000ms de delay médio com variação de 1990ms (gera a faixa de 10ms a 4000ms) e 5% de perda
        comando_tc = "tc qdisc add dev eth0 root netem delay 2000ms 1990ms loss 5%"
        print(f"[CAOS] Injetando latência (10ms-4000ms) e 5% de perda em: {c}...")
        executar_comando_docker(c, comando_tc)

if __name__ == "__main__":
    # Alvos do Módulo 2 (Sensores)
    sensores_alvo = ["sensor_a", "sensor_b"]
    aplicar_caos_rede(sensores_alvo)
