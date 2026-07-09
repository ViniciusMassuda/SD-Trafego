import subprocess
import time

def simular_queda_e_retorno(container_alvo, tempo_desligado=10):
    print(f"\n[CRÍTICO] Simulando queda abrupta do nó: {container_alvo} (docker kill)...")
    subprocess.run(f"docker kill {container_alvo}", shell=True)
    
    print(f"[AGUARDANDO] Mantendo o nó desligado por {tempo_desligado} segundos para testar o Heartbeat dos outros...")
    time.sleep(tempo_desligado)
    
    print(f"[RECUPERAÇÃO] Reiniciando o contêiner {container_alvo}...")
    subprocess.run(f"docker start {container_alvo}", shell=True)
    print(f"[OK] {container_alvo} voltou à vida.")

if __name__ == "__main__":
    # Exemplo derrubando o semaforo_1
    simular_queda_e_retorno("semaforo_1", tempo_desligado=12)
