import socket
import threading
import json
import time
import argparse
import pika


NOS_CONFIG = {
    1: "192.168.140.11",
    2: "192.168.140.12",
    3: "192.168.140.13",
    4: "192.168.140.14",
}
PORTA_CONTROLE = 9999
TOTAL_NOS = len(NOS_CONFIG)
QUORUM_MINIMO = TOTAL_NOS // 2 + 1  

TIMEOUT_SOCKET = 2.0               
INTERVALO_MONITORAMENTO = 2.0      
TIMEOUT_ESPERA_COORDINATOR = 5.0   


class SemaforoNode:
    def __init__(self, meu_id, broker_host):
        self.meu_id = meu_id
        self.broker_host = broker_host
        self.pares = {i: ip for i, ip in NOS_CONFIG.items() if i != meu_id}

        self.lock = threading.Lock()
        self.estado = "FOLLOWER"        
        self.lider_atual = None
        self.alcancaveis = {i: False for i in self.pares}
        self.eleicao_em_andamento = False
        self.momento_ultima_eleicao = 0.0

    def iniciar_servidor_controle(self):
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(("0.0.0.0", PORTA_CONTROLE))
        servidor.listen(8)
        print(f"[semaforo_{self.meu_id}] Servidor de controle ativo na porta {PORTA_CONTROLE}.")

        while True:
            try:
                conexao, _ = servidor.accept()
                threading.Thread(target=self._tratar_conexao, args=(conexao,), daemon=True).start()
            except OSError as e:
                print(f"[semaforo_{self.meu_id}] Erro no servidor de controle: {e}")

    def _tratar_conexao(self, conexao):
        try:
            conexao.settimeout(TIMEOUT_SOCKET)
            dados = conexao.recv(4096)
            if not dados:
                return
            msg = json.loads(dados.decode().strip())
            self._processar_mensagem(msg, conexao)
        except (socket.timeout, TimeoutError, ConnectionResetError, json.JSONDecodeError, OSError) as e:
            print(f"[semaforo_{self.meu_id}] Falha ao tratar conexão de controle recebida: {e}")
        finally:
            try:
                conexao.close()
            except OSError:
                pass

    def _enviar_mensagem(self, destino_id, tipo, espera_resposta=False):
        ip = NOS_CONFIG.get(destino_id)
        if ip is None:
            return None
        try:
            with socket.create_connection((ip, PORTA_CONTROLE), timeout=TIMEOUT_SOCKET) as sock:
                sock.settimeout(TIMEOUT_SOCKET)
                payload = json.dumps({"tipo": tipo, "origem": self.meu_id}) + "\n"
                sock.sendall(payload.encode())
                if espera_resposta:
                    resposta = sock.recv(4096)
                    if resposta:
                        return json.loads(resposta.decode().strip())
                    return None
            return {"tipo": "ACK"}
        except (socket.timeout, TimeoutError, ConnectionRefusedError, ConnectionResetError, OSError):
            return None

    def _processar_mensagem(self, msg, conexao):
        tipo = msg.get("tipo")
        origem = msg.get("origem")

        if tipo == "PING":
            resposta = json.dumps({"tipo": "PONG", "origem": self.meu_id})
            conexao.sendall(resposta.encode())

        elif tipo == "ELECTION":
            if self.meu_id > origem:
                resposta = json.dumps({"tipo": "OK", "origem": self.meu_id})
                conexao.sendall(resposta.encode())
                print(f"[semaforo_{self.meu_id}] Recebi ELECTION de semaforo_{origem} (prioridade menor). "
                      f"Respondendo OK e iniciando minha própria eleição.")
                threading.Thread(target=self.iniciar_eleicao, daemon=True).start()

        elif tipo == "COORDINATOR":
            with self.lock:
                self.lider_atual = origem
                self.estado = "LEADER" if origem == self.meu_id else "FOLLOWER"
            print(f"[semaforo_{self.meu_id}] >>> NOVO LÍDER ANUNCIADO: semaforo_{origem} "
                  f"| Meu estado agora: {self.estado} <<<")

    def _contar_nos_visiveis(self):
        alcancaveis_agora = {}
        for pid in self.pares:
            resposta = self._enviar_mensagem(pid, "PING", espera_resposta=True)
            alcancaveis_agora[pid] = bool(resposta and resposta.get("tipo") == "PONG")
        with self.lock:
            self.alcancaveis = alcancaveis_agora
        return 1 + sum(alcancaveis_agora.values())  # +1 = eu mesmo

    def monitorar_quorum(self):
        while True:
            tinha_quorum_antes = self.estado != "SEGURANCA"
            nos_visiveis = self._contar_nos_visiveis()

            if nos_visiveis < QUORUM_MINIMO:
                with self.lock:
                    era_lider = self.estado == "LEADER"
                    self.estado = "SEGURANCA"
                    self.lider_atual = None
                if tinha_quorum_antes:
                    extra = "Abandonando a liderança. " if era_lider else ""
                    print(f"[semaforo_{self.meu_id}] [QUORUM PERDIDO] Vejo {nos_visiveis}/{TOTAL_NOS} nós "
                          f"(mínimo exigido: {QUORUM_MINIMO}). {extra}"
                          f"Entrando em MODO DE SEGURANÇA — recusando ser ou seguir um líder.")

            else:
                if not tinha_quorum_antes:
                    print(f"[semaforo_{self.meu_id}] [QUORUM RECUPERADO] Vejo {nos_visiveis}/{TOTAL_NOS} nós. "
                          f"Saindo do modo de segurança e disparando nova eleição.")
                    with self.lock:
                        self.estado = "FOLLOWER"
                    threading.Thread(target=self.iniciar_eleicao, daemon=True).start()

                elif self.estado == "FOLLOWER" and self.lider_atual is not None:
                    with self.lock:
                        lider_alcancavel = self.alcancaveis.get(self.lider_atual, False)
                    if not lider_alcancavel:
                        print(f"[semaforo_{self.meu_id}] [FALHA DETECTADA] O líder atual "
                              f"(semaforo_{self.lider_atual}) tornou-se inalcançável. Convocando nova eleição.")
                        with self.lock:
                            self.lider_atual = None
                        threading.Thread(target=self.iniciar_eleicao, daemon=True).start()

                elif self.estado == "FOLLOWER" and self.lider_atual is None:
                    if time.time() - self.momento_ultima_eleicao > TIMEOUT_ESPERA_COORDINATOR:
                        threading.Thread(target=self.iniciar_eleicao, daemon=True).start()

            time.sleep(INTERVALO_MONITORAMENTO)

    def iniciar_eleicao(self):
        with self.lock:
            if self.eleicao_em_andamento:
                return
            self.eleicao_em_andamento = True
            self.momento_ultima_eleicao = time.time()

        nos_visiveis = self._contar_nos_visiveis()
        if nos_visiveis < QUORUM_MINIMO:
            print(f"[semaforo_{self.meu_id}] Sem quórum ({nos_visiveis}/{TOTAL_NOS}, mínimo {QUORUM_MINIMO}). "
                  f"Abortando eleição — modo de segurança.")
            with self.lock:
                self.estado = "SEGURANCA"
                self.lider_atual = None
                self.eleicao_em_andamento = False
            return

        print(f"[semaforo_{self.meu_id}] --- Iniciando ELEIÇÃO (Bully) --- ({nos_visiveis}/{TOTAL_NOS} nós visíveis)")
        superiores = [pid for pid in self.pares if pid > self.meu_id]
        respostas_ok = []
        for pid in superiores:
            resposta = self._enviar_mensagem(pid, "ELECTION", espera_resposta=True)
            if resposta and resposta.get("tipo") == "OK":
                respostas_ok.append(pid)
                print(f"[semaforo_{self.meu_id}] semaforo_{pid} (prioridade maior) respondeu OK.")

        if respostas_ok:
            print(f"[semaforo_{self.meu_id}] Nó(s) de maior prioridade vivo(s): {respostas_ok}. "
                  f"Aguardando anúncio de COORDINATOR.")
            with self.lock:
                if self.estado != "LEADER":
                    self.estado = "FOLLOWER"
                self.eleicao_em_andamento = False
            return

        nos_visiveis = self._contar_nos_visiveis()
        if nos_visiveis < QUORUM_MINIMO:
            print(f"[semaforo_{self.meu_id}] Quórum perdido durante a eleição ({nos_visiveis}/{TOTAL_NOS}). "
                  f"Abortando autoproclamação — modo de segurança.")
            with self.lock:
                self.estado = "SEGURANCA"
                self.lider_atual = None
                self.eleicao_em_andamento = False
            return

        print(f"[semaforo_{self.meu_id}] Nenhum nó de prioridade maior respondeu. Autoproclamando-me LÍDER.")
        with self.lock:
            self.estado = "LEADER"
            self.lider_atual = self.meu_id
        for pid in self.pares:
            self._enviar_mensagem(pid, "COORDINATOR", espera_resposta=False)
        print(f"[semaforo_{self.meu_id}] >>> EU SOU O LÍDER (semaforo_{self.meu_id}) <<<")
        with self.lock:
            self.eleicao_em_andamento = False

    def consumir_eventos_trafego(self):
        parametros = pika.ConnectionParameters(
            host=self.broker_host,
            heartbeat=0,
            blocked_connection_timeout=300,
        )
        while True:
            try:
                conexao = pika.BlockingConnection(parametros)
                canal = conexao.channel()
                canal.exchange_declare(exchange='trafego_events', exchange_type='fanout')

                fila = canal.queue_declare(queue='', exclusive=True)
                nome_fila = fila.method.queue
                canal.queue_bind(exchange='trafego_events', queue=nome_fila)

                def callback(ch, method, properties, body):
                    evento = json.loads(body)
                    with self.lock:
                        sou_lider = self.estado == "LEADER"
                        estado_atual = self.estado
                    if sou_lider:
                        print(f"[semaforo_{self.meu_id}][LÍDER] Evento de {evento['sensor_id']} "
                              f"(L={evento['lamport_clock']}): {evento['evento']} -> decisão de controle aplicada.")
                    else:
                        print(f"[semaforo_{self.meu_id}][{estado_atual}] Evento recebido de {evento['sensor_id']} "
                              f"(L={evento['lamport_clock']}) — não sou coordenador, apenas observando.")

                canal.basic_consume(queue=nome_fila, on_message_callback=callback, auto_ack=True)
                canal.start_consuming()

            except pika.exceptions.AMQPConnectionError:
                print(f"[semaforo_{self.meu_id}] Aguardando o broker RabbitMQ iniciar...")
                time.sleep(2)
            except (TimeoutError, ConnectionRefusedError, ConnectionResetError, OSError) as e:
                print(f"[semaforo_{self.meu_id}] Conexão com o broker perdida ({e}). Tentando reconectar...")
                time.sleep(2)

    def iniciar(self):
        print(f"[semaforo_{self.meu_id}] Iniciado. Pares conhecidos: {self.pares}. "
              f"Quórum mínimo: {QUORUM_MINIMO}/{TOTAL_NOS}.")

        threading.Thread(target=self.iniciar_servidor_controle, daemon=True).start()
        time.sleep(2) 
        threading.Thread(target=self.monitorar_quorum, daemon=True).start()
        threading.Thread(target=self.iniciar_eleicao, daemon=True).start()

        self.consumir_eventos_trafego()  


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, type=int, choices=list(NOS_CONFIG.keys()))
    parser.add_argument("--broker", required=True)
    args = parser.parse_args()

    no = SemaforoNode(args.id, args.broker)
    no.iniciar()