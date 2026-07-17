import pika
import json
import time
import threading
import random
import argparse

class SensorNode:
    """Representa um sensor do sistema de tráfego.

    O nodo tem dois papéis simples:
    1. publica eventos de tráfego no broker;
    2. tenta ajustar seu relógio local usando um protocolo de sincronização.

    Em sistemas distribuídos, isso é meio confuso porque cada processo vê
    o tempo de forma diferente, e o relógio local não é totalmente confiável.
    """

    def __init__(self, sensor_id, broker_host):
        self.sensor_id = sensor_id
        self.broker_host = broker_host
        self.clock = 0
        self.lock = threading.Lock()
        # Deriva artificial simulada para deixar o relógio local "um pouco diferente".
        self.drift_artificial = random.uniform(-10.0, 10.0)
        self.time_offset = 0.0

    def get_local_time(self):
        # Relógio local do sensor, com uma deriva artificial só para simular
        # o que acontece em um sistema real: cada nó tem seu próprio relógio.
        return time.time() + self.drift_artificial

    def get_synced_time(self):
        # Quando o sensor já conseguiu estimar o offset, ele usa o relógio
        # "sincronizado" para timestampar os eventos.
        return self.get_local_time() + self.time_offset

    def get_connection_and_channel(self):
        """
        Cria uma conexão isolada.
        Desativa o heartbeat (heartbeat=0) para suportar a latência de 4000ms
        sem que o RabbitMQ feche a conexão achando que o nó morreu.
        """
        parameters = pika.ConnectionParameters(
            host=self.broker_host,
            heartbeat=0, 
            blocked_connection_timeout=300
        )
        while True:
            try:
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                channel.exchange_declare(exchange='trafego_events', exchange_type='fanout')
                return connection, channel
            except pika.exceptions.AMQPConnectionError:
                print(f"[{self.sensor_id}] Aguardando Broker iniciar...")
                time.sleep(2)

    def start(self):
        print(f"[{self.sensor_id}] Iniciado. Relógio Lógico: {self.clock} | Deriva Inicial: {self.drift_artificial:.2f}s")
        
        t_consume = threading.Thread(target=self.consume)
        t_produce = threading.Thread(target=self.produce)
        t_sync = threading.Thread(target=self.sync_time_cristian)
        
        t_consume.start()
        t_produce.start()
        t_sync.start()
        
        t_consume.join()
        t_produce.join()
        t_sync.join()

    def sync_time_cristian(self):
        # Aqui o sensor fica enviando pedidos de tempo para o broker.
        # O semáforo, quando é líder, responde com o tempo do servidor.
        # Depois disso, o sensor tenta fazer uma correção de relógio.
        while True:
            try:
                connection, channel = self.get_connection_and_channel()
                while True:
                    time.sleep(5)
                    req = {
                        "tipo": "TIME_REQUEST",
                        "sensor_id": self.sensor_id,
                        "t0": self.get_local_time()
                    }
                    channel.basic_publish(exchange='trafego_events', routing_key='', body=json.dumps(req))
            except Exception as e:
                print(f"[{self.sensor_id}] Falha na sincronização de tempo: {e}. Tentando reconectar...")
                time.sleep(2)

    def consume(self):
        while True:
            try:
                connection, channel = self.get_connection_and_channel()
                
                result = channel.queue_declare(queue='', exclusive=True)
                queue_name = result.method.queue
                channel.queue_bind(exchange='trafego_events', queue=queue_name)

                def callback(ch, method, properties, body):
                    msg = json.loads(body)
                    if msg.get("tipo") == "TIME_RESPONSE" and msg.get("sensor_id") == self.sensor_id:
                        # Essa parte é o coração da sincronização: o sensor calcula
                        # a metade do RTT e usa isso como estimativa do tempo do servidor.
                        # Não é perfeito, mas é um jeito bem comum de aproximar relógios.
                        t1 = self.get_local_time()
                        t0 = msg["t0"]
                        t_server = msg["t_server"]
                        rtt = t1 - t0
                        estimated_server_time = t_server + (rtt / 2)
                        self.time_offset = estimated_server_time - t1
                        print(f"[{self.sensor_id}] [CRISTIAN] Tempo sincronizado. Offset aplicado = {self.time_offset:.3f} (RTT={rtt:.3f}s)")
                        return
                    
                    if msg.get("tipo") in ["TIME_REQUEST", "TIME_RESPONSE"]:
                        return
                        
                    if msg.get('sensor_id') != self.sensor_id and 'lamport_clock' in msg:
                        with self.lock:
                            old_clock = self.clock
                            self.clock = max(self.clock, msg['lamport_clock']) + 1
                            print(f"[{self.sensor_id}] Notificado por {msg['sensor_id']}. Sincronizou: L={old_clock} -> L={self.clock}")

                channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
                channel.start_consuming()
            except Exception as e:
                print(f"[{self.sensor_id}] Erro na conexão de consumo: {e}. Tentando reconectar...")
                time.sleep(2)

    def produce(self):
        eventos = ["Veículo detectado", "Bloqueio na via", "Semáforo ignorado"]
        while True:
            try:
                connection, channel = self.get_connection_and_channel()
                while True:
                    time.sleep(random.uniform(2, 6))
                    with self.lock:
                        self.clock += 1
                        msg = {
                            "tipo": "evento",
                            "sensor_id": self.sensor_id,
                            "evento": random.choice(eventos),
                            "lamport_clock": self.clock,
                            "timestamp_real": self.get_synced_time()
                        }
                    channel.basic_publish(exchange='trafego_events', routing_key='', body=json.dumps(msg))
                    print(f"[{self.sensor_id}] Disparou Evento: {msg['evento']} (L={self.clock}, T_sync={msg['timestamp_real']:.2f})")
            except Exception as e:
                print(f"[{self.sensor_id}] Erro na publicação: {e}. Tentando reconectar...")
                time.sleep(2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--broker", required=True)
    args = parser.parse_args()
    
    sensor = SensorNode(args.id, args.broker)
    sensor.start()