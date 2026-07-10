import pika
import json
import time
import threading
import random
import argparse

class SensorNode:
    def __init__(self, sensor_id, broker_host):
        self.sensor_id = sensor_id
        self.broker_host = broker_host
        self.clock = 0
        self.lock = threading.Lock()

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
        print(f"[{self.sensor_id}] Iniciado. Relógio Lógico: {self.clock}")
        
        t_consume = threading.Thread(target=self.consume)
        t_produce = threading.Thread(target=self.produce)
        
        t_consume.start()
        t_produce.start()
        
        t_consume.join()
        t_produce.join()

    def consume(self):
        connection, channel = self.get_connection_and_channel()
        
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='trafego_events', queue=queue_name)

        def callback(ch, method, properties, body):
            msg = json.loads(body)
            if msg['sensor_id'] != self.sensor_id:
                with self.lock:
                    old_clock = self.clock
                    self.clock = max(self.clock, msg['lamport_clock']) + 1
                    print(f"[{self.sensor_id}] Notificado por {msg['sensor_id']}. Sincronizou: L={old_clock} -> L={self.clock}")

        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        try:
            channel.start_consuming()
        except Exception as e:
            print(f"[{self.sensor_id}] Erro na conexão de consumo: {e}")

    def produce(self):
        connection, channel = self.get_connection_and_channel()
        eventos = ["Veículo detectado", "Bloqueio na via", "Semáforo ignorado"]
        
        while True:
            time.sleep(random.uniform(2, 6))
            with self.lock:
                self.clock += 1
                msg = {
                    "sensor_id": self.sensor_id,
                    "evento": random.choice(eventos),
                    "lamport_clock": self.clock,
                    "timestamp_real": time.time()
                }
            try:
                channel.basic_publish(exchange='trafego_events', routing_key='', body=json.dumps(msg))
                print(f"[{self.sensor_id}] Disparou Evento: {msg['evento']} (L={self.clock})")
            except Exception as e:
                print(f"[{self.sensor_id}] Erro ao publicar: {e}")
                break

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--broker", required=True)
    args = parser.parse_args()
    
    sensor = SensorNode(args.id, args.broker)
    sensor.start()