import pika
import json
import time
import threading

buffer = []
lock = threading.Lock()

def process_buffer():
    # Esse observador faz uma coisa bem simples, mas importante:
    # ele mostra como a ordem de chegada na rede pode ser diferente da ordem lógica.
    # Em sistemas distribuídos isso é bem comum — e aí entra o relógio de Lamport.
    while True:
        time.sleep(10)
        with lock:
            if not buffer:
                continue
            
            print("\n" + "="*70)
            print(" [PROVA DA ORDENAÇÃO CAUSAL] - PROCESSANDO BUFFER COM ATRASO FÍSICO")
            print("="*70)
            
            # Ordena os eventos pelo relógio lógico de Lamport.
            # O timestamp real vira desempate, tipo um "tiebreaker".
            buffer_ordenado = sorted(buffer, key=lambda x: (x['lamport_clock'], x['timestamp_real']))
            
            print(f"{'ORDEM DE CHEGADA':<20} | {'ORDEM LÓGICA (LAMPORT)':<25} | {'EVENTO'}")
            print("-" * 70)
            
            for msg in buffer_ordenado:
                print(f"Chegou às {msg['chegada_real'][-8:]} | L={msg['lamport_clock']:02d} (Origem: {msg['sensor_id']}) | {msg['evento']}")
            
            buffer.clear()
            print("="*70 + "\n")

def callback(ch, method, properties, body):
    msg = json.loads(body)
    if msg.get("tipo") in ["TIME_REQUEST", "TIME_RESPONSE"]:
        return
    msg['chegada_real'] = time.strftime('%H:%M:%S', time.localtime())
    with lock:
        buffer.append(msg)
    print(f"[REDE] Pacote físico chegou: {msg['sensor_id']} relatando L={msg['lamport_clock']}")

if __name__ == "__main__":
    print("Iniciando Observador de Rede Central...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='trafego_events', exchange_type='fanout')
    
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='trafego_events', queue=queue_name)
    
    threading.Thread(target=process_buffer, daemon=True).start()
    
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
    