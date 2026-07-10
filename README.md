# SD-Trafego

# Módulo 1: Infraestrutura de Rede e Injeção de Caos

Este módulo é responsável pela orquestração de contêineres, topologia de redes isoladas e pela automação dos cenários de engenharia de caos para validação das restrições de consistência e tolerância a falhas do sistema distribuído.

## Tecnologias Utilizadas
* Docker e Docker Compose (Ambiente multicontêiner)
* Linux Traffic Control (`tc/netem`) - Injeção de latência e perda de pacotes
* `iptables` - Simulação de partições de rede dinâmicas
* Python 3 / Bash - Scripts de automação

## Como Executar a Infraestrutura

### 1. Inicializar o Ambiente
Para subir o Broker (RabbitMQ), os nós sensores e as instâncias de semáforos, execute:
docker compose up -d


### 2. Executar o Cenário A: Rede Não-Confiável (Latência e Perda)
Este cenário injeta uma latência flutuante de 10ms a 4000ms com 5% de perda de pacotes na interface dos sensores. Isso força o Módulo 2 a ordenar os eventos usando Relógios Lógicos de Lamport.

Passo 1 (Monitorar): Em um terminal separado, ver a comunicação normal rodando um ping do sensor para o broker:
docker exec -it sensor_a ping rabbitmq_broker

Passo 2 (Injetar Caos): Executar o script:
python3 injetar_latencia.py

Passo 3 (Validar): Volte e observe o tempo do ping oscilar severamente e apresentar falhas esporádicas.

### 3. Executar o Cenário B: Partição de Rede (Split-Brain)
Divide os semáforos em duas sub-redes isoladas (nós 1 e 2 de um lado, nós 3 e 4 do outro). Obriga o Módulo 3 a aplicar a regra de Quórum Absoluto (Maioria Estrita) para evitar múltiplos líderes.

Passo 1 (Monitorar): Iniciar um ping direto entre semáforos de grupos diferentes:
docker exec -it semaforo_1 ping 192.168.140.13

Passo 2 (Injetar Caos): Executar o script para cortar as conexões:
bash criar_particao.sh

Passo 3 (Validar): O ping travará imediatamente.

Passo 4 (Restaurar/Curar): Para unificar a rede novamente:
bash curar_particao.sh
(O ping voltará a responder na mesma hora).

### 4. Executar o Cenário C: Queda Abrupta do Líder (Crash)
Simula a falha total e violenta de um nó específico (docker kill). Serve para validar os mecanismos de detecção por Heartbeats e a recuperação de estado via Checkpoints locais do Módulo 4.

Passo 1 (Injetar Caos): Executar o script de derrubada:
python3 matar_lider.py

Passo 2 (Validar): Executar rapidamente o comando abaixo para ver que o semaforo_1 sumiu da lista:
docker ps

Passo 3 (Recuperação): Aguarde alguns segundos e execute docker ps novamente. O Docker terá reiniciado o contêiner de forma automática, permitindo que ele recupere seu estado salvo.

---

## Módulo 2: Ordenação Causal e Sincronização Lógica (Lamport)

Este módulo resolve o problema introduzido pelo Cenário A de Caos (Latência extrema e perda de pacotes). Em sistemas distribuídos assíncronos não existe relógio global, o que faria eventos de tráfego chegarem desordenados e causarem processamentos incorretos nos semáforos.

### Tecnologias Utilizadas

* Python 3 (`pika`) e Multithreading (`Lock`).
* Relógios Lógicos de Lamport.

### Como Validar a Prova de Ordenação Causal (Critério 2)

1. Instale as dependências locais:

```bash
pip install pika

```

2. Suba o cluster recriando os sensores com a imagem python via:

```bash
docker compose up -d --build

```

3. Inicie o observador no seu terminal local (Host):

```bash
python observador.py

```

4. Em outro terminal, injete latência flutuante severa (10ms a 4000ms):

```bash
python injetar_latencia.py

```

5. **Comprovação:** Observe o terminal do `observador.py`. Você notará que o print `[REDE] Pacote físico chegou` acusará que mensagens mais recentes estão ultrapassando mensagens mais antigas na rede física. No entanto, a cada 10 segundos, o buffer é descarregado e o quadro `[PROVA DA ORDENAÇÃO CAUSAL]` reconstrói a sequência histórica perfeita utilizando a assinatura lógica (L=X) embutida pelos sensores, provando a resiliência do algoritmo.