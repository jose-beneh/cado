import os, json, sys
from confluent_kafka import Consumer, Producer, KafkaError
from database import get_neo4j_driver

KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
neo4j_driver = get_neo4j_driver()

# ============================================================================
# CONFIGURAÇÃO DE SEGURANÇA DO CONSUMIDOR (CONTROLE DE OFFSETS)
# ============================================================================
conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'persister-group-v1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,       # CRÍTICO: Desativa a confirmação automática
    'session.timeout.ms': 45000,       # Tempo para detectar queda do container (45s)
    'max.poll.interval.ms': 300000     # Janela máxima tolerada para o processamento de I/O (5 min)
}

consumer = Consumer(conf_consumer)
consumer.subscribe(["system_anomalies"])

# Configuração robusta para o Producer (Garante entrega e resiliência)
producer = Producer({
    'bootstrap.servers': KAFKA_URL,
    'acks': 'all',                     # Exige confirmação de recebimento do broker Kafka
    'retries': 5                       # Tenta retransmitir automaticamente se houver oscilação de rede
})

CYPHER_PERSISTIR = """
MATCH (comp:componente {id: $componente_id})
MERGE (ae:anomalyevent {id: $anomaly_id})
ON CREATE SET
    ae.timestamp = toInteger($timestamp_ms),
    ae.anomaly_score = toFloat($anomaly_score),
    ae.status = "active"
MERGE (ae)-[:AFETOU]->(comp)
"""

print("--- [Anomaly Persister Ativo com Transação] ---")
sys.stdout.flush()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None: 
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[KAFKA ERRO DE LEITURA] {msg.error()}")
                sys.stdout.flush()
            continue

        payload = None
        try:
            payload = json.loads(msg.value().decode('utf-8'))

            # Passo 1: Transação Atômica no Neo4J
            with neo4j_driver.session() as session:
                session.run(CYPHER_PERSISTIR, 
                            componente_id=payload["componente_id"],
                            anomaly_id=payload["anomaly_id"], 
                            timestamp_ms=payload["timestamp_ms"],
                            anomaly_score=payload["anomaly_score"])

            # Passo 2: Publicação com garantia de persistência no Kafka (Fase 5)
            producer.produce("candidate_causes", key=payload["componente_id"], value=json.dumps(payload).encode('utf-8'))
            
            # O .flush() força o envio físico imediato dos bytes e lança exceção se o Kafka rejeitar
            producer.flush(timeout=3.0)

            # Passo 3: O "Aperto de Mão" Final (Commit Síncrono Obrigatório)
            # Se chegamos até aqui sem erros, confirma de forma definitiva a leitura para o Kafka
            consumer.commit(message=msg, asynchronous=False)
            print(f"[SUCESSO] Anomalia {payload['anomaly_id']} persistida no Neo4J e enviada para a Fase 5.")
            sys.stdout.flush()

        except Exception as e:
            # MECANISMO DE DEFESA: Se qualquer um dos passos falhar (Neo4J fora, Kafka timeout, JSON corrompido)
            # Nós caímos aqui e NÃO executamos o consumer.commit(). 
            print(f"[FALHA DE TRANSAÇÃO] Erro crítico no processamento da mensagem. Falha: {e}")
            if payload:
                print(f" -> Detalhes da anomalia travada: {payload.get('anomaly_id')} do componente {payload.get('componente_id')}")
            sys.stdout.flush()
            
            # Pequeno intervalo antes de tentar ler novamente para evitar loop infinito em alta velocidade que gaste CPU
            time.sleep(2.0)

except KeyboardInterrupt:
    print("\n[AVISO] Encerramento solicitado pelo operador.")
finally:
    consumer.close()
    print("[DESLIGAMENTO] Conexão do consumidor Kafka finalizada de forma limpa.")

