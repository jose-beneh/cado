import os, json, sys
from confluent_kafka import Consumer, Producer, KafkaError
from database import get_neo4j_driver

KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
neo4j_driver = get_neo4j_driver()

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'candidate-group-v1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = Consumer(conf_consumer)
consumer.subscribe(["candidate_causes"])

producer = Producer({'bootstrap.servers': KAFKA_URL, 'acks': 'all'})

# Query alinhada com o artigo: Isola o subgrafo estrutural de vizinhança direcionada de até 2 saltos
CYPHER_SUBGRAFO_FALHA = """
MATCH (comp_alvo:componente {id: $componente_id})
MATCH path = (comp_alvo)-[:DEPENDS_ON|CHAMA_REDE|REPORTA_SERVICO_A|REPORTA_RECURSOS_A*1..2]-(vizinho:componente)
MATCH (vizinho)-[:POSSUI_METRICA]->(m:operationalmetric)
WITH vizinho, m, comp_alvo
MATCH (origem:componente)-[r:DEPENDS_ON|CHAMA_REDE|REPORTA_SERVICO_A|REPORTA_RECURSOS_A]->(destino:componente)
WHERE (origem.id = vizinho.id OR origem.id = comp_alvo.id) AND (destino.id = vizinho.id OR destino.id = comp_alvo.id)
RETURN DISTINCT 
    vizinho.id AS componente_id, 
    vizinho.nome AS componente_nome, 
    m.metric_name AS metric_name, 
    m.vm_query AS query,
    origem.id AS aresta_origem,
    destino.id AS aresta_destino
"""

print("--- Candidate Extractor Ativo ---")
sys.stdout.flush()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error(): continue

        try:
            payload = json.loads(msg.value().decode('utf-8'))
            comp_id = payload["componente_id"]

            with neo4j_driver.session() as session:
                result = session.run(CYPHER_SUBGRAFO_FALHA, componente_id=comp_id)
                
                candidatos_dict = {}
                edges = []
                
                for r in result:
                    c_id = r["componente_id"]
                    if c_id not in candidatos_dict:
                        candidatos_dict[c_id] = {
                            "id": c_id,
                            "nome": r["componente_nome"],
                            "metric": r["metric_name"],
                            "query": r["query"]
                        }
                    edge = {"from": r["aresta_origem"], "to": r["aresta_destino"]}
                    if edge not in edges:
                        edges.append(edge)

            # Enriquecimento estrutural do payload com o subgrafo local delimitado
            payload["candidates"] = list(candidatos_dict.values())
            payload["topological_edges"] = edges

            producer.produce("causal_inference_jobs", key=comp_id, value=json.dumps(payload).encode('utf-8'))
            producer.flush()
            
            consumer.commit(message=msg, asynchronous=False)
            print(f"[FASE 5 SUCESSO] Subgrafo de falha delimitado para {comp_id}. Suspeitos: {len(candidatos_dict)} | Conexões: {len(edges)}")
            sys.stdout.flush()

        except Exception as e:
            print(f"[FASE 5 ERRO] Falha ao processar topologia: {e}")
            sys.stdout.flush()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

