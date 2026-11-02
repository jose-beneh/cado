import os
import json
import sys
import time
from confluent_kafka import Consumer, Producer, KafkaError
from database import get_neo4j_driver

# ============================================================================
# CONFIGURAÇÕES DE AMBIENTE E INFRAESTRUTURA
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "192.168.3.212:9092")
neo4j_driver = get_neo4j_driver()

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'candidate-extractor-group-v2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf_consumer)
consumer.subscribe(["candidate_causes"])
producer = Producer({'bootstrap.servers': KAFKA_URL, 'acks': 1})

# ============================================================================
# QUERY CYPHER MULTIMODAL ENRIQUECIDA (TOPOLOGIA + LOGS + TRACES)
# ============================================================================
CYPHER_ENRIQUECIMENTO_MULTIMODAL = """
MATCH (comp_alvo:componente {id: $componente_id})

// Passo 1: Isola os vizinhos topológicos de até 2 saltos no cluster Hadoop
OPTIONAL MATCH path = (comp_alvo)-[:DEPENDS_ON|CHAMA_REDE|REPORTA_SERVICO_A|REPORTA_RECURSOS_A*1..2]-(vizinho:componente)
WITH comp_alvo, collect(DISTINCT vizinho) + comp_alvo AS todos_componentes

UNWIND todos_componentes AS comp
MATCH (comp)-[:POSSUI_METRICA]->(m:operationalmetric)

// Passo 2: Captura as arestas estruturais ativas entre os componentes mapeados
OPTIONAL MATCH (origem:componente)-[r_topo:DEPENDS_ON|REPORTA_SERVICO_A|REPORTA_RECURSOS_A]->(destino:componente)
WHERE origem IN todos_componentes AND destino IN todos_componentes

// Passo 3: Captura evidências de LOGS associados ocorridos próximos à janela (tolerância de 30s)
OPTIONAL MATCH (comp)-[r_log:REGISTROU_LOG]->(lt:logtemplate)
WHERE r_log.timestamp_janela >= ($timestamp_ms - 30000) AND r_log.timestamp_janela <= ($timestamp_ms + 15000)

// Passo 4: Captura fluxos de rede de TRACES/RPCs falhos ocorridos na mesma janela
OPTIONAL MATCH (comp)-[r_trace:CHAMA_REDE]->(destino_trace:componente)
WHERE r_trace.timestamp_janela >= ($timestamp_ms - 30000) AND r_trace.timestamp_janela <= ($timestamp_ms + 15000)

RETURN 
    comp.id AS comp_id,
    comp.nome AS comp_nome,
    m.metric_name AS metric_name,
    m.vm_query AS vm_query,
    origem.id AS edge_from,
    destino.id AS edge_to,
    {
        template_id: lt.id,
        pattern: lt.text_pattern,
        occurrences: r_log.occurrences
    } AS evidência_log,
    {
        from: comp.id,
        to: destino_trace.id,
        route: r_trace.route,
        errors: r_trace.errors,
        avg_duration_ms: r_trace.avg_duration_ms
    } AS evidência_trace
"""

print("--- [Candidate Extractor Multimodal Ativo] ---")
sys.stdout.flush()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: 
            continue
        if msg.error(): 
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[KAFKA ERRO] {msg.error()}")
            continue
            
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            comp_id = payload["componente_id"]
            timestamp_ms = payload["timestamp_ms"]
            
            with neo4j_driver.session() as session:
                result = session.run(
                    CYPHER_ENRIQUECIMENTO_MULTIMODAL, 
                    componente_id=comp_id, 
                    timestamp_ms=timestamp_ms
                )
                
                candidatos_dict = {}
                edges = []
                logs_enriquecidos = []
                traces_enriquecidos = []
                
                for r in result:
                    c_id = r["comp_id"]
                    # Consolida informações de componentes e suas queries métricas
                    if c_id and c_id not in candidatos_dict:
                        candidatos_dict[c_id] = {
                            "id": c_id,
                            "nome": r["comp_nome"],
                            "metric": r["metric_name"],
                            "query": r["vm_query"]
                        }
                    
                    # Consolida arestas topológicas estruturais
                    if r["edge_from"] and r["edge_to"]:
                        edge = {"from": r["edge_from"], "to": r["edge_to"]}
                        if edge not in edges:
                            edges.append(edge)
                    
                    # Coleta evidências semânticas de Logs (Filtra registros nulos do OPTIONAL MATCH)
                    if r["evidência_log"] and r["evidência_log"]["template_id"] is not None:
                        log_data = r["evidência_log"]
                        log_snapshot = {
                            "componente_id": c_id,
                            "template_id": log_data["template_id"],
                            "pattern": log_data["pattern"],
                            "occurrences": log_data["occurrences"]
                        }
                        if log_snapshot not in logs_enriquecidos:
                            logs_enriquecidos.append(log_snapshot)
                            
                    # Coleta evidências semânticas de Traces/RPCs
                    if r["evidência_trace"] and r["evidência_trace"]["to"] is not None:
                        trace_data = r["evidência_trace"]
                        if trace_data["errors"] > 0 or trace_data["avg_duration_ms"] > 100: # Foca em lentidões ou falhas
                            if trace_data not in traces_enriquecidos:
                                traces_enriquecidos.append(trace_data)
            
            # Acoplamento de metadados expandidos ao payload original da Fase 4
            payload["candidates"] = list(candidatos_dict.values())
            payload["topological_edges"] = edges
            payload["EVIDENCIAS_SEMANTICAS"] = {
                "logs_janela": logs_enriquecidos,
                "traces_falhos_janela": traces_enriquecidos
            }
            
            # Encaminha o snapshot enriquecido para o motor de inferência causal (Fase 6)
            producer.produce("causal_inference_jobs", key=comp_id, value=json.dumps(payload).encode('utf-8'))
            producer.flush()
            
            # Confirmação manual de leitura estável (Aperto de mão síncrono)
            consumer.commit(message=msg, asynchronous=False)
            print(f"[FASE 5 SUCESSO] Payload multimodal consolidado para {comp_id}.")
            print(f" -> Suspeitos: {len(candidatos_dict)} | Conexões: {len(edges)} | Logs coletados: {len(logs_enriquecidos)}")
            sys.stdout.flush()
            
        except Exception as e:
            print(f"[FASE 5 ERRO] Falha crítica no processamento da topologia: {e}")
            sys.stdout.flush()

except KeyboardInterrupt:
    print("\n[AVISO] Interrupção manual solicitada.")
finally:
    consumer.close()

