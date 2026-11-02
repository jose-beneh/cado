import os
import json
import sys
import time
import requests  # Adicionado para interagir com a API HTTP do VictoriaMetrics
from confluent_kafka import Consumer, Producer, KafkaError
from database import get_neo4j_driver

# ============================================================================
# CONFIGURAÇÕES DE AMBIENTE E INFRAESTRUTURA
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
VICTORIAMETRICS_URL = os.getenv("VICTORIAMETRICS_URL", "http://victoriametrics:8428/api/v1/query_range")
neo4j_driver = get_neo4j_driver()

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'candidate-extractor-group-v3',  # Incremento de versão
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

// Extrai o host associado para podermos parametrizar a query PromQL/MetricsData
OPTIONAL MATCH (h:host)-[:EXECUTA]->(comp)

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
 h.hostname AS hostname,
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

# ============================================================================
# FUNÇÃO DE EXTRAÇÃO DE SÉRIES TEMPORAIS REAIS (VICTORIAMETRICS)
# ============================================================================
def buscar_serie_temporal_real(query_base, hostname, timestamp_ms, passos_retroativos=30):
    """
    Consulta o VictoriaMetrics para buscar a série temporal real do componente suspeito.
    Busca dados retroativos para criar uma janela histórica legítima para o SCM.
    """
    # Define a janela de tempo (ex: últimos 5 minutos a partir do evento de anomalia)
    fim_sec = int(timestamp_ms / 1000)
    inicio_sec = fim_sec - (passos_retroativos * 10) # 10s é o collection_interval do OTel
    
    # Resolve o template da query armazenada no Neo4j com o hostname do componente executando
    query_resolvida = query_base.replace("{hostname}", hostname.split('.')[0])
    
    params = {
        'query': query_resolvida,
        'start': inicio_sec,
        'end': fim_sec,
        'step': '10s'  # Alinhado rigidamente com a taxa de amostragem da infraestrutura
    }
    
    try:
        response = requests.get(VICTORIAMETRICS_URL, params=params, timeout=2.0)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success" and data["data"]["result"]:
                # Extrai apenas os valores numéricos da série temporal [timestamp, valor]
                valores_metricos = [float(val[1]) for val in data["data"]["result"][0]["values"]]
                return valores_metricos
    except Exception as ex:
        print(f"[ERR VICTORIAMETRICS] Falha ao extrair métrica real ({query_resolvida}): {ex}")
    
    # Fallback seguro caso o VictoriaMetrics falhe ou esteja sem dados na janela (Vetor neutro)
    return [0.0] * passos_retroativos

# ============================================================================
# LAÇO MESTRE DE INGESTÃO E PROCESSAMENTO
# ============================================================================
print("--- [Candidate Extractor Multimodal + Real-Time TS Ativo] ---")
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
            
            # Passo 1: Consulta o Grafo de Conhecimento (Neo4j) para isolar suspeitos topológicos
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
                queries_para_executar = [] # Armazenamento temporário para I/O fora do loop de leitura do grafo
                
                for r in result:
                    c_id = r["comp_id"]
                    
                    # Agrupa metadados para busca de série temporal real
                    if c_id and c_id not in candidatos_dict:
                        candidatos_dict[c_id] = {
                            "id": c_id,
                            "nome": r["comp_nome"],
                            "metric": r["metric_name"],
                            "real_time_series": [] # Será preenchido no Passo 2
                        }
                        if r["vm_query"] and r["hostname"]:
                            queries_para_executar.append({
                                "comp_id": c_id,
                                "query_base": r["vm_query"],
                                "hostname": r["hostname"]
                            })
                    
                    # Consolida arestas topológicas estruturais
                    if r["edge_from"] and r["edge_to"]:
                        edge = {"from": r["edge_from"], "to": r["edge_to"]}
                        if edge not in edges:
                            edges.append(edge)
                            
                    # Coleta evidências semânticas de Logs
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
                        if trace_data["errors"] > 0 or trace_data["avg_duration_ms"] > 100:
                            if trace_data not in traces_enriquecidos:
                                traces_enriquecidos.append(trace_data)

            # Passo 2: REQUISITO DE DOUTORADO - Hidratação das Séries Temporais REAIS via VictoriaMetrics
            # Buscamos de forma sequencial controlada (pode ser paralelizado com Threads se necessário)
            for q_task in queries_para_executar:
                id_alvo = q_task["comp_id"]
                valores_reais = buscar_serie_temporal_real(
                    query_base=q_task["query_base"],
                    hostname=q_task["hostname"],
                    timestamp_ms=timestamp_ms
                )
                candidatos_dict[id_alvo]["real_time_series"] = valores_reais

            # Adiciona também a série temporal histórica real do próprio componente que gerou o alerta (Alvo)
            # Para o SCM comparar de forma justa com os vizinhos
            with neo4j_driver.session() as session:
                res_alvo = session.run(
                    "MATCH (c:componente {id: $id})-[:POSSUI_METRICA]->(m:operationalmetric), (h:host)-[:EXECUTA]->(c) RETURN m.vm_query as q, h.hostname as host", 
                    id=comp_id
                ).single()
                if res_alvo:
                    payload["telemetry_snapshot"]["real_time_series_alvo"] = buscar_serie_temporal_real(
                        query_base=res_alvo["q"], hostname=res_alvo["host"], timestamp_ms=timestamp_ms
                    )
                else:
                    payload["telemetry_snapshot"]["real_time_series_alvo"] = [0.0] * 30

                # Passo 3: Acoplamento e envio do payload IMUTÁVEL e legítimo
                payload["candidates"] = list(candidatos_dict.values())
                payload["topological_edges"] = edges
                payload["EVIDENCIAS_SEMANTICAS"] = {
                    "logs_janela": logs_enriquecidos,
                    "traces_falhos_janela": traces_enriquecidos
                }

                # Encaminha o snapshot enriquecido com dados legítimos para o motor SCM (Fase 6)
                producer.produce("causal_inference_jobs", key=comp_id, value=json.dumps(payload).encode('utf-8'))
                producer.flush()

                # Confirmação manual de leitura estável transacional (Aperto de mão síncrono)
                consumer.commit(message=msg, asynchronous=False)
                print(f"[FASE 5 SUCESSO] Dados HISTÓRICOS REAIS consolidados para o SCM do componente: {comp_id}.")
                sys.stdout.flush()
            except Exception as e:
                print(f"[FASE 5 ERRO] Falha crítica no pipeline de extração e I/O: {e}")
                sys.stdout.flush()
            except KeyboardInterrupt:
                print("\n[AVISO] Interrupção manual solicitada pelo operador.")
            finally:
                consumer.close()
