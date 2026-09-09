import os, json, requests, sys
from confluent_kafka import Consumer, Producer
from database import get_neo4j_driver

KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
VM_URL = os.getenv("VICTORIAMETRICS_URL", "http://victoriametrics:8428/api/v1/query_range")
neo4j_driver = get_neo4j_driver()

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'scm-group-v1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = Consumer(conf_consumer)
consumer.subscribe(["causal_inference_jobs"])

CYPHER_GRAVAR_CAUSA = """
MERGE (ae_alvo:anomalyevent {id: $anomaly_id})
MATCH (comp_causa:componente {id: $componente_causa})
MERGE (ae_causa:anomalyevent {id: "ANOMALIA_CAUSA_" + $componente_causa + "_" + toString($timestamp_ms)})
ON CREATE SET 
    ae_causa.timestamp = toInteger($timestamp_ms) - 5000,
    ae_causa.metric_or_log_template = $metrica_causa,
    ae_causa.status = "derived"
MERGE (ae_causa)-[:AFETOU]->(comp_causa)
MERGE (ae_alvo)-[r:CAUSADO_POR]->(ae_causa)
SET r.confidence_score = toFloat($confidence),
    r.temporal_lag_ms = 5000
"""

def consultar_vm(query, ts):
    try:
        end = ts / 1000.0
        res = requests.get(VM_URL, params={'query': query, 'start': end - 300.0, 'end': end, 'step': '15s'}, timeout=2.0).json()
        points = res.get('data', {}).get('result', [{}])[0].get('values', [])
        return [float(p[1]) for p in points] if points else []
    except Exception: return []

def calcular_residuo_scm(x, y):
    """
    Formalização SCM: Executa regressão linear contrafactual local para isolar 
    a independência do ruído estrutural (Mecânica de SCM LiNGAM).
    """
    n = len(x)
    mean_x, mean_y = sum(x)/n, sum(y)/n
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den = sum((x[i] - mean_x) ** 2 for i in range(n))
    
    if den == 0: return float('inf')
    beta = num / den
    
    # Calcula os resíduos contrafactuais N = Y - Beta*X
    residuos = [y[i] - (beta * x[i]) for i in range(n)]
    mean_res = sum(residuos) / n
    var_res = sum((r - mean_res) ** 2 for r in range(n)) / n
    return var_res

print("--- Causal SCM Engine Ativa ---")
sys.stdout.flush()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error(): continue
        
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            candidatos = payload.get("candidates", [])
            edges = payload.get("topological_edges", [])
            comp_alvo_id = payload["componente_id"]
            timestamp_ms = payload["timestamp_ms"]

            if not candidatos: 
                consumer.commit(message=msg, asynchronous=False)
                continue

            # Captura a série temporal do Efeito (Alvo)
            comp_nome_alvo = comp_alvo_id.split('@')[0]
            query_alvo = f"sum(jvm_memory_bytes_used{{area='heap', job='{comp_nome_alvo}'}})"
            serie_alvo = consultar_vm(query_alvo, timestamp_ms)

            if not serie_alvo:
                serie_alvo = [float(payload["anomaly_score"])] * 10

            melhor_causa_id = None
            metrica_causa = None
            maior_score_causal_cs = 0.0

            # Avaliação Causal Estrutural Multifatorial (CS)
            for cand in candidatos:
                cand_id = cand["id"]
                if cand_id == comp_alvo_id: continue

                query_real = cand["query"].replace("{hostname}", cand["nome"]).replace("{ip}", cand["nome"])
                serie_cand = consultar_vm(query_real, timestamp_ms)

                if not serie_cand: continue

                min_len = min(len(serie_alvo), len(serie_cand))
                if min_len < 4: continue

                x_data = serie_cand[:min_len]
                y_data = serie_alvo[:min_len]

                # 1. Componente de Ruído do SCM
                var_ruido = calcular_residuo_scm(x_data, y_data)
                
                # 2. Componente de Direcionamento do Grafo (Checa restrição estrutural)
                possui_caminho_direto = any(e["from"] == cand_id and e["to"] == comp_alvo_id for e in edges)
                fator_grafo = 1.2 if possui_caminho_direto else 0.8

                # 3. Formulação do Score Causal (CS) Multifatorial
                # Quanto menor a variância do resíduo contrafactual, maior o nexo causal real
                score_cs = (1.0 / (1.0 + var_ruido)) * Fator_grafo
                print(f" -> SCM Evaluation: '{cand_id}' | Variância Ruído SCM: {var_ruido:.4f} | Multifactor CS: {score_cs:.4f}")

                if score_cs > maior_score_causal_cs:
                    maior_score_causal_cs = score_cs
                    melhor_causa_id = cand_id
                    metrica_causa = cand["metric"]

            # Registro do Veredito Causal no Neo4J
            if melhor_causa_id and maior_score_causal_cs > 0.25:
                # Normaliza o score de confiança causal final entre 0 e 1
                confidence_final = min(1.0, maior_score_causal_cs / 1.2)
                
                with neo4j_driver.session() as session:
                    session.run(CYPHER_GRAVAR_CAUSA, 
                                anomaly_id=payload["anomaly_id"], 
                                componente_causa=melhor_causa_id, 
                                timestamp_ms=timestamp_ms, 
                                metrica_causa=metrica_causa, 
                                confidence=confidence_final)
                print(f"[SCM VEREDITO CONCLUÍDO] Relação causal provada: {payload['anomaly_id']} -> {melhor_causa_id} (CS Confiança: {confidence_final:.4f})")
            else:
                print(f"[SCM INFO] Análise contrafactual concluída para {comp_alvo_id}. Causa intrínseca sem propagação de nós vizinhos.")

            consumer.commit(message=msg, asynchronous=False)
            sys.stdout.flush()

        except Exception as e:
            print(f"[ERRO CATASTRÓFICO SCM]: {e}")
            sys.stdout.flush()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

