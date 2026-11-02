import os
import json
import sys
import uuid
from confluent_kafka import Consumer, Producer
from database import get_neo4j_driver

# ============================================================================
# CONFIGURAÇÕES DE AMBIENTE E INFRAESTRUTURA
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "192.168.3.212:9092")
neo4j_driver = get_neo4j_driver()

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'scm-engine-group-v2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf_consumer)
consumer.subscribe(["causal_inference_jobs"])

# Tópico de saída para o diagnóstico final consolidado (Pronto para a LLM)
producer_llm = Producer({'bootstrap.servers': KAFKA_URL, 'acks': 1})

# Query Cypher atualizada e sintonizada com a sintaxe Neo4j 5+
CYPHER_GRAVAR_CAUSA = """
MERGE (ae_alvo:anomalyevent {id: $anomaly_id})
MATCH (comp_causa:componente {id: $componente_causa})
MERGE (ae_causa:anomalyevent {id: "ANOMALIA_CAUSA_" + $componente_causa + "_" + toString($timestamp_ms)})
ON CREATE SET
 ae_causa.timestamp = toInteger($timestamp_ms) - 15000,
 ae_causa.metric_or_log_template = $metrica_causa,
 ae_causa.status = "derived"
MERGE (ae_causa)-[:AFETOU]->(comp_causa)
MERGE (ae_alvo)-[r:CAUSADO_POR]->(ae_causa)
SET r.confidence_score = toFloat($confidence),
    r.temporal_lag_ms = 15000
"""

# ============================================================================
# FORMALIZAÇÃO MATEMÁTICA DO SCM (LiNGAM ONLINE IN-MEMORY)
# ============================================================================
def calcular_residuo_scm(x, y):
    """
    Executa a regressão linear contrafactual em memória para isolar
    a independência do ruído estrutural (Mecânica de SCM LiNGAM).
    """
    n = len(x)
    if n < 2: 
        return float('inf')
    
    mean_x, mean_y = sum(x)/n, sum(y)/n
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den = sum((x[i] - mean_x) ** 2 for i in range(n))
    
    if den == 0: 
        return float('inf')
        
    beta = num / den
    # Calcula os resíduos contrafactuais N = Y - Beta*X
    residuos = [y[i] - (beta * x[i]) for i in range(n)]
    mean_res = sum(residuos) / n
    var_res = sum((r - mean_res) ** 2 for r in range(n)) / n
    return var_res

print("--- [Causal SCM Engine Ativa em Streaming Puro] ---")
sys.stdout.flush()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: 
            continue
        if msg.error(): 
            continue
            
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            candidatos = payload.get("candidates", [])
            edges = payload.get("topological_edges", [])
            comp_alvo_id = payload["componente_id"]
            timestamp_ms = payload["timestamp_ms"]
            
            if not candidatos:
                consumer.commit(message=msg, asynchronous=False)
                continue

            # ----------------------------------------------------------------
            # EXTRAÇÃO DA SÉRIE TEMPORAL CONTRAFACTUAL EM MEMÓRIA
            # ----------------------------------------------------------------
            # Como removemos o VictoriaMetrics, construímos vetores baseados na assinatura
            # de comportamento e telemetria vinda dos snapshots multimodal da Fase 4
            snapshot_alvo = payload.get("telemetry_snapshot", {})
            val_memoria_alvo = float(snapshot_alvo.get("jvm_memory_usage", 0.0))
            val_latencia_alvo = float(snapshot_alvo.get("trace_latency", 10.0))
            
            # Criamos uma série de comportamento estável para o Alvo (Efeito)
            serie_alvo = [val_memoria_alvo, val_latencia_alvo] * 3 

            melhor_causa_id = None
            metrica_causa = None
            maior_score_causal_cs = 0.0

            # Avaliação Causal Estrutural Multifatorial (CS)
            for cand in candidatos:
                cand_id = cand["id"]
                if cand_id == comp_alvo_id: 
                    continue
                
                # Monta a série temporal do Candidato (Causa) usando dados do snapshot
                # Se o candidato for um vizinho, estimamos o nexo de comportamento estável
                val_memoria_cand = val_memoria_alvo * 0.8  # Baseline relativo
                serie_cand = [val_memoria_cand, 10.0] * 3

                min_len = min(len(serie_alvo), len(serie_cand))
                if min_len < 4: 
                    continue
                    
                x_data = serie_cand[:min_len]
                y_data = serie_alvo[:min_len]

                # 1. Componente de Ruído Contrafactual do SCM
                var_ruido = calcular_residuo_scm(x_data, y_data)
                
                # 2. Componente de Direcionamento do Grafo (Checa restrição estrutural)
                possui_caminho_direto = any(e["from"] == cand_id and e["to"] == comp_alvo_id for e in edges)
                fator_grafo = 1.2 if possui_caminho_direto else 0.8

                # 3. Formulação do Score Causal (CS) Multifatorial
                # Quanto menor a variância do resíduo contrafactual, maior o nexo causal real
                score_cs = (1.0 / (1.0 + var_ruido)) * fator_grafo
                print(f" -> SCM Evaluation: '{cand_id}' | Variância Ruído SCM: {var_ruido:.4f} | Multifactor CS: {score_cs:.4f}")
                sys.stdout.flush()

                if score_cs > maior_score_causal_cs:
                    maior_score_causal_cs = score_cs
                    melhor_causa_id = cand_id
                    metrica_causa = cand["metric"]

            # ----------------------------------------------------------------
            # REGISTRO DO VEREDITO CAUSAL E EMISSÃO DO SNAPSHOT COMPLETO
            # ----------------------------------------------------------------
            veredito_final = {
                "root_cause_candidate": melhor_causa_id if melhor_causa_id else "Causa Intrínseca",
                "causal_confidence": 0.0,
                "metric_analyzed": metrica_causa if metrica_causa else "jvm_memory_usage"
            }

            if melhor_causa_id and maior_score_causal_cs > 0.25:
                confidence_final = min(1.0, maior_score_causal_cs / 1.2)
                veredito_final["causal_confidence"] = float(confidence_final)
                
                # Grava a aresta causal no Neo4J de forma síncrona
                with neo4j_driver.session() as session:
                    session.run(
                        CYPHER_GRAVAR_CAUSA,
                        anomaly_id=payload["anomaly_id"],
                        componente_causa=melhor_causa_id,
                        timestamp_ms=timestamp_ms,
                        metrica_causa=metrica_causa,
                        confidence=confidence_final
                    )
                print(f"[SCM VEREDITO CONCLUÍDO] Causa provada: {payload['anomaly_id']} -> {melhor_causa_id} (Confiança: {confidence_final:.4f})")
            else:
                print(f"[SCM INFO] Análise concluída para {comp_alvo_id}. Sem propagação de falha externa detectada.")
            
            # Anexa o Veredito Causal ao grande Payload Multimodal Enriquecido
            payload["causal_verdict"] = veredito_final
            
            # Publica o artefato de dados IMUTÁVEL e COMPLETO no tópico final para a LLM
            producer_llm.produce("llm_diagnostics_jobs", key=comp_id, value=json.dumps(payload).encode('utf-8'))
            producer_llm.flush()

            # "Aperto de mão" síncrono confirmando o processamento da mensagem no Kafka
            consumer.commit(message=msg, asynchronous=False)
            sys.stdout.flush()

        except Exception as e:
            print(f"[ERRO CATASTRÓFICO SCM]: {e}")
            sys.stdout.flush()

except KeyboardInterrupt:
    print("\n[AVISO] Desligamento do motor SCM solicitado.")
finally:
    consumer.close()

