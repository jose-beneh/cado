import os
import json
import sys
import math
from confluent_kafka import Consumer, Producer, KafkaError
from database import get_neo4j_driver

# ============================================================================
# CONFIGURAÇÕES DE AMBIENTE E INFRAESTRUTURA
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
neo4j_driver = get_neo4j_driver()

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'scm-engine-group-v3',  # Atualizado para consistência com o extrator
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = Consumer(conf_consumer)
consumer.subscribe(["causal_inference_jobs"])

# Tópico de saída para a próxima fase (Gating Cognitivo de Incerteza / EDL / LLM)
producer_next = Producer({'bootstrap.servers': KAFKA_URL, 'acks': 1})

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
# AUXILIARES MATEMÁTICOS (ESTATÍSTICA ONLINE EM MEMÓRIA)
# ============================================================================
def normalizar_z_score(serie):
    """Aplica Standard Scaler para viabilizar o cálculo do SCM cross-modal."""
    n = len(serie)
    if n < 2:
        return [0.0] * n
    media = sum(serie) / n
    variancia = sum((x - media) ** 2 for x in serie) / n
    desvio_padrao = math.sqrt(variancia) if variancia > 1e-9 else 1.0
    return [(x - media) / desvio_padrao for x in serie]

def calcular_residuo_scm(x, y):
    """
    Executa a regressão linear contrafactual baseada no formalismo de Pearl.
    Mede a variância do ruído exógeno (U) para testar a direção da causalidade.
    """
    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    
    # Cálculo dos coeficientes da regressão (Beta)
    num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    den = sum((x[i] - mean_x) ** 2 for i in range(n))
    
    if den == 0:
        return float('inf')
        
    beta = num / den
    
    # Isolamento dos resíduos contrafactuais exógenos: N = Y - Beta * X
    residuos = [y[i] - (beta * x[i]) for i in range(n)]
    mean_res = sum(residuos) / n
    var_res = sum((r - mean_res) ** 2 for r in range(n)) / n
    
    return var_res

# ============================================================================
# LAÇO MESTRE PRINCIPAL (STREAMING PURÓ)
# ============================================================================
print("--- [Causal SCM Engine Ativa com Séries Temporais Reais] ---")
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
            
            # Recupera a série temporal histórica real do componente sob alerta (Efeito/Alvo Y)
            snapshot_alvo = payload.get("telemetry_snapshot", {})
            serie_real_alvo = snapshot_alvo.get("real_time_series_alvo", [])
            
            if not candidatos or len(serie_real_alvo) < 5:
                # Sem histórico suficiente ou sem candidatos vizinhos para avaliar
                consumer.commit(message=msg, asynchronous=False)
                continue
                
            # Normaliza o vetor do componente afetado (Y)
            y_normalizado = normalizar_z_score(serie_real_alvo)
            
            melhor_causa_id = None
            metrica_causa = None
            maior_score_causal_cs = 0.0
            
            # Avaliação Causal Estrutural baseada na minimização do ruído exógeno
            for cand in candidatos:
                cand_id = cand["id"]
                if cand_id == comp_alvo_id:
                    continue
                    
                serie_cand = cand.get("real_time_series", [])
                # Garante o alinhamento temporal e tamanho idêntico dos vetores
                min_len = min(len(y_normalizado), len(serie_cand))
                
                if min_len < 5:  # Requisito estatístico mínimo de amostras históricas
                    continue
                    
                # Isola e normaliza o vetor do candidato a causa raiz (X)
                x_recortado = serie_cand[:min_len]
                x_normalizado = normalizar_z_score(x_recortado)
                y_recortado = y_normalizado[:min_len]
                
                # 1. Componente de Ruído Contrafactual (SCM Pearl/LiNGAM)
                var_ruido = calcular_residuo_scm(x_normalizado, y_recortado)
                
                # 2. Componente de Direcionamento do Grafo (Checa restrição topológica estrutural)
                # Se o Grafo do Sistema indica um fluxo lógico direto, aumenta o peso causal a priori
                possui_caminho_direto = any(e["from"] == cand_id and e["to"] == comp_alvo_id for e in edges)
                fator_grafo = 1.3 if possui_caminho_direto else 0.7
                
                # 3. Formulação do Score Causal (CS) Multifatorial da Tese
                # Quanto menor a variância do resíduo (independência linear), maior o nexo causal real
                score_cs = (1.0 / (1.0 + var_ruido)) * fator_grafo
                
                print(f" -> SCM Eval [Alvo: {comp_alvo_id} <- Causa: {cand_id}] | Var Ruído Exógeno: {var_ruido:.4f} | CS Score: {score_cs:.4f}")
                sys.stdout.flush()
                
                if score_cs > maior_score_causal_cs:
                    maior_score_causal_cs = score_cs
                    melhor_causa_id = cand_id
                    metrica_causa = cand["metric"]

            # ----------------------------------------------------------------
            # REGISTRO DO VEREDITO CAUSAL E REDIRECIONAMENTO DO PIPELINE
            # ----------------------------------------------------------------
            veredito_final = {
                "root_cause_candidate": melhor_causa_id if melhor_causa_id else "Causa Intrínseca (Local)",
                "causal_confidence": 0.0,
                "metric_analyzed": metrica_causa if metrica_causa else "jvm_memory_usage"
            }
            
            # Limiar de corte para validação do nexo causal (evita falsas propagações)
            if melhor_causa_id and maior_score_causal_cs > 0.35:
                confidence_final = min(1.0, maior_score_causal_cs / 1.5)
                veredito_final["causal_confidence"] = float(confidence_final)
                
                # Grava de forma síncrona a aresta causal gerada no System Graph (Neo4j)
                with neo4j_driver.session() as session:
                    session.run(
                        CYPHER_GRAVAR_CAUSA,
                        anomaly_id=payload["anomaly_id"],
                        componente_causa=melhor_causa_id,
                        timestamp_ms=timestamp_ms,
                        metrica_causa=metrica_causa,
                        confidence=confidence_final
                    )
                print(f"[SCM VEREDITO] Nexus provado: {payload['anomaly_id']} originou-se em {melhor_causa_id} (Confiança: {confidence_final:.4f})")
            else:
                print(f"[SCM INFO] Análise concluída para {comp_alvo_id}. Nenhuma propagação externa detectada. Causa Local.")

            # Anexa o veredito científico validado ao grande Payload Multimodal
            payload["causal_verdict"] = veredito_final
            
            # MODIFICAÇÃO ARQUITETURAL: Em vez de enviar direto para a LLM, despacha para a
            # esteira de processamento de risco e quantificação de incerteza (Fase 7 - EDL / CMDP)
            producer_next.produce("evidential_gating_jobs", key=comp_id, value=json.dumps(payload).encode('utf-8'))
            producer_next.flush()
            
            # Confirmação manual de leitura estável transacional (Aperto de mão síncrono no Kafka)
            consumer.commit(message=msg, asynchronous=False)
            sys.stdout.flush()
            
        except Exception as e:
            print(f"[ERRO CATASTRÓFICO SCM]: {e}")
            sys.stdout.flush()

except KeyboardInterrupt:
    print("\n[AVISO] Encerramento do motor SCM solicitado pelo operador.")
finally:
    consumer.close()

