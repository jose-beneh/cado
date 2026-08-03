import os
import json
import time
import math
import faust
from database import get_neo4j_driver

KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://kafka:29092")

app = faust.App(
    'hadoop-causal-inference-scm',
    broker=KAFKA_URL,
    reply_create_topic=False,
    value_serializer='json'
)

candidate_causes_topic = app.topic('candidate_causes', value_type=dict)
root_cause_verdicts_topic = app.topic('root_cause_verdicts', value_type=dict)

neo4j_driver = get_neo4j_driver()

# Query Cypher para persistir o grafo explicativo final (Fase 6)
CYPHER_PERSIST_RCA_EDGE = """
MATCH (sintoma:anomalyevent {id: $alerta_id})
MERGE (causa:anomalyevent {id: $causa_id})
  ON CREATE SET 
    causa.timestamp = toInteger($causa_timestamp),
    causa.metric_or_log_template = toString($causa_gatilho),
    causa.severity = toString($causa_severidade),
    causa.status = "resolved"
MERGE (sintoma)-[r:CAUSADO_POR]->(causa)
SET r.confidence_score = toFloat($confidence_score),
    r.temporal_lag_ms = toInteger($temporal_lag_ms)
"""

def calcular_rca_matematico(payload):
    componente_foco = payload.get("componente_foco")
    subgrafo = payload.get("subgrafo_suspeito", {})
    timestamp_alerta = payload.get("timestamp_investigacao")
    
    candidatos_avaliados = []
    
    # --- MODELAGEM CAUSAL 1: AVALIAÇÃO DOS LOG TEMPLATES VIZINHOS ---
    for log in subgrafo.get("logs", []):
        template_id = log.get("template_id")
        pattern = log.get("pattern", "")
        
        # Coeficiente 1: Severidade Intrínseca (Busca por palavras-chave críticas de Java/Hadoop)
        keywords_criticas = ["exception", "timeout", "failed", "error", "fatal", "killed", "leak"]
        s_severidade = 1.0 if any(k in pattern.lower() for k in keywords_criticas) else 0.4
        
        # Coeficiente 2: Alinhamento Temporal (Simulação simplificada do Time Decay SCM)
        # Como o Faust agregou na mesma janela de 30s, assumimos um lag aceitável padrão de 5000ms
        temporal_lag = 5000 
        s_tempo = math.exp(-temporal_lag / 30000.0) # Constante de relaxamento de 30s
        
        # Coeficiente 3: Impacto Topológico (Logs do próprio nó afetado têm peso máximo)
        s_topologia = 1.0
        
        # Cálculo do Score de Confiança Causal (Pesos: 50% tempo, 30% topologia, 20% severidade)
        confidence_score = (0.5 * s_tempo) + (0.3 * s_topologia) + (0.2 * s_severidade)
        
        candidatos_avaliados.append({
            "tipo": "LOG_TEMPLATE",
            "id_causa": template_id,
            "descricao": pattern,
            "componente_origem": componente_foco,
            "confidence_score": round(confidence_score, 4),
            "temporal_lag_ms": temporal_lag
        })

    # --- MODELAGEM CAUSAL 2: AVALIAÇÃO DAS DEPENDÊNCIAS E REDE ---
    # Analisa se componentes vizinhos da topologia física registraram erros que impactaram o nó alvo
    for dep in subgrafo.get("dependencias", []):
        if dep.get("status_atual") != "healthy":
            confidence_score = 0.85 # Alta confiança se uma dependência estática declarada estiver instável
            candidatos_avaliados.append({
                "tipo": "COMPONENT_DEPENDENCY_FAILURE",
                "id_causa": f"ANOMALIA_DEP_{dep.get('componente_id')}",
                "descricao": f"Dependência estrutural tipo '{dep.get('tipo')}' está com status {dep.get('status_atual')}",
                "componente_origem": dep.get("componente_id"),
                "confidence_score": confidence_score,
                "temporal_lag_ms": 10000
            })

    for r_rede in subgrafo.get("rede_entrada", []):
        if int(r_rede.get("erros_rede", 0)) > 0:
            # Coeficiente de latência e erro na chamada de rede (Traces RPC do Hadoop)
            confidence_score = 0.4 + (0.1 * min(float(r_rede.get("latencia_media", 0)) / 100.0, 5.0))
            candidatos_avaliados.append({
                "tipo": "NETWORK_RPC_DELAY",
                "id_causa": f"ANOMALIA_NET_{r_rede.get('origem')}",
                "descricao": f"Lentidão/Erros em chamadas RPC originadas de {r_rede.get('origem')}. Latência: {r_rede.get('latencia_media')}ms",
                "componente_origem": r_rede.get("origem"),
                "confidence_score": round(confidence_score, 4),
                "temporal_lag_ms": 2000
            })

    # Ordena todos os candidatos matemáticos pelo maior Score de Confiança Causal (SCM)
    candidatos_avaliados.sort(key=lambda x: x["confidence_score"], reverse=True)
    return candidatos_avaliados

def persistir_grafo_explicativo_sync(alerta_id, principal_causa):
    try:
        with neo4j_driver.session() as session:
            session.run(
                CYPHER_PERSIST_RCA_EDGE,
                alerta_id=str(alerta_id),
                causa_id=str(principal_causa["id_causa"]),
                causa_timestamp=int(time.time() * 1000 - principal_causa["temporal_lag_ms"]),
                causa_gatilho=str(principal_causa["descricao"]),
                causa_severidade="CRITICAL" if principal_causa["confidence_score"] > 0.7 else "MEDIUM",
                confidence_score=float(principal_causa["confidence_score"]),
                temporal_lag_ms=int(principal_causa["temporal_lag_ms"])
            )
            print(f"🔗 [NEO4J SCM] Aresta [:CAUSADO_POR] gravada com sucesso do Alerta {alerta_id} para a Causa {principal_causa['id_causa']}.")
    except Exception as e:
        print(f"[NEO4J SCM ERR] Falha ao persistir aresta causal: {e}")

@app.agent(candidate_causes_topic)
async def processar_inferencia_causal(stream):
    async for payload in stream:
        try:
            alerta_id = payload.get("alerta_origem_id")
            componente_foco = payload.get("componente_foco")
            
            print(f"\n🧠 [INFERÊNCIA CAUSAL SCM] Processando matriz de suspeitos para o componente: {componente_foco}")
            
            # Executa o cálculo determinístico baseado em tempo, topologia e severidade
            causas_ranqueadas = calcular_rca_matematico(payload)
            
            if not causas_ranqueadas:
                print(f"⚠️ [SCM] Nenhuma causa provável pôde ser isolada matematicamente para {componente_foco}.")
                continue
                
            principal_causa = causas_ranqueadas[0]
            print(f"🏆 [CAUSA RAIZ ELEITA]: {principal_causa['id_causa']} | Confiança: {principal_causa['confidence_score']*100}% | Motivo: {principal_causa['descricao']}")
            
            # 1. Garante a atualização física do subgrafo explicativo no Neo4j
            await app.loop.run_in_executor(None, persistir_grafo_explicativo_sync, alerta_id, principal_causa)
            
            # 2. Montagem do payload de veredito final que alimentará o agente LLM
            veredito_final = {
                "veredito_id": f"VERDICT_{int(time.time())}",
                "alerta_id": str(alerta_id),
                "componente_afetado": str(componente_foco),
                "score_anomalia_original": float(payload.get("score_anomalia_gatilho", 1.0)),
                "causa_raiz_eleita": principal_causa,
                "outros_suspeitos_ranqueados": causas_ranqueadas[1:4], # Envia o top 3 secundário para contexto da LLM
                "snapshot_subgrafo": payload.get("subgrafo_suspeito")
            }
            
            # Despacha para o tópico final que o Agente LLM estará escutando
            await root_cause_verdicts_topic.send(value=veredito_final)
            
        except Exception as e:
            print(f"[ERR-SCM-AGENT] Falha no processamento de inferência causal: {e}")

if __name__ == '__main__':
    app.main()

