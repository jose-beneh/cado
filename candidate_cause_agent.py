import os
import json
import time
import traceback
import faust
from database import get_neo4j_driver

# Configuração do Broker do Kafka
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://kafka:29092")

app = faust.App(
    'hadoop-candidate-cause-agent',
    broker=KAFKA_URL,
    reply_create_topic=False,
    value_serializer='json'
)

# Tópicos de Ingestão (Alerta) e Amostragem (Saída enriquecida)
anomaly_events_topic = app.topic('anomaly_events', value_type=dict)
candidate_causes_topic = app.topic('candidate_causes', value_type=dict)

neo4j_driver = get_neo4j_driver()

# ============================================================================
# QUERY CYPHER: EXTRAÇÃO DE CONTEXTO E VIZINHANÇA DE FALHAS
# ============================================================================
# Esta query busca o componente anômalo, localiza seus logs recentes e varre a 
# vizinhança de rede e dependências estruturais num raio de 1 a 2 saltos.
CYPHER_FIND_CANDIDATES = """
MATCH (target:componente {id: $componente_id})

// 1. Coleta de Logs Recentes do próprio componente afetado
OPTIONAL MATCH (target)-[r_log:REGISTROU_LOG]->(lt:logtemplate)
WHERE r_log.last_seen >= $window_start
WITH target, collect({
  template_id: lt.id,
  pattern: lt.text_pattern,
  occurrences: r_log.occurrences
}) AS logs_locais

// 2. Coleta de Dependências Diretas (Ex: Se o DataNode depende do NameNode)
OPTIONAL MATCH (target)-[r_dep:DEPENDS_ON]->(dep:componente)
WITH target, logs_locais, collect({
  componente_id: dep.id,
  tipo: r_dep.tipo_dependencia,
  status_atual: dep.status
}) AS dependencias_estruturais

// 3. Coleta de Comportamento de Rede com Vizinhos (Quem ele chama ou quem o chama)
OPTIONAL MATCH (vizinho:componente)-[r_rede:CHAMA_REDE]->(target)
WHERE r_rede.last_seen >= $window_start
WITH target, logs_locais, dependencias_estruturais, collect({
  origem: vizinho.id,
  destino: target.id,
  erros_rede: r_rede.errors,
  latencia_media: r_rede.avg_duration_ms,
  occurrences: r_rede.occurrences
}) AS fluxo_rede_entrada

OPTIONAL MATCH (target)-[r_rede_out:CHAMA_REDE]->(vizinho_out:componente)
WHERE r_rede_out.last_seen >= $window_start
WITH target, logs_locais, dependencias_estruturais, fluxo_rede_entrada, collect({
  origem: target.id,
  destino: vizinho_out.id,
  erros_rede: r_rede_out.errors,
  latencia_media: r_rede_out.avg_duration_ms,
  occurrences: r_rede_out.occurrences
}) AS fluxo_rede_saida

RETURN {
  id: target.id,
  nome: target.nome,
  status: target.status,
  logs: logs_locais,
  dependencias: dependencias_estruturais,
  rede_entrada: fluxo_rede_entrada,
  rede_saida: fluxo_rede_saida
} AS subgrafo_contexto
"""

def interrogar_neo4j_sync(componente_id, timestamp_alerta):
    # Define uma janela retroativa de 60 segundos com base no momento do alerta
    window_start = int(timestamp_alerta) - 60000 
    
    try:
        with neo4j_driver.session() as session:
            result = session.run(
                CYPHER_FIND_CANDIDATES, 
                componente_id=str(componente_id), 
                window_start=int(window_start)
            )
            record = result.single()
            if record:
                return record["subgrafo_contexto"]
            return None
    except Exception as e:
        print(f"[NEO4J CANDIDATE ERR] Erro ao buscar subgrafo: {e}")
        return None

# ============================================================================
# AGENTE FAUST: INVESTIGADOR DE CAUSAS CANDIDATAS
# ============================================================================
@app.agent(anomaly_events_topic)
async def investigar_causas_candidatas(stream):
    async for alerta in stream:
        try:
            componente_id = alerta.get("componente_target")
            timestamp_alerta = alerta.get("timestamp")
            alerta_id = alerta.get("id")

            if not componente_id:
                print(f"[CANDIDATE LOGIC] Alerta rejeitado. Sem componente_target válido.")
                continue

            print(f"\n🔍 [INVESTIGAÇÃO INICIADA] Alerta {alerta_id} recebido para: {componente_id}")

            # Despacha a busca síncrona no Neo4j para o pool de threads do Faust
            subgrafo = await app.loop.run_in_executor(
                None, 
                interrogar_neo4j_sync, 
                componente_id, 
                timestamp_alerta
            )

            if not subgrafo:
                print(f"❌ [CANDIDATE LOGIC] Nenhum contexto topológico encontrado no Neo4j para {componente_id}")
                continue

            # Montagem do Payload Enriquecido da Causa Candidata
            payload_causa_candidata = {
                "causa_candidata_id": f"CANDIDATE_{alerta_id.split('_')[-1]}",
                "alerta_origem_id": str(alerta_id),
                "timestamp_investigacao": int(time.time() * 1000),
                "componente_foco": str(componente_id),
                "score_anomalia_gatilho": float(alerta.get("anomaly_score", 1.0)),
                "detalhes_vetor_gatilho": alerta.get("detalhes_vetor", {}),
                "subgrafo_suspeito": subgrafo
            }

            print(f"📦 [SUBGRAFO EXTRAÍDO] Sucesso! Encontrados {len(subgrafo['logs'])} templates de logs e {len(subgrafo['dependencias'])} dependências lógicas.")
            print(f"🚀 Despachando causas candidatas para o tópico 'candidate_causes'...")
            
            # Envia o subgrafo de suspeitos para a Fase 6 realizar a Inferência Causal matemática
            await candidate_causes_topic.send(value=payload_causa_candidata)

        except Exception as e:
            print(f"[ERR-CANDIDATE-AGENT] Falha na esteira de investigação: {e}")
            traceback.print_exc()

if __name__ == '__main__':
    app.main()

