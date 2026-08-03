import os
import faust
import time
import traceback
from database import get_neo4j_driver
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

# Classes oficiais do ecossistema OpenTelemetry para decodificar o formato binário Protobuf
from opentelemetry.proto.logs.v1 import logs_pb2

# Captura o broker correto ('kafka://kafka:29092' dentro do Docker ou o IP externo no host)
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://kafka:9092")

# Inicialização do Faust otimizada para o ecossistema Docker
app = faust.App(
    'hadoop-log-processor',
    broker=KAFKA_URL,
    reply_create_topic=False,
    value_serializer='raw'
)

# O tópico de logs configurado no pipeline do otel-collector é "otlp_logs"
log_topic = app.topic('otlp_logs', value_type=bytes)

# Recupera o driver centralizado do Neo4J
neo4j_driver = get_neo4j_driver()

# ============================================================================
# CONFIGURAÇÃO DO DRAIN3 (API OFICIAL)
# ============================================================================
config = TemplateMinerConfig()
config.load("drain3.ini")  
template_miner = TemplateMiner(config=config) 

# Query Cypher otimizada e tipada para garantir o casamento exato de strings no MATCH
CYPHER_BATCH_LOGS = """
UNWIND $batch AS item
MATCH (comp:componente) WHERE comp.id = toString(item.componente_id)
MERGE (lt:logtemplate {id: toString(item.template_id)})
  ON CREATE SET lt.text_pattern = toString(item.pattern)
MERGE (comp)-[r:REGISTROU_LOG]->(lt)
SET r.timestamp_janela = toInteger(item.timestamp_janela),
    r.occurrences = COALESCE(r.occurrences, 0) + toInteger(item.count)
"""

def enviar_lote_logs_sync(lote):
    print(f"[DEBUG NEO4J] Enviando lote com {len(lote)} itens agrupados para o Neo4j...")
    try:
        with neo4j_driver.session() as session:
            result = session.run(CYPHER_BATCH_LOGS, batch=lote)
            counters = result.consume().counters
            print(f"[DEBUG NEO4J ÉXITO] Resumo: Nós criados: {counters.nodes_created}, Relacionamentos: {counters.relationships_created}, Propriedades definidas: {counters.properties_set}")
    except Exception as e:
        print(f"[DEBUG NEO4J ERRO] Falha na execução da query Cypher: {e}")

@app.agent(log_topic)
async def processar_protobuf_logs(stream):
    async for batch in stream.take(1000, within=10.0):
        agregacao_janela = {}
        timestamp_atual = time.time()
        
        print(f"\n--- [DEBUG BATCH START] Processando {len(batch)} payloads brutos do Kafka ---")

        total_log_records_processados = 0
        total_payloads_com_sucesso = 0

        for idx, payload_binario in enumerate(batch):
            try:
                logs_data = logs_pb2.LogsData()
                logs_data.ParseFromString(payload_binario)

                if not logs_data.resource_logs:
                    continue

                total_payloads_com_sucesso += 1

                for resource_logs in logs_data.resource_logs:
                    resource_attrs = {}
                    for attr in resource_logs.resource.attributes:
                        tipo_val = attr.value.WhichOneof('value')
                        if tipo_val == 'string_value':
                            resource_attrs[attr.key] = attr.value.string_value
                        else:
                            resource_attrs[attr.key] = getattr(attr.value, tipo_val) if tipo_val else None

                    service_name = resource_attrs.get("service.name")
                    full_hostname = resource_attrs.get("host.name") or resource_attrs.get("net.host.name")

                    if not service_name or not full_hostname:
                        continue

                    # Corta o domínio do hostname ("node1.jobe.net" -> "node1") para bater com a topologia estática
                    host_curto = full_hostname.split('.')[0]
                    componente_id = f"{service_name}@{host_curto}".lower()

                    for scope_logs in resource_logs.scope_logs:
                        for log_record in scope_logs.log_records:
                            total_log_records_processados += 1
                            
                            # 1. Extração do corpo da mensagem
                            mensagem_log = ""
                            for attr in log_record.attributes:
                                if attr.key == "message":
                                    tipo_val = attr.value.WhichOneof('value')
                                if tipo_val == 'string_value':
                                    mensagem_log = attr.value.string_value
                                break 
                            # 2. Fallback Seguro: Se a tag message nao existir, extrai do corpo do log bruto
                            if not mensagem_log:
                                if log_record.body.HasField("string_value"):
                                    mensagem_log = log_record.body.string_value
                                elif log_record.body.HasField("int_value"):
                                    mensagem_log = str(log_record.body.int_value)
                                else:
                                    tipo_corpo = log_record.body.WhichOneof('value')
                                    if tipo_corpo:
                                        mensagem_log = str(getattr(log_record.body, tipo_corpo))

                            if not str(mensagem_log).strip():
                                continue

                            # 2. Processamento com Drain3
                            result = template_miner.add_log_message(str(mensagem_log))

                            if not result:
                                continue

                            if isinstance(result, dict):
                                cluster_id = result.get('cluster_id')
                                pattern = result.get('template')
                            else:
                                cluster_id = getattr(result, 'cluster_id', None)
                                pattern = getattr(result, 'template', None)

                            if cluster_id is None:
                                continue

                            # Fallback de propriedades para repetição (pattern=None)
                            if not pattern:
                                try:
                                    cluster_obj = template_miner.id_to_cluster.get(cluster_id)
                                    if cluster_obj:
                                        if hasattr(cluster_obj, "get_template"):
                                            pattern = cluster_obj.get_template()
                                        if not pattern and hasattr(cluster_obj, "template_str"):
                                            pattern = cluster_obj.template_str
                                        if not pattern and hasattr(cluster_obj, "log_template_tokens"):
                                            pattern = " ".join(cluster_obj.log_template_tokens)
                                except Exception:
                                    pattern = None

                            # SALVAGUARDA CONTRA DESCARTE SILENCIOSO: Garante o padrão mesmo com limitação da API
                            if not pattern or str(pattern).strip() == "":
                                pattern = str(mensagem_log).strip()
                                if len(pattern) > 150:
                                    pattern = pattern[:147] + "..."

                            template_id = f"TEMPLATE_{cluster_id}"

                            # Agregação em memória
                            chave = (componente_id, template_id)
                            if chave not in agregacao_janela:
                                agregacao_janela[chave] = {"pattern": pattern, "count": 0}
                            agregacao_janela[chave]["count"] += 1

            except Exception as e:
                print(f"  [DEBUG ERRO INTERNAL] Falha ao processar registro individual: {e}")
                continue

        print(f"[DEBUG BATCH METRICS] Fim do Lote: {total_payloads_com_sucesso}/{len(batch)} payloads decodificados com sucesso. {total_log_records_processados} linhas inspecionadas.")
        print(f"[DEBUG MAP AGGREGATION] Registros agrupados gerados para o Neo4j: {len(agregacao_janela)} combinações distintas.")

        lote_neo4j = [
            {
                "componente_id": str(comp_id),
                "template_id": str(temp_id),
                "pattern": str(dados_template["pattern"]),
                "count": int(dados_template["count"]),
                "timestamp_janela": int(timestamp_atual * 1000)
            } for (comp_id, temp_id), dados_template in agregacao_janela.items()
        ]

        if lote_neo4j:
            try:
                await app.loop.run_in_executor(None, enviar_lote_logs_sync, lote_neo4j)
            except Exception as e:
                print(f"[DEBUG EXECUTOR ERRO] Falha ao despachar thread para Neo4j: {e}")

if __name__ == '__main__':
    app.main()

