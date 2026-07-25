import os
import faust
from database import get_neo4j_driver
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

# Classes oficiais do ecossistema OpenTelemetry para decodificar o formato binário Protobuf
from opentelemetry.proto.logs.v1 import logs_pb2

# Captura o broker correto ('kafka://kafka:29092' dentro do Docker ou o IP externo no host)
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://192.168.3.212:9092")

# Inicialização do Faust otimizada para o ecossistema Docker
app = faust.App(
    'hadoop-log-processor', 
    broker=KAFKA_URL,
    reply_create_topic=False,
    topic_replication_factor=1
)

# O seu tópico de logs configurado no pipeline do otel-collector é "otlp_logs"
log_topic = app.topic('otlp_logs', value_type=bytes)

# Recupera o driver centralizado do Neo4J
neo4j_driver = get_neo4j_driver()

# ============================================================================
# CONFIGURAÇÃO CORRIGIDA DO DRAIN3 (API OFICIAL)
# ============================================================================
config = TemplateMinerConfig()
config.load("drain3.ini")  # Método correto para carregar o arquivo .ini do Hadoop
template_miner = TemplateMiner(config=config) # Parâmetro nomeado explícito para evitar conflitos

# Query Cypher em lote otimizada usando UNWIND para o Neo4J
CYPHER_BATCH_LOGS = """
UNWIND $batch AS item
MATCH (comp:Componente {id: item.componente_id})
MERGE (lt:LogTemplate {id: item.template_id})
ON CREATE SET lt.text_pattern = item.pattern
MERGE (comp)-[r:REGISTROU_LOG {timestamp_janela: item.timestamp_janela}]->(lt)
SET r.occurrences = COALESCE(r.occurrences, 0) + item.count
"""

def enviar_lote_logs(lote):
    with neo4j_driver.session() as session:
        session.run(CYPHER_BATCH_LOGS, batch=lote)

@app.agent(log_topic)
async def processar_protobuf_logs(stream):
    # Coleta registros acumulando uma janela de 10 segundos ou limite de 1000 mensagens
    async for batch in stream.take(1000, within=10.0):
        agregacao_janela = {}
        timestamp_atual = faust.now()

        for payload_binario in batch:
            try:
                # Desserializa o binário do Kafka para o objeto estruturado do OpenTelemetry
                logs_data = logs_pb2.LogsData()
                logs_data.ParseFromString(payload_binario)

                for resource_logs in logs_data.resource_logs:
                    # Cria um dicionário com os atributos de recurso injetados pelos seus processors
                    resource_attrs = {attr.key: attr.value for attr in resource_logs.resource.attributes}
                    
                    # Captura as propriedades definidas no seu config.yaml do OTel
                    service_name_attr = resource_attrs.get("service.name")
                    full_hostname_attr = resource_attrs.get("host.name")
                    
                    if not service_name_attr or not full_hostname_attr:
                        continue
                        
                    service_name = service_name_attr.string_value  # ex: "datanode"
                    full_hostname = full_hostname_attr.string_value # ex: "node1.jobe.net"
                    
                    # Corta o domínio do hostname ("node1.jobe.net" -> "node1") para bater com a topologia estática
                    host_curto = full_hostname.split('.')[0]
                    componente_id = f"{service_name}@{host_curto}" # ex: "datanode@node1"

                    # Varre os registros de log efetivos dentro do escopo do payload
                    for scope_logs in resource_logs.scope_logs:
                        for log_record in scope_logs.log_records:
                            # O corpo da mensagem tratada/combinada pelos seus operadores filelog
                            mensagem_log = log_record.body.string_value
                            
                            if not mensagem_log:
                                continue

                            # Executa a extração do padrão através do Drain3 com suas máscaras customizadas
                            result = template_miner.add_log_message(mensagem_log)
                            template_id = f"TEMPLATE_{result.get('cluster_id')}"
                            pattern = result.get("template_mined")

                            # Agrupa e incrementa o contador local em memória na janela corrente
                            chave = (componente_id, template_id)
                            if chave not in agregacao_janela:
                                agregacao_janela[chave] = {"pattern": pattern, "count": 0}
                            agregacao_janela[chave]["count"] += 1

            except Exception as e:
                print(f"[LOG CONSUMER] Erro na decodificação ou no parsing do log: {e}")
                continue

        # Formata os dados agregados garantindo o desempacotamento seguro das chaves compostas
        lote_neo4j = [
            {
                "componente_id": chave_composta[0],
                "template_id": chave_composta[1],
                "pattern": dados_template["pattern"],
                "count": dados_template["count"],
                "timestamp_janela": int(timestamp_atual * 1000) # Convertido para milissegundos
            } for chave_composta, dados_template in agregacao_janela.items()
        ]

        if lote_neo4j:
            try:
                enviar_lote_logs(lote_neo4j)
            except Exception as e:
                print(f"[LOG CONSUMER] Erro ao persistir lote no Neo4J: {e}")

if __name__ == '__main__':
    app.main()

