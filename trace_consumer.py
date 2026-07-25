import os
import faust
from database import get_neo4j_driver

# Classes oficiais do ecossistema OpenTelemetry para decodificar Traces binários em Protobuf
from opentelemetry.proto.trace.v1 import trace_pb2

# Captura o broker correto ('kafka://kafka:29092' dentro do Docker ou o IP externo no host)
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://192.168.3.212:9092")

# Inicialização do Faust apontando para o seu cluster Kafka
app = faust.App(
    'hadoop-trace-processor', 
    broker=KAFKA_URL,
    reply_create_topic=False,
    topic_replication_factor=1
)

# O seu tópico de traces configurado no pipeline do otel-collector é "otlp_traces"
trace_topic = app.topic('otlp_traces', value_type=bytes)

# Recupera o driver centralizado do Neo4J
neo4j_driver = get_neo4j_driver()

# Query Cypher otimizada utilizando UNWIND para vincular dinamicamente a execução à infraestrutura
CYPHER_BATCH_TRACES = """
UNWIND $batch AS item
// Garante que o nó raiz do Job exista
MERGE (j:Job {id_job: item.job_id})
ON CREATE SET j.usuario = item.usuario, j.status = "RUNNING", j.start_time = item.timestamp

// Garante o ciclo de vida e o estado da Task mapeada
MERGE (t:Task {id_task: item.task_id})
ON CREATE SET t.tipo = item.task_tipo, t.status = "RUNNING"

// Estrutura o relacionamento acoplado Job -> Task
MERGE (j)-[:COMPREENDE]->(t)

// Localiza o NodeManager físico correspondente onde a task rodou e cria o link contextualizado
WITH t, j, item
MATCH (nm:Componente {id: item.nodemanager_id})
MERGE (t)-[r:EXECUTADA_EM]->(nm)
SET r.trace_id = item.trace_id,
    r.timestamp_vinculo = item.timestamp,
    t.duration_ms = item.duration_ms,
    t.status = item.task_status
    
// Caso alguma task falhe, atualiza preventivamente o status macro do Job no grafo
WITH j, item
WHERE item.task_status = "FAILED"
SET j.status = "FAILED"
"""

def enviar_lote_traces(lote):
    with neo4j_driver.session() as session:
        session.run(CYPHER_BATCH_TRACES, batch=lote)

@app.agent(trace_topic)
async def processar_protobuf_traces(stream):
    # Janela de agregação e envio de 10 segundos ou limite de 500 Spans
    async for batch in stream.take(500, within=10.0):
        lote_neo4j = []

        for payload_binario in batch:
            try:
                # Desserializa os bytes vindos do Kafka no modelo OTLP Traces
                trace_data = trace_pb2.TracesData()
                trace_data.ParseFromString(payload_binario)

                for resource_spans in trace_data.resource_spans:
                    resource_attrs = {attr.key: attr.value for attr in resource_spans.resource.attributes}
                    
                    # Captura o host mapeado pelo detector do OpenTelemetry
                    full_hostname_attr = resource_attrs.get("host.name")
                    if not full_hostname_attr:
                        continue
                        
                    full_hostname = full_hostname_attr.string_value
                    host_curto = full_hostname.split('.')[0]
                    nodemanager_id = f"nodemanager@{host_curto}" # ex: "nodemanager@node1"

                    for scope_spans in resource_spans.scope_spans:
                        for span in scope_spans.spans:
                            # Converte o array de chaves/valores do span para um dicionário Python
                            span_attrs = {attr.key: attr.value for attr in span.attributes}
                            
                            # Extrai identificadores de negócio injetados pela instrumentação javaagent do Hadoop
                            job_id_attr = span_attrs.get("hadoop.job.id")       # ex: application_1700000000_0001
                            task_id_attr = span_attrs.get("hadoop.task.id")     # ex: task_1700000000_0001_m_000001
                            usuario_attr = span_attrs.get("hadoop.user")
                            
                            # Ignora spans puramente operacionais/internos que não carregam metadados de negócio do Hadoop
                            if not job_id_attr or not task_id_attr:
                                continue

                            job_id = job_id_attr.string_value
                            task_id = task_id_attr.string_value
                            usuario = usuario_attr.string_value if usuario_attr else "hdfs"

                            # Conversão dos carimbos de tempo nativos (Unix Nano) para Milissegundos
                            start_time_ms = int(span.start_time_unix_nano) / 1_000_000
                            end_time_ms = int(span.end_time_unix_nano) / 1_000_000
                            duration_ms = end_time_ms - start_time_ms if end_time_ms > start_time_ms else 0

                            # Traduz o código numérico de Status do OTLP para strings de negócio do seu Schema
                            # No Protobuf OTel: 1 = Ok (Success), 2 = Error (Failed)
                            status_code = span.status.code
                            task_status = "SUCCESS" if status_code == 1 else "RUNNING"
                            if status_code == 2:
                                task_status = "FAILED"

                            # Captura o Trace ID em formato hexadecimal legível
                            trace_id_hex = span.trace_id.hex()

                            lote_neo4j.append({
                                "job_id": job_id,
                                "task_id": task_id,
                                "task_tipo": "Map" if "_m_" in task_id else "Reduce",
                                "task_status": task_status,
                                "usuario": usuario,
                                "nodemanager_id": nodemanager_id,
                                "trace_id": trace_id_hex,
                                "duration_ms": duration_ms,
                                "timestamp": int(start_time_ms)
                            })

            except Exception as e:
                print(f"[TRACE CONSUMER] Erro ao decodificar Protobuf de trace: {e}")
                continue

        if lote_neo4j:
            try:
                enviar_lote_traces(lote_neo4j)
            except Exception as e:
                print(f"[TRACE CONSUMER] Erro ao persistir lote no Neo4J: {e}")

if __name__ == '__main__':
    app.main()

