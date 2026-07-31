import os
import faust
import time
import traceback
from database import get_neo4j_driver

# Classes oficiais do ecossistema OpenTelemetry para decodificar Traces binários em Protobuf
from opentelemetry.proto.trace.v1 import trace_pb2

# Captura o broker correto ('kafka://kafka:29092' dentro do Docker ou o IP externo no host)
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://kafka:9092")

# Inicialização do Faust
app = faust.App(
    'hadoop-trace-processor',
    broker=KAFKA_URL,
    reply_create_topic=False,
    web_enabled=False,
    value_serializer='raw',
    broker_commit_livelock_soft_timeout=600.0, # Aumenta tolerância para 10 minutosstream_processing_timeout=600.0
)

# O seu tópico de traces configurado no pipeline do otel-collector é "otlp_traces"
trace_topic = app.topic('otlp_traces', value_type=bytes)

# Recupera o driver centralizado do Neo4J
neo4j_driver = get_neo4j_driver()

# Query Cypher em lote corrigida e otimizada (Uso correto de condicionais em blocos separados)
CYPHER_BATCH_TRACES = """
UNWIND $batch AS item

// ============================================================================
// PASSO 1: FLUXO DE JOBS E TASKS (SINTAXE NEO4J 5.26+)
// ============================================================================
CALL (item) {
    WITH item WHERE item.is_job = true
    MERGE (j:job {id_job: toString(item.job_id)})
    ON CREATE SET j.usuario = toString(item.usuario),
                  j.status = "RUNNING",
                  j.start_time = toInteger(item.timestamp)

    MERGE (t:task {id_task: toString(item.task_id)})
    ON CREATE SET t.tipo = toString(item.task_tipo),
                  t.status = "RUNNING"
    SET t.duration_ms = toFloat(item.duration_ms),
        t.status = toString(item.task_status)

    MERGE (j)-[:COMPREENDE]->(t)
    SET j.status = CASE WHEN item.task_status = "FAILED" THEN "FAILED" ELSE j.status END
}

// ============================================================================
// PASSO 2: RESOLUÇÃO DA TOPOLOGIA E FLUXO DE REDE AGREGADO
// ============================================================================
WITH item
OPTIONAL MATCH (comp_dest:componente) WHERE comp_dest.id = toString(item.componente_id)

// Subquery A: Vincula Tasks ao Componente de Destino (NodeManager)
CALL (item, comp_dest) {
    WITH item, comp_dest WHERE comp_dest IS NOT NULL AND item.is_job = true
    MATCH (t:task) WHERE t.id_task = toString(item.task_id)
    MERGE (t)-[r:EXECUTADA_EM]->(comp_dest)
    SET r.trace_id = toString(item.trace_id),
        r.timestamp_vinculo = toInteger(item.timestamp)
}

// Subquery B: Conecta Componente de Origem ao de Destino Agregando Métricas na Aresta
CALL (item, comp_dest) {
    WITH item, comp_dest WHERE comp_dest IS NOT NULL AND item.is_job = false
    
    // Localiza dinamicamente o componente que iniciou a chamada de rede
    MATCH (comp_orig:componente) WHERE comp_orig.id = toString(item.origem_id)
    
    // Cria ou atualiza a aresta de conectividade ponta a ponta
    MERGE (comp_orig)-[r:CHAMA_REDE]->(comp_dest)
    SET r.timestamp_janela = toInteger(item.timestamp),
        r.route = toString(item.http_route),
        r.method = toString(item.http_method),
        r.occurrences = COALESCE(r.occurrences, 0) + 1,
        r.errors = COALESCE(r.errors, 0) + CASE WHEN toInteger(item.status_code) >= 400 THEN 1 ELSE 0 END,
        r.avg_duration_ms = COALESCE(r.avg_duration_ms, 0) * 0.9 + toFloat(item.duration_ms) * 0.1
}
"""


def enviar_lote_traces_sync(lote):
    print(f"[DEBUG NEO4J TRACES] Despachando {len(lote)} spans para o Neo4j (Fluxo Misto)...")
    try:
        with neo4j_driver.session() as session:
            result = session.run(CYPHER_BATCH_TRACES, batch=lote)
            counters = result.consume().counters
            print(f"[DEBUG NEO4J TRACES SUCESSO] Resumo de escrita:\n"
                  f" -> Nós criados: {counters.nodes_created}\n"
                  f" -> Relacionamentos criados: {counters.relationships_created}\n"
                  f" -> Propriedades definidas: {counters.properties_set}")
    except Exception as e:
        print(f"[DEBUG NEO4J TRACES ERRO] Falha na execução da query Cypher: {e}")
        traceback.print_exc()

@app.agent(trace_topic)
async def processar_protobuf_traces(stream):
    async for batch in stream.take(50, within=5.0):
        lote_neo4j = []

        print(f"\n--- [DEBUG TRACES APP] Lote capturado no Kafka contendo {len(batch)} payloads ---")

        for payload_binario in batch:
            try:
                trace_data = trace_pb2.TracesData()
                trace_data.ParseFromString(payload_binario)

                if not trace_data.resource_spans:
                    continue

                for resource_spans in trace_data.resource_spans:
                    resource_attrs = {}
                    for attr in resource_spans.resource.attributes:
                        tipo_val = attr.value.WhichOneof('value')
                        if tipo_val == 'string_value':
                            resource_attrs[attr.key] = attr.value.string_value
                        else:
                            resource_attrs[attr.key] = getattr(attr.value, tipo_val) if tipo_val else None

                    service_name = resource_attrs.get("service.name")
                    full_hostname = resource_attrs.get("host.name") or resource_attrs.get("net.host.name")

                    if not service_name or not full_hostname:
                        continue

                    # Casamento estrito com a topologia estática
                    host_curto = full_hostname.split('.')[0]
                    componente_id = f"{service_name}@{host_curto}".lower()

                    for scope_spans in resource_spans.scope_spans:
                        for span in scope_spans.spans:
                            span_attrs = {}
                            for attr in span.attributes:
                                tipo_val = attr.value.WhichOneof('value')
                                if tipo_val == 'string_value':
                                    span_attrs[attr.key] = attr.value.string_value
                                else:
                                    span_attrs[attr.key] = getattr(attr.value, tipo_val) if tipo_val else None
                            # Tenta capturar o serviço ou host de origem através de tags semânticas do OTel
                            peer_service = span_attrs.get("peer.service") or span_attrs.get("net.peer.name") or span_attrs.get("server.address")
                            if peer_service:
                                host_origem_curto = peer_service.split('.')[0].lower()
                                # Se o peer.service já trouxer o nome limpo do processo, usa ele, senão faz fallback para a app global
                                origem_id = f"{peer_service}" if "@" in peer_service else f"hadoop-apps-global@{host_origem_curto}"
                            else:
                                # Caso não mapeie a origem, assume como uma chamada externa disparada pelo client global
                                origem_id = "hadoop-apps-global@client"
                            job_id = span_attrs.get("hadoop.job.id") or span_attrs.get("hadoop.jobId") or span_attrs.get("mapreduce.job.id")
                            task_id = span_attrs.get("hadoop.task.id") or span_attrs.get("hadoop.taskId") or span_attrs.get("mapreduce.task.id")
                            usuario = span_attrs.get("hadoop.user") or span_attrs.get("hadoop.username") or "hdfs"

                            is_job_span = True if (job_id and task_id) else False

                            http_route = span_attrs.get("http.route") or span_attrs.get("url.path") or span_attrs.get("url.full") or "internal_rpc"
                            http_method = span_attrs.get("http.request.method") or "GET"
                            status_code = span_attrs.get("http.response.status_code") or 0
                            error_type = span_attrs.get("error.type") or "None"

                            start_time_ms = int(span.start_time_unix_nano) / 1_000_000
                            end_time_ms = int(span.end_time_unix_nano) / 1_000_000
                            duration_ms = end_time_ms - start_time_ms if end_time_ms > start_time_ms else 0

                            status_code_otel = span.status.code
                            task_status = "SUCCESS" if status_code_otel == 1 else "RUNNING"
                            if status_code_otel == 2:
                                task_status = "FAILED"
                                if error_type == "None":
                                    error_type = "OTel_Execution_Error"

                            trace_id_hex = span.trace_id.hex()

                            if is_job_span:
                                if "_m_" in str(task_id):
                                    tipo_task = "Map"
                                elif "_r_" in str(task_id):
                                    tipo_task = "Reduce"
                                else:
                                    tipo_task = "Spark/Generic"
                            else:
                                tipo_task = "HTTP_RPC_Call"

                            lote_neo4j.append({
                                "is_job": is_job_span,
                                "job_id": str(job_id) if job_id else "None",
                                "task_id": str(task_id) if task_id else "None",
                                "task_tipo": tipo_task,
                                "task_status": task_status,
                                "usuario": str(usuario),
                                "componente_id": str(componente_id),
                                "origem_id": str(origem_id),
                                "trace_id": str(trace_id_hex),
                                "duration_ms": float(duration_ms),
                                "timestamp": int(start_time_ms),
                                "http_route": str(http_route),
                                "http_method": str(http_method),
                                "status_code": int(status_code),
                                "error_type": str(error_type)
                            })

            except Exception as e:
                print(f"[TRACE CONSUMER ERROR] Falha no parsing do span: {e}")
                continue

        if lote_neo4j:
            try:
                await app.loop.run_in_executor(None, enviar_lote_traces_sync, lote_neo4j)
            except Exception as e:
                print(f"[TRACE CONSUMER] Erro ao despachar executor para Neo4J: {e}")

if __name__ == '__main__':
    app.main()

