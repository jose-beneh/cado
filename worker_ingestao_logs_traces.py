import os
import logging
from datetime import timedelta
import faust
from faust.serializers import codecs  # IMPORTAÇÃO CORRETA PARA REGISTRO DE CODECS
from neo4j import GraphDatabase
from drain3.template_miner import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

# Classes oficiais de desserialização Protobuf do OpenTelemetry
from opentelemetry.proto.logs.v1.logs_pb2 import LogsData
from opentelemetry.proto.trace.v1.trace_pb2 import TracesData

# Configurações de Log da Aplicação Python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURAÇÃO DO PARSER DRAIN3 (FASE 2: FEATURE EXTRACTION)
# ============================================================================
drain_config = TemplateMinerConfig()
config_filename = "drain3.ini"

if os.path.exists(config_filename):
    logger.info(f"Carregando mascaramento Hadoop do arquivo: {config_filename}")
    drain_config.load(config_filename)
else:
    logger.warning(f"Arquivo {config_filename} nao encontrado. Usando padroes do Drain3.")

drain_miner = TemplateMiner(config=drain_config)


# ============================================================================
# CODECS CUSTOMIZADOS PARA DESSERIALIZAR OTLP PROTOBUF NO FAUST
# ============================================================================
class OTLPLogCodec(codecs.Codec):
    """Decodifica bytes binarios OTLP Proto para Dicionarios Python (Logs)"""
    def __init__(self):
        super().__init__()
    
    def _loads(self, s: bytes):  # MÉTODO OFICIAL DE DESSERIALIZAÇÃO DO FAUST
        logs_data = LogsData()
        logs_data.ParseFromString(s)
        parsed_logs = []
        for resource_log in logs_data.resource_logs:
            resource_attrs = {attr.key: (attr.value.string_value or attr.value.int_value) for attr in resource_log.resource.attributes}
            hostname = resource_attrs.get('service.instance.id')
            service_name = resource_attrs.get('service.name')
            for scope_log in resource_log.scope_logs:
                for log_record in scope_log.log_records:
                    log_attrs = {attr.key: attr.value.string_value for attr in log_record.attributes}
                    parsed_logs.append({
                        'hostname': hostname,
                        'service_name': service_name,
                        'message': log_attrs.get('message') or log_record.body.string_value,
                        'severity': log_record.severity_text,
                        'timestamp': log_record.time_unix_nano // 1000000
                    })
        return parsed_logs

    def _dumps(self, value):  # MÉTODO OFICIAL DE SERIALIZAÇÃO DO FAUST
        return str(value).encode('utf-8')


class OTLPTraceCodec(codecs.Codec):
    """Decodifica bytes binarios OTLP Proto para Dicionarios Python (Traces)"""
    def __init__(self):
        super().__init__()
        
    def _loads(self, s: bytes):
        traces_data = TracesData()
        traces_data.ParseFromString(s)
        parsed_spans = []
        for resource_span in traces_data.resource_spans:
            resource_attrs = {attr.key: attr.value.string_value for attr in resource_span.resource.attributes}
            hostname = resource_attrs.get('service.instance.id')
            for scope_span in resource_span.scope_spans:
                for span in scope_span.spans:
                    span_attrs = {attr.key: attr.value.string_value for attr in span.attributes}
                    duration_ms = (span.end_time_unix_nano - span.start_time_unix_nano) // 1000000
                    parsed_spans.append({
                        'hostname': hostname,
                        'trace_id': span.trace_id.hex(),
                        'span_id': span.span_id.hex(),
                        'job_id': span_attrs.get('yarn.application.id') or span_attrs.get('spark.job.id') or 'unknown_job',
                        'task_id': span_attrs.get('spark.task.id') or span_attrs.get('yarn.container.id') or 'unknown_task',
                        'client_service_id': span_attrs.get('client_service_id'), 
                        'server_service_id': span_attrs.get('server_service_id'),
                        'latency_ms': duration_ms,
                        'is_error': int(span.status.code == 2),
                        'timestamp': span.start_time_unix_nano // 1000000
                    })
        return parsed_spans

    def _dumps(self, value):
        return str(value).encode('utf-8')


# CORREÇÃO: Registro dos codecs usando a sintaxe e escopo corretos do Faust
codecs.register('otlp_logs_proto', OTLPLogCodec())
codecs.register('otlp_traces_proto', OTLPTraceCodec())

# ============================================================================
# INICIALIZAÇÃO DO APLICATIVO FAUST
# ============================================================================
app = faust.App(
    'hadoop-multimodal-processor', 
    broker='kafka://192.168.3.212:9092',
    store='rocksdb://'
)

logs_topic = app.topic('otlp_logs', value_type=bytes, key_type=bytes).with_codec(value='otlp_logs_proto')
traces_topic = app.topic('otlp_traces', value_type=bytes, key_type=bytes).with_codec(value='otlp_traces_proto')

logs_window_table = app.Table('logs_aggregation_table', default=int).tumbling(timedelta(minutes=1), expires=timedelta(minutes=5))
traces_window_table = app.Table('traces_aggregation_table', default=dict).tumbling(timedelta(minutes=1), expires=timedelta(minutes=5))

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)


# ============================================================================
# AGENTES DE PROCESSAMENTO (STREAMING)
# ============================================================================
@app.agent(logs_topic)
async def process_logs_stream(logs):
    async for batch_list in logs:
        for log in batch_list:
            hostname = log.get('hostname')
            service_name = log.get('service_name')
            message = log.get('message')
            if not hostname or not service_name or not message:
                continue
            component_id = f"{service_name}@{hostname}"
            result = drain_miner.add_log_message(message)
            template_id = f"DRAIN_{result['cluster_id']}"
            template_pattern = result['template_mined']
            window_key = f"{component_id}||{template_id}||{template_pattern}"
            logs_window_table[window_key] += 1


@logs_window_table.on_window_close
async def on_log_window_close(key: str, value: int, window):
    component_id, template_id, template_pattern = key.split("||")
    window_start = int(window.start * 1000)
    try:
        with neo4j_driver.session() as session:
            cypher_query = "MATCH (comp:Componente {id: $component_id}) MERGE (lt:LogTemplate {id: $template_id}) ON CREATE SET lt.text_pattern = $pattern MERGE (comp)-[r:REGISTROU_LOG {timestamp_janela: $window_start}]->(lt) SET r.occurrences = $occurrences"
            session.run(cypher_query, component_id=component_id, template_id=template_id, pattern=template_pattern, window_start=window_start, occurrences=value)
    except Exception as e:
        logger.error(f"Erro ao persistir lote de logs no Neo4J: {e}")


@app.agent(traces_topic)
async def process_traces_stream(traces):
    async for batch_list in traces:
        for span in batch_list:
            client_service = span.get('client_service_id')
            server_service = span.get('server_service_id')
            if not client_service or not server_service:
                continue
            await save_task_topology(span['job_id'], span['task_id'], client_service, span['trace_id'], span['timestamp'])
            edge_key = f"{client_service}||{server_service}"
            state = traces_window_table[edge_key] or {'rpc_count': 0, 'total_latency': 0.0, 'total_errors': 0}
            state['rpc_count'] += 1
            state['total_latency'] += span['latency_ms']
            state['total_errors'] += span['is_error']
            traces_window_table[edge_key] = state


@traces_window_table.on_window_close
async def on_trace_window_close(key: str, value: dict, window):
    client_service, server_service = key.split("||")
    window_start = int(window.start * 1000)
    rpc_count = value['rpc_count']
    avg_latency = value['total_latency'] / rpc_count if rpc_count > 0 else 0
    error_rate = value['total_errors'] / rpc_count if rpc_count > 0 else 0
    try:
        with neo4j_driver.session() as session:
            cypher_query = "MATCH (src:Componente {id: $src_id}) MATCH (dst:Componente {id: $dst_id}) MERGE (src)-[r:COMMUNICATES_TO]->(dst) SET r.rpc_count_last_window = $rpc_count, r.avg_latency_ms = $avg_latency, r.error_rate = $error_rate, r.last_updated = $window_start"
            session.run(cypher_query, src_id=client_service, dst_id=server_service, rpc_count=rpc_count, avg_latency=avg_latency, error_rate=error_rate, window_start=window_start)
    except Exception as e:
        logger.error(f"Erro ao persistir indicadores de trace no Neo4J: {e}")


async def save_task_topology(job_id, task_id, client_service, trace_id, timestamp):
    if job_id == 'unknown_job': 
        return
    try:
        with neo4j_driver.session() as session:
            cypher_query = "MERGE (j:Job {id_job: $job_id}) MERGE (t:Task {id_task: $task_id}) MERGE (j)-[:COMPREENDE]->(t) WITH t MATCH (nm:Componente {id: $client_service}) MERGE (t)-[:EXECUTADA_EM {trace_id: $trace_id, timestamp: $ts}]->(nm)"
            session.run(cypher_query, job_id=job_id, task_id=task_id, client_service=client_service, trace_id=trace_id, ts=timestamp)

