import faust
import neo4j
import json
import logging
from datetime import datetime
import numpy as np
from sklearn.ensemble import IsolationForest

# Importações oficiais do OpenTelemetry Protobuf
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.trace.v1 import trace_pb2
from opentelemetry.proto.metrics.v1 import metrics_pb2

# ==============================================================================
# ⚙️ CONFIGURAÇÕES E INICIALIZAÇÃO
# ==============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = faust.App(
    'faust-hadoop-processor', 
    broker='kafka://192.168.3.212:9092',
    topic_allow_declare=False,
    topic_disable_leader_election=True,
    producer_max_request_size=5242880,
    max_buffer_size=50000,
    broker_max_poll_records=1000
)
app.conf.broker_client_credentials = {'api_version': '3.4.0'}

# Tópicos de Origem (Bytes brutos)
logs_topic = app.topic('otlp_logs', value_type=bytes, value_serializer='raw')
traces_topic = app.topic('otlp_traces', value_type=bytes, value_serializer='raw')
metrics_topic = app.topic('otlp_metrics', value_type=bytes, value_serializer='raw')

# Tópico de Destino para Anomalias Detectadas (JSON)
detected_anomalies_topic = app.topic('detected_anomalies', value_type=json.dumps)

# 📊 TABELA MULTIMODAL EM MEMÓRIA (Janela de 10 segundos)
# Estrutura do dicionário por componente: 
# {"log_errors": int, "trace_latencies": list, "cpu_usage": float, "disk_usage": float}
multimodal_table = app.Table('multimodal_table', default=dict).tumbling(10.0, expires=10.0)

# 🧠 INICIALIZAÇÃO DO MODELO ISOLATION FOREST (Modelo Leve)
# Em produção, você carregaria um modelo pré-treinado via joblib. 
# Aqui, inicializamos com contaminação estimada para detecção não supervisionada online.
clf = IsolationForest(contamination=0.05, random_state=42)
# Ajuste inicial fitício (apenas para inicializar os coeficientes do modelo)
clf.fit([[0, 0, 10.0, 20.0], [1, 50, 12.0, 22.0], [0, 5, 15.0, 25.0]])

# ==============================================================================
# 📥 INGESTÃO DE MÉTRICAS (Push das Séries Temporais para Memória)
# ==============================================================================
@app.agent(metrics_topic)
async def watch_metrics(stream):
    async for raw_message in stream:
        try:
            metrics_data = metrics_pb2.MetricsData()
            metrics_data.ParseFromString(raw_message)
            
            for rm in metrics_data.resource_metrics:
                attrs = {a.key: a.value.string_value for a in rm.resource.attributes}
                component_id = f"{attrs.get('service.name', '').lower()}@{attrs.get('host.name', '').split('.')[0]}"
                
                # Recupera ou inicializa o mapa do componente na janela atual
                state = multimodal_table[component_id].current() or {"log_errors": 0, "trace_latencies": [], "cpu_usage": 0.0, "disk_usage": 0.0}
                
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        # Extrai métricas específicas do Hadoop/VM
                        if metric.name == "system.cpu.utilization":
                            # Pega o último ponto de dado gerado
                            state["cpu_usage"] = metric.gauge.data_points[-1].as_double * 100
                        elif metric.name == "system.disk.utilization":
                            state["disk_usage"] = metric.gauge.data_points[-1].as_double * 100
                
                multimodal_table[component_id] = state
        except Exception as e:
            logger.error(f"Erro no processamento de métricas: {e}")

# ==============================================================================
# 📥 INGESTÃO DE LOGS & EXECUÇÃO DO MODELO (Isolation Forest)
# ==============================================================================
@app.agent(logs_topic)
async def watch_logs(stream):
    async for raw_message in stream:
        try:
            logs_data = logs_pb2.LogsData()
            logs_data.ParseFromString(raw_message)
            
            for rl in logs_data.resource_logs:
                attrs = {a.key: a.value.string_value for a in rl.resource.attributes}
                component_id = f"{attrs.get('service.name', '').lower()}@{attrs.get('host.name', '').split('.')[0]}"
                
                state = multimodal_table[component_id].current() or {"log_errors": 0, "trace_latencies": [], "cpu_usage": 0.0, "disk_usage": 0.0}
                
                for sl in rl.scope_logs:
                    for record in sl.log_records:
                        if record.severity_text in ['ERROR', 'FATAL']:
                            state["log_errors"] += 1
                
                multimodal_table[component_id] = state
                
                # 🧠 AVALIAÇÃO DA ANOMALIA COM ISOLATION FOREST
                await avaliar_anomalia_online(component_id, state)
        except Exception as e:
            logger.error(f"Erro no processamento de logs: {e}")

# ==============================================================================
# 📥 INGESTÃO DE TRACES
# ==============================================================================
@app.agent(traces_topic)
async def watch_traces(stream):
    async for raw_message in stream:
        try:
            traces_data = trace_pb2.TracesData()
            traces_data.ParseFromString(raw_message)
            
            for rs in traces_data.resource_spans:
                attrs = {a.key: a.value.string_value for a in rs.resource.attributes}
                component_id = f"{attrs.get('service.name', '').lower()}@{attrs.get('host.name', '').split('.')[0]}"
                
                state = multimodal_table[component_id].current() or {"log_errors": 0, "trace_latencies": [], "cpu_usage": 0.0, "disk_usage": 0.0}
                
                for ss in rs.scope_spans:
                    for span in ss.spans:
                        duration_ms = (span.end_time_unix_nano - span.start_time_unix_nano) / 1_000_000
                        state["trace_latencies"].append(duration_ms)
                
                multimodal_table[component_id] = state
        except Exception as e:
            logger.error(f"Erro no processamento de traces: {e}")

# ==============================================================================
# 🧠 FUNÇÃO DE INFERÊNCIA E NOTIFICAÇÃO (MENSAGERIA KAFKA)
# ==============================================================================
async def avaliar_anomalia_online(component_id, state):
    # Calcula a feature latência p95 a partir da lista coletada na janela
    latencies = state.get("trace_latencies", [])
    p95_latency = np.percentile(latencies, 95) if latencies else 0.0
    
    # 💥 MONTAGEM DO VETOR MULTIMODAL: [logs, traces, cpu, disk]
    features = [
        state.get("log_errors", 0),
        p95_latency,
        state.get("cpu_usage", 0.0),
        state.get("disk_usage", 0.0)
    ]
    
    # Predição: Isolation Forest retorna -1 para anomalia e 1 para normal
    prediction = clf.predict([features])[0]
    
    if prediction == -1:
        logger.warning(f"🚨 Isolation Forest detectou comportamento anômalo em {component_id}!")
        
        # Cria o payload estruturado para a fila de incidentes
        anomaly_payload = {
            "anomaly_id": f"anom_if_{int(datetime.utcnow().timestamp())}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "component_id": component_id,
            "detector": "Isolation_Forest_Multimodal",
            "features_snapshot": {
                "log_errors_10s": features[0],
                "p95_latency_ms": features[1],
                "cpu_utilization_pct": features[2],
                "disk_utilization_pct": features[3]
            },
            "status": "UNTREATED"
        }
        
        # 📨 Envia para o tópico do Kafka. O Agente e o Neo4j lerão daqui futuramente.
        await detected_anomalies_topic.send(value=anomaly_payload)

if __name__ == '__main__':
    app.main()

