import os
import time
import json
import traceback
import faust
from collections import defaultdict
from river import anomaly
from river import preprocessing


### Classes oficiais do ecossistema OpenTelemetry para decodificação Protobuf

from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.trace.v1 import trace_pb2
from opentelemetry.proto.metrics.v1 import metrics_pb2

### Captura do Broker do Kafka configurado no ambiente Docker

KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://kafka:29092")

### Inicialização da aplicação Faust dedicada para detecção de anomalias

### Mudamos para 'raw' global pois os 3 tópicos de entrada agora são Protobuf binários

app = faust.App(
    'hadoop-anomaly-detector',
    broker=KAFKA_URL,
    reply_create_topic=False,
    value_serializer='raw'
)

### Definição dos 3 tópicos de entrada consumidos diretamente da telemetria OpenTelemetry

log_topic = app.topic('otlp_logs', value_type=bytes)
trace_topic = app.topic('otlp_traces', value_type=bytes)
metric_topic = app.topic('otlp_metrics', value_type=bytes)

### Tópico centralizado de saída para alertas estruturados (Usa JSON)

anomaly_events_topic = app.topic('anomaly_events', value_type='json')

### ============================================================================
### TABELA DE JANELA EM MEMÓRIA MULTIMODAL COMPLETA
### ============================================================================

janela_multimodal_table = app.Table(
    'janela_multimodal_features_v4', default=lambda: {
        "log_count": 0,
        "log_errors": 0,
        "trace_count": 0,
        "trace_errors": 0,
        "trace_avg_duration": 0.0,
        "infra_cpu_utilization": 0.0,  # Nova feature de métrica integrada
        "last_update": 0
    }
)

### Fábrica de modelos HST isolados por componente para evitar contaminação

#def criar_novo_hst():
#    return anomaly.HalfSpaceTrees(n_trees=35, height=6, window_size=250, seed=42)

def criar_novo_hst():
    return preprocessing.StandardScaler() | anomaly.HalfSpaceTrees(
        n_trees=35, 
        height=6, 
        window_size=250, 
        seed=42
    )


modelos_componentes = defaultdict(criar_novo_hst)
THRESHOLD_ANOMALIA = 0.75

### ============================================================================
### AGENTE 1: CONSUMIDOR E DECODIFICADOR DE LOGS (PROTOBUF)
### ============================================================================

@app.agent(log_topic)
async def processar_protobuf_logs(stream):
    async for payload_binario in stream:
        try:
            logs_data = logs_pb2.LogsData()
            logs_data.ParseFromString(payload_binario)

            for resource_logs in logs_data.resource_logs:
                resource_attrs = {attr.key: attr.value.string_value for attr in resource_logs.resource.attributes if attr.value.HasField('string_value')}
                service_name = resource_attrs.get("service.name")
                full_hostname = resource_attrs.get("host.name") or resource_attrs.get("net.host.name")
        
                if not service_name or not full_hostname: continue
                
                host_curto = full_hostname.split('.')[0]
                componente_id = f"{service_name}@{host_curto}".lower()
                estado = janela_multimodal_table[componente_id]
        
                for scope_logs in resource_logs.scope_logs:
                    for log_record in scope_logs.log_records:
                        estado["log_count"] += 1
                        if log_record.severity_text in ["ERROR", "FATAL", "CRITICAL"]:
                            estado["log_errors"] += 1
                    
                estado["last_update"] = int(time.time())
                janela_multimodal_table[componente_id] = estado
        except Exception as e:
            print(f"[ERR-STREAM-LOGS] {e}")

### ============================================================================
### AGENTE 2: CONSUMIDOR E DECODIFICADOR DE TRACES (PROTOBUF)
### ============================================================================

@app.agent(trace_topic)
async def processar_protobuf_traces(stream):
    async for payload_binario in stream:
        try:
            trace_data = trace_pb2.TracesData()
            trace_data.ParseFromString(payload_binario)

            for resource_spans in trace_data.resource_spans:
                resource_attrs = {attr.key: attr.value.string_value for attr in resource_spans.resource.attributes if attr.value.HasField('string_value')}
                service_name = resource_attrs.get("service.name")
                full_hostname = resource_attrs.get("host.name") or resource_attrs.get("net.host.name")
                
                if not service_name or not full_hostname: continue
                host_curto = full_hostname.split('.')[0]
                componente_id = f"{service_name}@{host_curto}".lower()
                estado = janela_multimodal_table[componente_id]
                
                for scope_spans in resource_spans.scope_spans:
                    for span in scope_spans.spans:
                        estado["trace_count"] += 1
                        if span.status.code == 2:  # STATUS_CODE_ERROR do OTel
                            estado["trace_errors"] += 1
                        
                        start_ms = span.start_time_unix_nano // 1_000_000
                        end_ms = span.end_time_unix_nano // 1_000_000
                        duracao = float(end_ms - start_ms) if end_ms > start_ms else 0.0
                        
                        estado["trace_avg_duration"] = duracao if estado["trace_avg_duration"] == 0.0 else (estado["trace_avg_duration"] * 0.8) + (duracao * 0.2)
                        
                estado["last_update"] = int(time.time())
                janela_multimodal_table[componente_id] = estado
        except Exception as e:
            print(f"[ERR-STREAM-TRACES] {e}")

### ============================================================================
### AGENTE 3: CONSUMIDOR E DECODIFICADOR DE MÉTRICAS (PROTOBUF NATIVO)
### ============================================================================

@app.agent(metric_topic)
async def processar_protobuf_metricas(stream):
    async for payload_binario in stream:
        try:
            metrics_data = metrics_pb2.MetricsData()
            metrics_data.ParseFromString(payload_binario)

            for resource_metrics in metrics_data.resource_metrics:
                resource_attrs = {attr.key: attr.value.string_value for attr in resource_metrics.resource.attributes if attr.value.HasField('string_value')}
                service_name = resource_attrs.get("service.name")
                full_hostname = resource_attrs.get("host.name") or resource_attrs.get("net.host.name")
                
                if not service_name or not full_hostname: continue
                
                host_curto = full_hostname.split('.')[0]
                componente_id = f"{service_name}@{host_curto}".lower()
                estado = janela_multimodal_table[componente_id]
                
                for scope_metrics in resource_metrics.scope_metrics:
                    for metric in scope_metrics.metrics:
                        ### Monitora métricas de JVM injetadas pelo seu JMX receiver (Config.yaml)
                        if "jvm_memory_bytes_used" in metric.name or "jvm.memory.use" in metric.name:
                            ### Extrai o ponto de dado mais recente da série
                            if metric.HasField("sum"):
                                data_points = metric.sum.data_points
                            elif metric.HasField("gauge"):
                                data_points = metric.gauge.data_points
                            else: continue

                            if data_points:
                                valor_recente = data_points[-1].as_double if data_points[-1].HasField("as_double") else float(data_points[-1].as_int)

                        ### Transforma em uma métrica de saturação aproximada (guardando o valor bruto ou normalizado)
                        # Para o HST aprender padrões, dividimos por 1.000.000 para ler em MB e não estourar a escala do vetor
                        estado["infra_cpu_utilization"] = float(valor_recente / 1_000_000)
                            
                estado["last_update"] = int(time.time())
                janela_multimodal_table[componente_id] = estado

        except Exception as e:
            print(f"[ERR-STREAM-METRICS] {e}")

### ============================================================================
### TIMER ASSÍNCRONO: AVALIAÇÃO DO MODELO HST POR JANELA DE TEMPO
### ============================================================================

@app.timer(interval=30.0)
async def avaliar_anomalias_streaming():
    print(f"\n--- [DETECTOR HST MULTIMODAL COMPLETO] Avaliando 3 Fontes do Kafka ---")
    timestamp_atual = int(time.time())
    
    for componente_id, features in janela_multimodal_table.pairs():
        try:
            if timestamp_atual - features["last_update"] > 120: continue

            ### 1. Construção do Vetor Verdadeiramente Multimodal com as 3 Fontes
            vetor_caracteristicas = {
                "infra_resource": float(features["infra_cpu_utilization"]), # Fonte 1: Métricas
                "log_count": float(features["log_count"]),                  # Fonte 2: Logs Volumétricos
                "log_errors": float(features["log_errors"]),                # Fonte 2: Logs Críticos
                "trace_count": float(features["trace_count"]),              # Fonte 3: Traces Volumétricos
                "trace_errors": float(features["trace_errors"]),            # Fonte 3: Traces Críticos
                "trace_avg_duration": float(features["trace_avg_duration"]) # Fonte 3: Latência
            }

            model_hst = modelos_componentes[componente_id]
            anomaly_score = model_hst.score_one(vetor_caracteristicas)
            model_hst.learn_one(vetor_caracteristicas)

            print(f"[HST] {componente_id} -> Vetor Multimodal: {[round(v,2) for v in vetor_caracteristicas.values()]} -> Score: {anomaly_score:.4f}")
            
            if anomaly_score >= THRESHOLD_ANOMALIA:
                evento_alerta = {
                    "id": f"ANOMALIA_STREAM_{componente_id.replace('@', '')}{int(time.time())}",
                    "timestamp": int(time.time() * 1000),
                    "origem": "streaming_hst_detector",
                    "componente_target": str(componente_id),
                    "anomaly_score": float(anomaly_score),
                    "severity": "CRITICAL" if features["log_errors"] > 0 or features["trace_errors"] > 0 else "WARNING",
                    "status": "active",
                    "metric_or_log_template": "Multimodal Stream Vector Drift",
                    "detalhes_vetor": vetor_caracteristicas
                }

                print(f"[ALERTA DISPARADO] Inconsistência Multimodal Detectada em {componente_id}!")

                # Serializa manualmente para JSON para publicar no tópico de saída
                await anomaly_events_topic.send(value=json.dumps(evento_alerta).encode('utf-8'))

            # Reset Parcial de Janela para a próxima rodada de 30 segundos
            features["log_errors"] = 0
            features["trace_count"] = 0
            features["trace_errors"] = 0
            features["log_count"] = 0
            
            # Mantemos infra_cpu_utilization e trace_avg_duration para preservar o último estado lido
            janela_multimodal_table[componente_id] = features

        except Exception as e:
            print(f"[ERR-EVAL] Componente {componente_id}: {e}")
            traceback.print_exc()


if __name__ == '__main__':
    app.main()

