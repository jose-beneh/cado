import os
import sys
import time
import json
import uuid
import math
from confluent_kafka import Consumer, Producer, KafkaError
from river import anomaly
from river import compose
from river import preprocessing

# Protocolos binários nativos do OpenTelemetry (Protobuf)
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.trace.v1 import trace_pb2
from opentelemetry.proto.metrics.v1 import metrics_pb2

# Módulos do Drain3 para compartilhamento de base em modo Leitura
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

# ============================================================================
# CONFIGURAÇÕES DA TOPOLOGIA E COMPARTILHAMENTO DE ESTADO
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "192.168.3.212:9092")
TOPICO_LOGS = "otlp_logs"
TOPICO_TRACES = "otlp_traces"
TOPICO_METRICAS = "otlp_metrics"
TOPICO_ANOMALIAS = "system_anomalies"
GRUPO_CONSUMO = "hst-multimodal-engine-v3"

# Caminho do arquivo de estado gerenciado pelo log_consumer.py
DRAIN_STATE_PATH = "/app/var/log/state/drain3_state.bin"

# Janela de consolidação expandida para harmonizar o fluxo contínuo de logs/traces
# com o intervalo rígido de amostragem de 10s das métricas do coletor
JANELA_CONSOLIDACAO_SEG = 15.0 

# Limiar de anomalia calibrado para o escalonador por desvios padrões (Score Z)
LIMIAR_ANOMALIA = 0.78

# ============================================================================
# INICIALIZAÇÃO E SUBSCRIPÇÃO KAFKA
# ============================================================================
conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': GRUPO_CONSUMO,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(conf_consumer)
consumer.subscribe([TOPICO_LOGS, TOPICO_TRACES, TOPICO_METRICAS])
producer = Producer({'bootstrap.servers': KAFKA_URL})

# ============================================================================
# CONTROLE DO CÉREBRO COMPARTILHADO DRAIN3 (READ-ONLY)
# ============================================================================
template_miner_ia = None
timestamp_ultimo_reload_drain = time.time()

def carregar_drain_leitura_pura():
    """Carrega ou atualiza a árvore de templates em memória RAM sem alterar o arquivo."""
    global template_miner_ia, timestamp_ultimo_reload_drain
    try:
        if os.path.exists(DRAIN_STATE_PATH):
            persistence_handler = FilePersistence(DRAIN_STATE_PATH)
            config = TemplateMinerConfig()
            if os.path.exists("drain3.ini"):
                config.load("drain3.ini")
            
            miner = TemplateMiner(persistence_handler=persistence_handler, config=config)
            template_miner_ia = miner
            timestamp_ultimo_reload_drain = time.time()
        elif template_miner_ia is None:
            # Fallback temporário para evitar quebra no primeiro boot do ambiente
            template_miner_ia = TemplateMiner()
    except Exception as e:
        if template_miner_ia is None:
            template_miner_ia = TemplateMiner()

def checar_e_recarregar_drain():
    """Recarrega a base a cada 30s se o arquivo físico foi atualizado pelo log_consumer."""
    global timestamp_ultimo_reload_drain
    agora = time.time()
    if agora - timestamp_ultimo_reload_drain >= 30.0:
        carregar_drain_leitura_pura()

# Inicialização imediata da base de logs
carregar_drain_leitura_pura()

# ============================================================================
# ESTRUTURA DO BUFFER MULTIMODAL REATIVO (CARRY-FORWARD)
# ============================================================================
estado_componentes = {}
modelos_por_componente = {}
ultima_consolidacao = time.time()

def garantir_estrutura_buffer(comp_id):
    """Garante a estrutura em memória aplicando a persistência de estados contínuos."""
    if comp_id not in estado_componentes:
        estado_componentes[comp_id] = {
            # Resetam rigidamente ao fim de cada ciclo avaliado
            "log_templates_vistos": set(),
            "trace_errors": 0,
            "trace_latencies": [],
            
            # Carry-Forward: Mantêm a última percepção real em caso de janelas vazias
            "last_jvm_memory": 0.0,
            "last_valid_latency": 10.0  # Baseline neutro de repouso para RPCs Hadoop
        }

def obter_modelo_hst(componente_id):
    """Instancia o encadeamento incremental da IA indexado por componente."""
    if componente_id not in modelos_por_componente:
        modelos_por_componente[componente_id] = compose.Pipeline(
            preprocessing.StandardScaler(),  # Estabilizador por Score Z (Resiliente a outliers)
            anomaly.HalfSpaceTrees(
                n_trees=35,
                height=15,
                window_size=250,
                seed=42
            )
        )
    return modelos_por_componente[componente_id]

# ============================================================================
# PROCESSAMENTO DOS FLUXOS BINÁRIOS (PARSING EVENT-DRIVEN)
# ============================================================================
def processar_mensagem_kafka(msg):
    payload_binario = msg.value()
    topico = msg.topic()
    
    if topico == TOPICO_LOGS:
        try:
            logs_data = logs_pb2.LogsData()
            logs_data.ParseFromString(payload_binario)
            for res_log in logs_data.resource_logs:
                attrs = {a.key: a.value.string_value for a in res_log.resource.attributes if a.value.HasField('string_value')}
                service = attrs.get("service.name")
                host = (attrs.get("host.name") or "unknown").split('.')[0]
                
                if service and host:
                    comp_id = f"{service}@{host}".lower()
                    garantir_estrutura_buffer(comp_id)
                    
                    for scope_log in res_log.scope_logs:
                        for record in scope_log.log_records:
                            msg_texto = record.body.string_value.replace("\n", " ").strip()
                            
                            # Casamento ultrarrápido em memória RAM (Thread-Safe e Process-Safe)
                            cluster_obj = template_miner_ia.match(msg_texto)
                            # Se for um log inédito ainda não persistido pelo log_consumer, assume ID 0
                            template_id = cluster_obj.cluster_id if cluster_obj else 0
                            
                            estado_componentes[comp_id]["log_templates_vistos"].add(template_id)
        except Exception: pass

    elif topico == TOPICO_TRACES:
        try:
            trace_data = trace_pb2.TracesData()
            trace_data.ParseFromString(payload_binario)
            for res_span in trace_data.resource_spans:
                attrs = {a.key: a.value.string_value for a in res_span.resource.attributes if a.value.HasField('string_value')}
                service = attrs.get("service.name")
                host = (attrs.get("host.name") or "unknown").split('.')[0]
                
                if service and host:
                    comp_id = f"{service}@{host}".lower()
                    garantir_estrutura_buffer(comp_id)
                    
                    for scope_span in res_span.scope_spans:
                        for span in scope_span.spans:
                            duracao = (span.end_time_unix_nano - span.start_time_unix_nano) / 1_000_000
                            estado_componentes[comp_id]["trace_latencies"].append(max(0.0, duracao))
                            if span.status.code == trace_pb2.Status.StatusCode.STATUS_CODE_ERROR:
                                estado_componentes[comp_id]["trace_errors"] += 1
        except Exception: pass

    elif topico == TOPICO_METRICAS:
        try:
            metrics_data = metrics_pb2.MetricsData()
            metrics_data.ParseFromString(payload_binario)
            for res_metric in metrics_data.resource_metrics:
                attrs = {a.key: a.value.string_value for a in res_metric.resource.attributes if a.value.HasField('string_value')}
                service = attrs.get("service.name")
                host = (attrs.get("host.name") or "unknown").split('.')[0]
                
                if service and host:
                    comp_id = f"{service}@{host}".lower()
                    garantir_estrutura_buffer(comp_id)
                    
                    for scope_metric in res_metric.scope_metrics:
                        for metric in scope_metric.metrics:
                            if metric.name == "jvm.memory.heap.used":
                                data_points = metric.gauge.data_points if metric.HasField("gauge") else metric.sum.data_points
                                if data_points:
                                    pt = data_points[-1]
                                    valor_bytes = float(pt.as_int if pt.HasField("as_int") else pt.as_double)
                                    # Armazenamento passivo do dado real (Carry-Forward ativo)
                                    estado_componentes[comp_id]["last_jvm_memory"] = valor_bytes
        except Exception: pass

# ============================================================================
# AVALIAÇÃO E INFERÊNCIA DA IA MULTIMODAL
# ============================================================================
# Amortecimento Matemático no Vetor
def avaliar_anomalias_multimodais():
    global ultima_consolidacao
    agora = time.time()
    
    if agora - ultima_consolidacao < JANELA_CONSOLIDACAO_SEG:
        return

    # Sincroniza a árvore de templates em modo leitura com o disco
    checar_e_recarregar_drain()

    # Varredura segura isolando chaves locais para evitar race conditions
    for comp_id in list(estado_componentes.keys()):
        dados = estado_componentes[comp_id]
        
        # 1. CONSOLIDAÇÃO MATEMÁTICA DA LATÊNCIA (TRACES)
        if dados["trace_latencies"]:
            lat_feat = float(sum(dados["trace_latencies"]) / len(dados["trace_latencies"]))
            estado_componentes[comp_id]["last_valid_latency"] = lat_feat
        else:
            lat_feat = dados["last_valid_latency"]

        # 2. IMPLEMENTAÇÃO DA ESTRATÉGIA 1: AMORTECIMENTO DA DIVERSIDADE DE LOGS
        templates_reais = dados["log_templates_vistos"].copy()
        contagem_bruta = float(len(templates_reais))
        
        # Aplicamos a transformação logarítmica log1p -> ln(x + 1)
        # Se contagem for 0 (sem logs)      -> ln(1) = 0.00
        # Se contagem for 1 (apenas temp 0) -> ln(2) = 0.69  (Suavizado para o StandardScaler)
        # Se contagem for 2                 -> ln(3) = 1.10
        # Se contagem for 15 (erro em massa)-> ln(16) = 2.77 (Mantém o peso para isolamento do HST)
        log_diversity_amortecido = math.log1p(contagem_bruta)

        # 3. MONTAGEM DO VETOR MULTIMODAL SUAVIZADO
        x = {
            "jvm_memory_usage": float(dados["last_jvm_memory"]),
            "log_diversity": log_diversity_amortecido, 
            "trace_errors": float(dados["trace_errors"]),
            "trace_latency": lat_feat
        }
        
        # 4. INFERÊNCIA INCREMENTAL ONLINE (IA)
        hst = obter_modelo_hst(comp_id)
        anomaly_score = hst.score_one(x)
        hst.learn_one(x)
        
        # 5. DISPARO DE ALERTAS MULTIMODAIS SINTONIZADOS
        if anomaly_score > LIMIAR_ANOMALIA:
            payload_anomalia = {
                "anomaly_id": f"ANOMALIA_CADO_{uuid.uuid4().hex[:6].upper()}",
                "timestamp_ms": int(time.time() * 1000),
                "componente_id": comp_id,
                "model_type": "Framework_Lambda_CADO_HST_Streaming_Suavizado",
                "anomaly_score": float(anomaly_score),
                "telemetry_snapshot": {
                    # Enviado os dados brutos no snapshot para facilitar o RCA na LLM
                    "jvm_memory_usage": float(dados["last_jvm_memory"]),
                    "log_diversity_raw": contagem_bruta,
                    "trace_errors": float(dados["trace_errors"]),
                    "trace_latency": lat_feat
                }
            }
            try:
                producer.produce(TOPICO_ANOMALIAS, key=comp_id, value=json.dumps(payload_anomalia).encode('utf-8'))
            except Exception as kp_err:
                print(f"[ERRO KAFKA PRODUCER] Falha ao encaminhar alerta: {kp_err}")
            
            print(f"[ALERTA MULTIMODAL IA] Componente '{comp_id}' ISOLADO! Score: {anomaly_score:.4f} | Bruto: {contagem_bruta} -> Amortecido: {log_diversity_amortecido:.4f}")
            sys.stdout.flush()

        # 6. LIMPEZA SELETIVA DOS CONTADORES DA JANELA
        estado_componentes[comp_id]["log_templates_vistos"] = set()
        estado_componentes[comp_id]["trace_errors"] = 0
        estado_componentes[comp_id]["trace_latencies"] = []

    producer.flush()
    ultima_consolidacao = agora


#def avaliar_anomalias_multimodais():
#    global ultima_consolidacao
#    agora = time.time()
#    
#    if agora - ultima_consolidacao < JANELA_CONSOLIDACAO_SEG:
#        return
#
#    checar_e_recarregar_drain()
#
#    # Varredura segura isolando chaves locais para evitar race conditions
#    for comp_id in list(estado_componentes.keys()):
#        dados = estado_componentes[comp_id]
#        
#        # Consolidação matemática estável para a latência
#        if dados["trace_latencies"]:
#            lat_feat = float(sum(dados["trace_latencies"]) / len(dados["trace_latencies"]))
#            estado_componentes[comp_id]["last_valid_latency"] = lat_feat
#        else:
#            lat_feat = dados["last_valid_latency"]
#        # Vetor multimodal unificado enviado simultaneamente para o pipeline da IA
#        x = {
#            "jvm_memory_usage": float(dados["last_jvm_memory"]),
#            "log_diversity": float(len(dados["log_templates_vistos"])), # Diversidade de assinaturas Drain3
#            "trace_errors": float(dados["trace_errors"]),
#            "trace_latency": lat_feat
#        }
#
#        hst = obter_modelo_hst(comp_id)
#        anomaly_score = hst.score_one(x)
#        hst.learn_one(x)
#
#        if anomaly_score > LIMIAR_ANOMALIA:
#            payload_anomalia = {
#                "anomaly_id": f"ANOMALIA_CADO_{uuid.uuid4().hex[:6].upper()}",
#                "timestamp_ms": int(time.time() * 1000),
#                "componente_id": comp_id,
#                "model_type": "Framework_Lambda_CADO_HST_Streaming",
#                "anomaly_score": float(anomaly_score),
#                "telemetry_snapshot": x
#            }
#            producer.produce(TOPICO_ANOMALIAS, key=comp_id, value=json.dumps(payload_anomalia).encode('utf-8'))
#            print(f"[ALERTA MULTIMODAL IA] Componente '{comp_id}' ISOLADO! Score: {anomaly_score:.4f} | Vetor: {x}")
#            sys.stdout.flush()
#
#        # LIMPEZA SELETIVA: Zera contadores mantendo o histórico de métricas (Carry-Forward)
#        estado_componentes[comp_id]["log_templates_vistos"] = set()
#        estado_componentes[comp_id]["trace_errors"] = 0
#        estado_componentes[comp_id]["trace_latencies"] = []
#    producer.flush()
#    ultima_consolidacao = agora

============================================================================
LAÇO MESTRE PRINCIPAL
============================================================================
if __name__ == "__main__":
    print("--- Detector Multimodal λ-CADO: Ativo com Sucesso (Streaming Puro) ---")
    sys.stdout.flush()
    time.sleep(2.0)
    try:
        while True:
            m = consumer.poll(timeout=1.0)

            if m is not None and not m.error():
                processar_mensagem_kafka(m)

                if estado_componentes:
                    avaliar_anomalias_multimodais()
    except KeyboardInterrupt:
        print("\nDesligando motor de IA...")
    finally:
        consumer.close()
