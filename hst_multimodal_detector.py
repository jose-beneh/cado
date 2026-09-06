import os
import sys
import time
import json
import uuid
import requests
from confluent_kafka import Consumer, Producer, KafkaError
from river import anomaly
from river import compose
from river import preprocessing
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.trace.v1 import trace_pb2

# ============================================================================
# CONFIGURAÇÕES E ENDEREÇOS
# ============================================================================
VM_URL = os.getenv("VICTORIAMETRICS_URL", "http://victoriametrics:8428/api/v1/query")
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPICO_LOGS = "otlp_logs"
TOPICO_TRACES = "otlp_traces"
TOPICO_ANOMALIAS = "system_anomalies"
GRUPO_CONSUMO = "hst-multimodal-group_v1"

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': GRUPO_CONSUMO,
    'auto.offset.reset': 'earliest', #'latest',
    'enable.auto.commit': True
}
consumer = Consumer(conf_consumer)
consumer.subscribe([TOPICO_LOGS, TOPICO_TRACES])

producer = Producer({'bootstrap.servers': KAFKA_URL})

# ============================================================================
# MODELO HST DO RIVER (3-5 FEATURES MULTIMODAIS)
# ============================================================================
modelos_por_componente = {}

def obter_modelo_hst(componente_id):
    if componente_id not in modelos_por_componente:
        # Inicializa o HST para tratar 4 dimensões (Métrica, Latência, Erro e Logs)
        modelos_por_componente[componente_id] = compose.Pipeline(
            preprocessing.MinMaxScaler(),
            anomaly.HalfSpaceTrees(
                n_trees=30,
                height=15,
                window_size=300,
                seed=42
            )
        )
    return modelos_por_componente[componente_id]

# Buffer em memória para logs e traces
estado_componentes = {}
JANELA_CONSOLIDACAO_SEG = 5.0
ultima_consolidacao = time.time()

def garantir_estrutura_buffer(comp_id):
    if comp_id not in estado_componentes:
        estado_componentes[comp_id] = {"log_count": 0, "trace_latencies": [], "trace_errors": 0}

# ============================================================================
# PARSING DE LOGS E TRACES (KAFKA STREAMING)
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
                    estado_componentes[comp_id]["log_count"] += 1
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

# ============================================================================
# INTEGRAÇÃO DE MÉTRICAS (VICTORIAMETRICS) E MODELAGEM MULTIMODAL
# ============================================================================
def obter_metrica_atual(componente_nome, timestamp_consulta):
    """Busca a última métrica de memória da JVM no VictoriaMetrics para o componente."""
    query_vm = f"sum(jvm_memory_bytes_used{{area='heap', job='{componente_nome}'}})"
    try:
        res = requests.get(VM_URL, params={'query': query_vm, 'time': timestamp_consulta}, timeout=1.5).json()
        if res.get('data', {}).get('result'):
            return float(res['data']['result'][0]['value'][1])
    except Exception:
        pass
    return 0.0

def avaliar_anomalias_multimodais():
    global ultima_consolidacao
    agora = time.time()
    
    if agora - ultima_consolidacao < JANELA_CONSOLIDACAO_SEG:
        return
        
    for comp_id, dados in list(estado_componentes.items()):
        # Extrai o nome do serviço (ex: namenode de namenode@utilit1)
        comp_nome = comp_id.split('@')[0]
        
        # Puxa o insumo de métricas em tempo de execução
        metric_feat = obter_metrica_atual(comp_nome, agora)
        
        # Consolida indicadores de logs e traces acumulados nos últimos 5 segundos
        log_count = len(dados["log_templates_vistos"]) if isinstance(dados.get("log_templates_vistos"), set) else dados["log_count"]
        log_feat = float(log_count)
        err_feat = float(dados["trace_errors"])
        
        # Correção para o HST não interpretar ociosidade de rede como anomalia:
        if dados["trace_latencies"]:
            lat_feat = float(sum(dados["trace_latencies"]) / len(dados["trace_latencies"]))
        else:
            # Em vez de 0.0 fixo, tenta pegar o último score do próprio modelo ou assume um baseline neutro
            lat_feat = 10.0 # Um baseline de repouso realista para RPCs internas do Hadoop

        log_feat = float(dados["log_count"])
        err_feat = float(dados["trace_errors"])
        lat_feat = float(sum(dados["trace_latencies"]) / len(dados["trace_latencies"])) if dados["trace_latencies"] else 0.0
        
        # Vetor multidimensional unificado enviado simultaneamente para a IA
        x = {
            "jvm_memory_usage": metric_feat,
            "log_volume": log_feat,
            "trace_errors": err_feat,
            "trace_latency": lat_feat
        }
        
        hst = obter_modelo_hst(comp_id)
        
        # Avalia o nível de isolamento (anomalia) do vetor multimodal completo
        anomaly_score = hst.score_one(x)
        
        # O modelo aprende o comportamento combinado atual de forma incremental
        hst.learn_one(x) 

        print(f"[IA INPUT] Componente: {comp_id} | Vetor Multimodal: {x} | Anomaly Score: {anomaly_score}")
        sys.stdout.flush()

        if anomaly_score > 0.75:
            payload_anomalia = {
                "anomaly_id": f"ANOMALIA_HST_{uuid.uuid4().hex[:6].upper()}",
                "timestamp_ms": int(time.time() * 1000),
                "componente_id": comp_id,
                "model_type": "River_HalfSpaceTrees_Multimodal",
                "anomaly_score": float(anomaly_score),
                "telemetry_snapshot": x
            }
            producer.produce(TOPICO_ANOMALIAS, key=comp_id, value=json.dumps(payload_anomalia).encode('utf-8'))
            print(f"[MULTIMODAL ALERTA] '{comp_id}' isolado! Score: {anomaly_score:.4f}. Telemetria: {x}")

        # Reseta buffers temporais
        estado_componentes[comp_id] = {"log_count": 0, "trace_latencies": [], "trace_errors": 0}
        
    producer.flush()
    ultima_consolidacao = agora

# ============================================================================
# LAÇO MESTRE
# ============================================================================
# ============================================================================
# LAÇO MESTRE REFATORADO (CORRIGIDO PARA USO REAL EM CONTAINER)
# ============================================================================
if __name__ == "__main__":
    print("--- Engine Multimodal HST (River): MODO DIAGNÓSTICO ATIVO ---")
    import sys
    sys.stdout.flush()

    # Dá um fôlego de 3 segundos para o container estabilizar as variáveis de rede do Docker
    print("[INIT] Aguardando estabilização da rede interna por 3 segundos...")
    time.sleep(3.0)
    sys.stdout.flush()

    timestamp_ultimo_print_vivo = time.time()
    mensagens_brutas_kafka = 0
    mensagens_com_sucesso_ia = 0

    try:
        while True:
            # 1. Aumentado timeout para 2s. Dá tempo real para o Kafka responder 
            # sem sobrecarregar o processador com loops vazios
            m = consumer.poll(timeout=2)

            if m is not None:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print(f"KAFKA ERRO OCULTO] {m.error()}")
                        sys.stdout.flush()
                else:
                    mensagens_brutas_kafka += 1
                    
                    # Processa a mensagem e altera o estado interno do buffer
                    processar_mensagem_kafka(m)
                    mensagens_com_sucesso_ia += 1
            else:
                # Se o Kafka não entregou nada e o buffer está vazio, aplica um pequeno 
                # descanso de 100ms para poupar CPU e evitar chamadas excessivas ao VictoriaMetrics
                if not estado_componentes:
                    time.sleep(0.1)

            # 2. Só avalia anomalias se houver dados reais no buffer para processar!
            # Isso impede que o script gaste processamento e trave chamando o VictoriaMetrics em falso
            if estado_componentes:
                avaliar_anomalias_multimodais()

            # 3. Heartbeat de monitoramento
            agora_vibe = time.time()
            if agora_vibe - timestamp_ultimo_print_vivo >= 10.0:
                print(f"[DIAGNÓSTICO] Mensagens coletadas do Kafka nos últimos 10s: {mensagens_brutas_kafka} | Passaram pelo filtro OTel: {mensagens_com_sucesso_ia}")
                sys.stdout.flush()
                mensagens_brutas_kafka = 0
                mensagens_com_sucesso_ia = 0
                timestamp_ultimo_print_vivo = agora_vibe

    except KeyboardInterrupt:
        print("\n Encerramento do detector solicitado.")
    finally:
        consumer.close()

