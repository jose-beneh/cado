import os
import json
import time
import traceback
import faust
from river import anomaly  # Biblioteca padrão ouro para Streaming ML

# Captura do Broker do Kafka configurado no ambiente Docker
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka://kafka:29092")

# Inicialização da aplicação Faust dedicada para detecção de anomalias
app = faust.App(
    'hadoop-anomaly-detector',
    broker=KAFKA_URL,
    reply_create_topic=False,
    value_serializer='json'  # O output e tabelas internas usarão JSON estruturado
)

# Definição dos tópicos de entrada (produzidos pelos seus consumers da Fase 2 e 3 ou OTel)
# NOTA: Ajuste os nomes caso os seus scripts Faust publiquem dados agregados em outros tópicos.
log_metrics_topic = app.topic('faust_log_metrics', value_type=dict)
trace_metrics_topic = app.topic('faust_trace_metrics', value_type=dict)

# Tópico centralizado de saída para alertas, consumido futuramente pelas Fases 5 e 6
anomaly_events_topic = app.topic('anomaly_events', value_type=dict)

# ============================================================================
# TABELA DE JANELA EM MEMÓRIA (ALINHAMENTO MULTIMODAL)
# ============================================================================
# Esta tabela acumula e sincroniza os indicadores lógicos de logs e traces por componente.
# Chave: componente_id (ex: "namenode@utilit1")
# Valor: dicionário com os contadores agregados da janela atual
janela_multimodal_table = app.Table(
    'janela_multimodal_features',
    default=lambda: {
        "log_count": 0,
        "log_errors": 0,
        "trace_count": 0,
        "trace_errors": 0,
        "trace_avg_duration": 0.0,
        "last_update": 0
    }
)

# ============================================================================
# INICIALIZAÇÃO DO MODELO HALF-SPACE TREES (RIVER)
# ============================================================================
# O HST cria um conjunto de árvores com partições aleatórias.
# n_trees: número de árvores (25 a 50 é ótimo para performance/precisão)
# height: profundidade da árvore (limita o espaço de busca e consumo de memória)
# window_size: número de instâncias de referência para computar o score de anomalia
hst_model = anomaly.HalfSpaceTrees(
    n_trees=30,
    height=6,
    window_size=250,
    seed=42
)

# Limiar acadêmico/técnico para classificar como anomalia (Scores acima de 0.75 indicam forte isolamento)
THRESHOLD_ANOMALIA = 0.75

# ============================================================================
# AGENTE 1: CONSUMIDOR E AGREGADOR DE RECURSOS DE LOGS
# ============================================================================
@app.agent(log_metrics_topic)
async def processar_indicadores_logs(stream):
    async for msg in stream:
        try:
            componente_id = msg.get("componente_id")
            if not componente_id:
                continue

            # Recupera o estado atual ou cria um novo registro na tabela Faust
            estado = janela_multimodal_table[componente_id]
            
            estado["log_count"] += int(msg.get("count", 1))
            if msg.get("severity") in ["ERROR", "FATAL", "CRITICAL"]:
                estado["log_errors"] += int(msg.get("count", 1))
            
            estado["last_update"] = int(time.time())
            janela_multimodal_table[componente_id] = estado

        except Exception as e:
            print(f"[ERR-ANOMALY-LOGS] Falha ao processar indicador de log: {e}")

# ============================================================================
# AGENTE 2: CONSUMIDOR E AGREGADOR DE RECURSOS DE TRACES
# ============================================================================
@app.agent(trace_metrics_topic)
async def processar_indicadores_traces(stream):
    async for msg in stream:
        try:
            componente_id = msg.get("componente_id")
            if not componente_id:
                continue

            estado = janela_multimodal_table[componente_id]
            
            estado["trace_count"] += 1
            if msg.get("task_status") == "FAILED" or int(msg.get("status_code", 0)) >= 400:
                estado["trace_errors"] += 1
            
            # Média móvel exponencial local para suavizar picos isolados na janela
            duracao = float(msg.get("duration_ms", 0.0))
            if estado["trace_avg_duration"] == 0.0:
                estado["trace_avg_duration"] = duracao
            else:
                estado["trace_avg_duration"] = (estado["trace_avg_duration"] * 0.8) + (duracao * 0.2)
                
            estado["last_update"] = int(time.time())
            janela_multimodal_table[componente_id] = estado

        except Exception as e:
            print(f"[ERR-ANOMALY-TRACES] Falha ao processar indicador de trace: {e}")

# ============================================================================
# TIMER ASSÍNCRONO: AVALIAÇÃO DO MODELO HST POR JANELA DE TEMPO
# ============================================================================
# Este timer roda de forma independente a cada 30 segundos (alinhado com o hostmetrics),
# extrai o vetor de características da tabela Faust, alimenta o HST e gera os alertas.
@app.timer(interval=30.0)
async def avaliar_anomalias_streaming():
    print(f"\n--- [DETECTOR HST] Iniciando avaliação da janela temporal multimodal ---")
    timestamp_atual = int(time.time())
    
    # Itera sobre todos os componentes ativos monitorados na tabela local do Faust
    for componente_id, features in janela_multimodal_table.items():
        try:
            # Ignora componentes obsoletos que não reportaram telemetria nos últimos 2 minutos
            if timestamp_atual - features["last_update"] > 120:
                continue

            # 1. Construção do Vetor Multimodal Mapeado por Dicionário (Exigência do River)
            vetor_caracteristicas = {
                "log_count": float(features["log_count"]),
                "log_errors": float(features["log_errors"]),
                "trace_count": float(features["trace_count"]),
                "trace_errors": float(features["trace_errors"]),
                "trace_avg_duration": float(features["trace_avg_duration"])
            }

            # 2. Computação do Score de Anomalia via HST (Retorna um float de 0.0 a 1.0)
            # O score indica o quão isolado o vetor atual está em relação ao histórico recente da janela
            anomaly_score = hst_model.score_one(vetor_caracteristicas)

            # 3. Aprendizado Online Contínuo (O modelo se atualiza a cada passo com o dado atual)
            # Isso mitiga o falso positivo de "Concept Drift" (ex: Jobs MapReduce pesados e legítimos)
            hst_model.learn_one(vetor_caracteristicas)

            print(f"[HST EVAL] Componente: {componente_id} -> Vetor: {list(vetor_caracteristicas.values())} -> Score calculado: {anomaly_score:.4f}")

            # 4. Avaliação do Limiar e Publicação Desacoplada do Alerta
            if anomaly_score >= THRESHOLD_ANOMALIA:
                evento_alerta = {
                    "id": f"ANOMALIA_STREAM_{componente_id.replace('@', '_')}_{int(time.time())}",
                    "timestamp": int(time.time() * 1000),
                    "origem": "streaming_hst_detector",
                    "componente_target": str(componente_id),
                    "anomaly_score": float(anomaly_score),
                    "severity": "CRITICAL" if features["log_errors"] > 0 or features["trace_errors"] > 0 else "WARNING",
                    "status": "active",
                    "metric_or_log_template": "Multimodal Stream Vector Drift",
                    "detalhes_vetor": vetor_caracteristicas
                }
                
                print(f"⚠️ [ALERTA DISPARADO] Anomalia detectada em {componente_id}! Enviando para o Kafka...")
                await anomaly_events_topic.send(value=evento_alerta)

            # 5. Reset Parcial dos Contadores Volumétricos para a Próxima Janela Limpa de 30s
            # Mantemos a média de duração modificada para preservar a tendência temporal
            features["log_count"] = 0
            features["log_errors"] = 0
            features["trace_count"] = 0
            features["trace_errors"] = 0
            janela_multimodal_table[componente_id] = features

        except Exception as e:
            print(f"[ERR-HST-EVALUATION] Erro ao avaliar componente {componente_id}: {e}")
            traceback.print_exc()

if __name__ == '__main__':
    app.main()

