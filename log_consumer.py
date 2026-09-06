import os
import sys
import time
import traceback
from confluent_kafka import Consumer, KafkaError
from database import get_neo4j_driver  # Importa a conexão síncrona estável
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig
from opentelemetry.proto.logs.v1 import logs_pb2

# ============================================================================
# CONFIGURAÇÕES E VARIÁVEIS GLOBAIS
# ============================================================================
DRAIN_STATE_PATH = "/app/var/log/state/drain3_state.bin"
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPICO_LOGS = "otlp_logs"
GRUPO_CONSUMO = "hadoop-log-processor-confluent"

# Parâmetros de controle de micro-loteamento na memória
TAMANHO_MAX_LOTE = 500          # Limite de registros acumulados para descarregar no Neo4j
TEMPO_MAX_JANELA_SEG = 5.0      # Tempo máximo de espera (segundos) antes de forçar a gravação

CYPHER_BATCH_LOGS = """
UNWIND $batch AS item
MATCH (comp:componente) WHERE comp.id = toString(item.componente_id)
MERGE (lt:logtemplate {id: toString(item.template_id)})
  ON CREATE SET lt.text_pattern = toString(item.pattern)
MERGE (comp)-[r:REGISTROU_LOG]->(lt)
SET r.timestamp_janela = toInteger(item.timestamp_janela),
    r.occurrences = COALESCE(r.occurrences, 0) + toInteger(item.count)
"""

# ============================================================================
# PERSISTÊNCIA SÍNCRONA NO NEO4J
# ============================================================================
def enviar_lote_logs_neo4j(lote):
    print(f"[DEBUG NEO4J] Enviando lote com {len(lote)} itens agregados para o Neo4j...")
    try:
        with neo4j_driver.session() as session:
            result = session.run(CYPHER_BATCH_LOGS, batch=lote)
            counters = result.consume().counters
            print(f"[DEBUG NEO4J ÉXITO] Resumo: Nós criados: {counters.nodes_created}, "
                  f"Relacionamentos: {counters.relationships_created}, "
                  f"Propriedades definidas: {counters.properties_set}")
            return True
    except Exception as e:
        print(f"[DEBUG NEO4J ERRO] Falha crítica na execução da query Cypher: {e}")
        return False

# ============================================================================
# INICIALIZAÇÃO DOS COMPONENTES
# ============================================================================
print("--- Inicializando o Processador de Logs Corporativo (Confluent Kafka) ---")

# 1. Conexão com o Banco de Dados Neo4j (Modo Síncrono Estável)
neo4j_driver = get_neo4j_driver()

# 2. Inicialização do Minerador Drain3
print("Carregando o Minerador Drain3 e recuperando estados antigos...")
try:
    os.makedirs(os.path.dirname(DRAIN_STATE_PATH), exist_ok=True)
    persistence_handler = FilePersistence(DRAIN_STATE_PATH)
    config = TemplateMinerConfig()
    config.load("drain3.ini")
    template_miner = TemplateMiner(persistence_handler=persistence_handler, config=config)
    print(f"Drain3 carregado com sucesso. Arquivo físico: {DRAIN_STATE_PATH}")
except Exception as e:
    print(f"Erro catastrófico ao instanciar o Drain3: {e}")
    sys.exit(1)

# 3. Inicialização e Configuração do Consumidor Confluent Kafka
conf_kafka = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': GRUPO_CONSUMO,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,          # Controle manual e transacional do commit
    'session.timeout.ms': 45000,          # Tolerância de 45s antes de considerar queda
    'max.poll.interval.ms': 300000        # Dá até 5 minutos para processar os lotes sem cair
}

try:
    consumer = Consumer(conf_kafka)
    consumer.subscribe([TOPICO_LOGS])
    print(f"Inscrito com sucesso no tópico Kafka: '{TOPICO_LOGS}' no broker {KAFKA_URL}")
except Exception as e:
    print(f"Erro ao conectar com o cluster Kafka: {e}")
    sys.exit(1)

# ============================================================================
# LAÇO MESTRE DE INGESTÃO E PROCESSAMENTO (PIPELINE)
# ============================================================================
lote_acumulado = []
timestamp_ultima_gravacao = time.time()

print("\n--- Pipeline Ativo! Aguardando mensagens do barramento Kafka... ---")

try:
    while True:
        # Busca um lote compacto de mensagens (aguarda no máximo 1 segundo se estiver vazio)
        mensagens = consumer.consume(num_messages=100, timeout=1.0)
        
        if not mensagens:
            # Avalia se a janela de tempo estourou mesmo sem mensagens novas chegarem
            if lote_acumulado and (time.time() - timestamp_ultima_gravacao >= TEMPO_MAX_JANELA_SEG):
                print("[GATILHO TIMEOUT] Descarregando lote acumulado devido ao tempo limite da janela.")
                if enviar_lote_logs_neo4j(lote_acumulado):
                    consumer.commit(asynchronous=False)
                    template_miner.save_state(snapshot_reason="timeout_window")
                    lote_acumulado.clear()
                timestamp_ultima_gravacao = time.time()
            continue

        timestamp_atual = time.time()
        agregacao_janela = {}

        for msg in mensagens:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[KAFKA ERRO] Falha no barramento de dados: {msg.error()}")
                    continue

            payload_binario = msg.value()

            try:
                logs_data = logs_pb2.LogsData()
                logs_data.ParseFromString(payload_binario)
                if not logs_data.resource_logs:
                    continue

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

                    host_curto = full_hostname.split('.')[0]
                    componente_id = f"{service_name}@{host_curto}".lower()

                    for scope_logs in resource_logs.scope_logs:
                        for log_record in scope_logs.log_records:
                            mensagem_log = ""
                            for attr in log_record.attributes:
                                if attr.key == "message":
                                    tipo_val = attr.value.WhichOneof('value')
                                    if tipo_val == 'string_value':
                                        mensagem_log = attr.value.string_value
                                        break

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

                            mensagem_limpa = str(mensagem_log).replace("\n", " ").replace("\r", " ").strip()

                            # Mineração síncrona ultrarrápida em memória RAM
                            result = template_miner.add_log_message(mensagem_limpa)
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

                            if not pattern or str(pattern).strip() == "":
                                pattern = mensagem_limpa
                                if len(pattern) > 150:
                                    pattern = pattern[:147] + "..."

                            template_id = f"TEMPLATE_{cluster_id}"
                            chave = (componente_id, template_id)

                            if chave not in agregacao_janela:
                                agregacao_janela[chave] = {"pattern": pattern, "count": 0}

                            agregacao_janela[chave]["count"] += 1

            except Exception as e:
                print(f"[DEBUG PARSE ERRO] Falha ao decodificar payload individual: {e}")
                continue

        # Consolida os dados agregados da janela na memória de lote para gravação
        for (comp_id, temp_id), dados_template in agregacao_janela.items():
            lote_acumulado.append({
                "componente_id": str(comp_id),
                "template_id": str(temp_id),
                "pattern": str(dados_template["pattern"]),
                "count": int(dados_template["count"]),
                "timestamp_janela": int(timestamp_atual * 1000)
            })


        # Avaliação de gatilhos físicos para descarga (Volume máximo atingido ou Tempo Limite esgotado)
        if len(lote_acumulado) >= TAMANHO_MAX_LOTE or (time.time() - timestamp_ultima_gravacao >= TEMPO_MAX_JANELA_SEG):
            if lote_acumulado:
                if enviar_lote_logs_neo4j(lote_acumulado):
                    # Só confirma leitura para o Kafka e persiste arquivo se gravou no banco com sucesso
                    consumer.commit(asynchronous=False)
                    template_miner.save_state(snapshot_reason="batch_completed")
                    lote_acumulado.clear()
                    timestamp_ultima_gravacao = time.time()

except KeyboardInterrupt:
    print("\n[AVISO] Encerramento manual solicitado pelo operador.")
except Exception as e:
    print(f"\n[FALHA CATASTRÓFICA] Erro fatal no laço mestre: {e}")
    traceback.print_exc()

finally:
    print("\n[DESLIGAMENTO] Descarregando registros finais e limpando alocações...")

    if 'template_miner' in locals() and template_miner is not None:
        try:
            template_miner.save_state(snapshot_reason="shutdown")
            print("[DESLIGAMENTO] Estado final do Drain3 salvo em disco.")
        except Exception as e:
            print(f"[DESLIGAMENTO ERRO] Falha ao persistir estado final: {e}")

    try:
        consumer.close()
        neo4j_driver.close()
        print("[DESLIGAMENTO] Conexões finalizadas de forma limpa.")
    except Exception as e:
        print(f"[DESLIGAMENTO ERRO] Falha ao encerrar drivers da stack: {e}")

