import os
import sys
import time
import traceback
from confluent_kafka import Consumer, KafkaError
from database import get_neo4j_driver  # Importa a conexão síncrona estável
from opentelemetry.proto.trace.v1 import trace_pb2

# ============================================================================
# CONFIGURAÇÕES E VARIÁVEIS GLOBAIS
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPICO_TRACES = "otlp_traces"
GRUPO_CONSUMO = "hadoop-trace-processor-confluent"

# Parâmetros de controle de micro-loteamento na memória
TAMANHO_MAX_LOTE = 500          # Limite de registros acumulados para descarregar no Neo4j
TEMPO_MAX_JANELA_SEG = 5.0      # Tempo máximo de espera (segundos) antes de forçar a gravação

# Query Cypher em lote unificada e sintonizada (Sintaxe oficial Neo4j 5.26+)
CYPHER_BATCH_TRACES = """
UNWIND $batch AS item
// ============================================================================
// PASSO 1: ATUALIZADO PARA PROCESSAR QUALQUER EVENTO ATIVO DO LOG
// ============================================================================
CALL (item) {
    // Processa se for um job real OU se tiver um usuário válido ativo no log (ex: hdfs)
    WITH item 
    WHERE item.is_job = true 
       OR (item.usuario IS NOT NULL AND item.usuario <> 'None')

    // Se o job_id/task_id vier como a string 'None', gera IDs baseados na thread e trace_id
    WITH item, 
         CASE WHEN item.job_id = 'None' OR item.job_id IS NULL 
              THEN 'job-' + toString(item.usuario) 
              ELSE toString(item.job_id) END AS id_do_job,
         CASE WHEN item.task_id = 'None' OR item.task_id IS NULL 
              THEN 'task-' + substring(toString(item.trace_id), 0, 8) 
              ELSE toString(item.task_id) END AS id_da_task

    MERGE (j:job {id_job: id_do_job})
      ON CREATE SET
        j.usuario = toString(item.usuario),
        j.status = 'RUNNING',
        j.start_time = toInteger(item.timestamp)

    MERGE (t:task {id_task: id_da_task})
      ON CREATE SET
        t.tipo = toString(item.task_tipo),
        t.status = 'RUNNING'
      SET
        t.duration_ms = toFloat(item.duration_ms),
        t.status = toString(item.task_status)

    MERGE (j)-[:COMPREENDE]->(t)
      SET j.status = CASE WHEN item.task_status = 'FAILED' THEN 'FAILED' ELSE j.status END
}

// ============================================================================
// PASSO 2: ATUALIZADO PARA VÍNCULO RESILIENTE
// ============================================================================
CALL (item) {
    WITH item 
    WHERE item.is_job = true 
       OR (item.usuario IS NOT NULL AND item.usuario <> 'None')

    WITH item, 
         CASE WHEN item.task_id = 'None' OR item.task_id IS NULL 
              THEN 'task-' + substring(toString(item.trace_id), 0, 8) 
              ELSE toString(item.task_id) END AS id_da_task,
         split(toString(item.componente_id), '@')[-1] AS host_alvo

    OPTIONAL MATCH (h:host)-[:EXECUTA]->(comp_real:componente)
      WHERE h.id = host_alvo AND comp_real.nome = 'nodemanager'

    WITH item, id_da_task, coalesce(comp_real.id, item.componente_id) AS target_resolved_id

    MERGE (comp_dest:componente {id: target_resolved_id})
    
    WITH id_da_task, comp_dest, item
    MATCH (t:task {id_task: id_da_task})
    MERGE (t)-[r:EXECUTADA_EM]->(comp_dest)
      SET
        r.trace_id = toString(item.trace_id),
        r.timestamp_vinculo = toInteger(item.timestamp)
}


// ============================================================================
// PASSO 3: FLUXO DE REDE / RPC AGREGADO (O CORAÇÃO DO TRACE_CONSUMER)
// ============================================================================
CALL (item) {
    WITH item WHERE item.is_job = false

    WITH item,
         split(item.componente_id, '@')[-1] AS dest_host,
         split(item.origem_id, '@')[-1] AS orig_host

    // Se o ID vier genérico do OTel, reconcilia com o componente real do host estático
    OPTIONAL MATCH (h_d:host)-[:EXECUTA]->(real_dest:componente)
      WHERE h_d.id = dest_host AND item.componente_id STARTS WITH 'hadoop-apps-global'
    WITH item, orig_host, dest_host, COALESCE(real_dest.id, item.componente_id) AS final_dest_id

    OPTIONAL MATCH (h_o:host)-[:EXECUTA]->(real_orig:componente)
      WHERE h_o.id = orig_host AND item.origem_id STARTS WITH 'hadoop-apps-global'
    WITH item, final_dest_id, COALESCE(real_orig.id, item.origem_id) AS final_orig_id

    // Garante que os nós existam sem duplicar a infraestrutura lúdica
    MERGE (comp_dest:componente {id: final_dest_id})
    MERGE (comp_orig:componente {id: final_orig_id})

    // Cria/Atualiza a aresta agregada com média móvel exponencial de latência
    MERGE (comp_orig)-[r:CHAMA_REDE]->(comp_dest)
      SET
        r.timestamp_janela = toInteger(item.timestamp_janela),
        r.route = toString(item.http_route),
        r.occurrences = COALESCE(r.occurrences, 0) + 1,
        r.errors = COALESCE(r.errors, 0) + CASE WHEN toInteger(item.status_code) >= 400 OR item.error_type <> 'None' THEN 1 ELSE 0 END,
        r.avg_duration_ms = COALESCE(r.avg_duration_ms, 0) * 0.8 + toFloat(item.duration_ms) * 0.2
}


// ============================================================================
// PASSO 4: VERSÃO ULTRA-OTIMIZADA BASEADA NAS CHAVES REAIS DO LOG
// ============================================================================
CALL (item) {
    WITH item 
    WHERE item.componente_id IS NOT NULL 
      AND item.componente_id <> 'None' 
      AND toString(item.componente_id) CONTAINS '@'

    // Extrai o nome do serviço pegando o primeiro elemento do split
    WITH item, 
         split(toString(item.componente_id), '@')[0] AS srv_nome,
         'hadoop-CADO' AS cl_nome

    MERGE (cl:Cluster {nome: cl_nome})
    MERGE (srv:Servico {id: srv_nome})
    MERGE (srv)-[:PERTENCE_AO]->(cl)

    WITH item, srv
    MATCH (comp:componente {id: toString(item.componente_id)})
    MERGE (comp)-[:HOSPEDA_SERVICO]->(srv)
}

"""


# ============================================================================
# PERSISTÊNCIA SÍNCRONA NO NEO4J
# ============================================================================
def enviar_lote_traces_neo4j(lote):
    amostra = lote[0]
    print(f"[DEBUG NEO4J TRACES] Despachando {len(lote)} spans para o Neo4j (Modo Agregado)")
    print(f"[DEBUG CONSUMER] Chaves do primeiro item do lote: {list(lote[0].keys())}")
    print(f"[DEBUG CONSUMER] Conteúdo do primeiro item: {lote[0]}")
    print("========================================\n")	
    print("\n=== [DIAGNÓSTICO DE CHAVES DO LOTE] ===")
    print(f"Chaves disponíveis no objeto: {list(amostra.keys())}")
    print(f"Valor de 'is_job': {amostra.get('is_job')} (Tipo: {type(amostra.get('is_job'))})")
    print(f"Valor de 'job_id': {amostra.get('job_id')}")
    print(f"Valor de 'task_id': {amostra.get('task_id')}")
    print("========================================\n")	    
    try:
        with neo4j_driver.session() as session:
            result = session.run(CYPHER_BATCH_TRACES, batch=lote)
            counters = result.consume().counters
            print(f"[DEBUG NEO4J TRACES SUCESSO] Resumo de escrita:\n"
                  f" -> Nós criados: {counters.nodes_created}\n"
                  f" -> Relacionamentos criados: {counters.relationships_created}\n"
                  f" -> Propriedades definidas: {counters.properties_set}")
            return True
    except Exception as e:
        print(f"[DEBUG NEO4J TRACES ERRO] Falha na execução da query Cypher: {e}")
        traceback.print_exc()
        return False

# ============================================================================
# INICIALIZAÇÃO DOS COMPONENTES
# ============================================================================
print("--- Inicializando o Processador de Traces Corporativo (Confluent Kafka) ---")

# 1. Conexão com o Banco de Dados Neo4j (Modo Síncrono Estável)
neo4j_driver = get_neo4j_driver()

# 2. Inicialização e Configuração do Consumidor Confluent Kafka
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
    consumer.subscribe([TOPICO_TRACES])
    print(f"Inscrito com sucesso no tópico Kafka: '{TOPICO_TRACES}' no broker {KAFKA_URL}")
except Exception as e:
    print(f"Erro ao conectar com o cluster Kafka: {e}")
    sys.exit(1)

# ============================================================================
# LAÇO MESTRE DE INGESTÃO E PROCESSAMENTO (PIPELINE)
# ============================================================================
lote_acumulado = []
timestamp_ultima_gravacao = time.time()

print("\n--- Pipeline de Traces Ativo! Aguardando spans do barramento Kafka... ---")

try:
    while True:
        # Busca um lote compacto de mensagens do Kafka (timeout de 1s)
        mensagens = consumer.consume(num_messages=100, timeout=1.0)

        if not mensagens:
            # Avalia se a janela de tempo estourou mesmo sem mensagens novas chegarem
            if lote_acumulado and (time.time() - timestamp_ultima_gravacao >= TEMPO_MAX_JANELA_SEG):
                print("[GATILHO TIMEOUT] Descarregando lote de traces acumulado devido ao tempo limite.")
                if enviar_lote_traces_neo4j(lote_acumulado):
                    consumer.commit(asynchronous=False)
                    lote_acumulado.clear()
                timestamp_ultima_gravacao = time.time()
            continue

        for msg in mensagens:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[KAFKA ERRO] Falha no barramento de traces: {msg.error()}")
                    continue

            payload_binario = msg.value()

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

                    # Extrai a string pura do hostname antes de concatenar
                    host_partes = full_hostname.split('.')
                    host_curto = host_partes[0] if host_partes else "unknown"
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

                            job_id = span_attrs.get("hadoop.job.id") or span_attrs.get("hadoop.jobId") or span_attrs.get("mapreduce.job.id")
                            task_id = span_attrs.get("hadoop.task.id") or span_attrs.get("hadoop.taskId") or span_attrs.get("mapreduce.task.id")
                            usuario = span_attrs.get("hadoop.user") or span_attrs.get("hadoop.username") or "hdfs"

                            is_job_span = True if (job_id and task_id) else False

                            # Atributos de rotas HTTP e chamadas RPC
                            http_route = span_attrs.get("http.route") or span_attrs.get("url.path") or span_attrs.get("url.full") or "internal_rpc"
                            http_method = span_attrs.get("http.request.method") or "GET"
                            status_code = span_attrs.get("http.response.status_code") or 0
                            error_type = span_attrs.get("error.type") or "None"

                            # Conversão e cálculo de duração dos spans
                            start_time_ms = int(span.start_time_unix_nano / 1_000_000)
                            end_time_ms = int(span.end_time_unix_nano / 1_000_000)
                            duration_ms = end_time_ms - start_time_ms if end_time_ms > start_time_ms else 0

                            # Alinha o timestamp a janelas fixas de 1 minuto (60000 ms) para casar logs e traces
                            timestamp_janela = start_time_ms - (start_time_ms % 60000)

                            # Mapeamento do status da Task com base no status do OpenTelemetry
                            status_code_otel = span.status.code
                            task_status = "SUCCESS" if status_code_otel == 1 else "RUNNING"
                            if status_code_otel == 2:
                                task_status = "FAILED"
                                if error_type == "None":
                                    error_type = "OTel_Execution_Error"

                            trace_id_hex = span.trace_id.hex()

                            # Extração e tratamento de string para a origem (Peer Service)
                            peer_service = span_attrs.get("peer.service") or span_attrs.get("net.peer.name") or span_attrs.get("server.address")
                            if peer_service:
                                peer_partes = peer_service.split('.')
                                host_origem_curto = peer_partes[0].lower() if peer_partes else "unknown"
                                origem_id = f"{peer_service}" if "@" in peer_service else f"hadoop-apps-global@{host_origem_curto}"
                            else:
                                origem_id = "hadoop-apps-global@client"

                            # Classificação do tipo de tarefa executada no ecossistema Hadoop
                            if is_job_span:
                                if "m" in str(task_id):
                                    tipo_task = "Map"
                                elif "r" in str(task_id):
                                    tipo_task = "Reduce"
                                else:
                                    tipo_task = "Spark/Generic"
                            else:
                                tipo_task = "Web/RPC"

                            # Alocação estruturada no lote em memória para inserção em lote
                            lote_acumulado.append({
                                "is_job": bool(is_job_span),
                                "job_id": str(job_id) if job_id else "None",
                                "task_id": str(task_id) if task_id else "None",
                                "task_tipo": tipo_task,
                                "task_status": task_status,
                                "usuario": str(usuario),
                                "componente_id": str(componente_id),
                                "origem_id": str(origem_id).lower(),
                                "trace_id": str(trace_id_hex),
                                "duration_ms": float(duration_ms),
                                "timestamp": int(start_time_ms),
                                "timestamp_janela": int(timestamp_janela),
                                "http_route": str(http_route),
                                "http_method": str(http_method),
                                "status_code": int(status_code),
                                "error_type": str(error_type)
                            })
            except Exception as e:
                print(f"[TRACE PARSE ERROR] Falha ao processar span individual: {e}")
                continue

        # Avaliação de gatilhos físicos para descarga (Volume máximo atingido ou Tempo Limite esgotado)
        if len(lote_acumulado) >= TAMANHO_MAX_LOTE or (time.time() - timestamp_ultima_gravacao >= TEMPO_MAX_JANELA_SEG):
            if lote_acumulado:
                if enviar_lote_traces_neo4j(lote_acumulado):
                    consumer.commit(asynchronous=False)
                    lote_acumulado.clear()
                    timestamp_ultima_gravacao = time.time()

except KeyboardInterrupt:
    print("\n[AVISO] Encerramento manual solicitado pelo operador.")
except Exception as e:
    print(f"\n[FALHA CATASTRÓFICA] Erro fatal no laço mestre: {e}")
    traceback.print_exc()
finally:
    print("\n[DESLIGAMENTO] Fechando conexões de traces...")
    try:
        consumer.close()
        neo4j_driver.close()
        print("[DESLIGAMENTO] Recursos de Traces limpos com sucesso.")
    except Exception as e:
        print(f"[DESLIGAMENTO ERRO] Falha ao encerrar drivers: {e}")

