import os
import json
import sys
import time
import requests
from confluent_kafka import Consumer, KafkaError

# ============================================================================
# CONFIGURAÇÕES DE AMBIENTE E ORQUESTRAÇÃO
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")  # Endereço da GPU/Gemma
MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8020/rpc")  # Servidor MCP Hadoop/Ambari

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'agentic-remediation-group-v3',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = Consumer(conf_consumer)
consumer.subscribe(["llm_diagnostics_jobs"])

# ============================================================================
# INTERFACES DE EXECUÇÃO EXCLUSIVAS (MCP & BARREIRA COGNITIVA)
# ============================================================================
def executar_ferramenta_via_mcp(tool_name, arguments):
    """
    Abstração de chamada de ferramenta via Model Context Protocol (MCP) 
    para interagir nativamente com as APIs do Apache Ambari / Hadoop.
    """
    payload = {
        "jsonrpc": "2.0",
        "method": f"tools/call/{tool_name}",
        "params": {"arguments": arguments},
        "id": int(time.time())
    }
    try:
        response = requests.post(MCP_SERVER_URL, json=payload, timeout=5.0)
        if response.status_code == 200:
            return response.json().get("result", {}).get("text", "Sucesso na execução.")
    except Exception as ex:
        return f"[ERR MCP] Falha na execução da ferramenta: {ex}"
    return "[ERR MCP] Resposta inválida do servidor MCP."

def acionar_barreira_cognitiva_humana(payload, justificativa):
    """
    Força a degradação do framework para o modo assistido (Human-in-the-Loop),
    enviando os metadados causais e a evidência Dirichlet para análise dos operadores SRE.
    """
    print(f"\n[!!! BARREIRA COGNITIVA ATIVADA !!!] Encaminhando para Intervenção Humana.")
    print(f" -> Justificativa: {justificativa}")
    print(f" -> Anomalia ID: {payload['anomaly_id']} | Componente: {payload['componente_id']}")
    
    # Simulação de envio para endpoint de governança humana (ex: painel ou chat de SRE)
    # requests.post("http://ambari-util/sre-alert", json=payload)
    return "Ação retida por salvaguarda matemática. Aguardando liberação manual do operador."

# ============================================================================
# LÓGICA DE RACIOCÍNIO COGNITIVO (GEMMA LOCAL VIA OLLAMA)
# ============================================================================
def gerar_plano_cognitivo_gemma(payload, prompt_contexto):
    """Gera a explicação semântica do RCA e escolhe o runbook apropriado."""
    prompt_final = f"""
    [DIRETRIZ DE MISSÃO CRÍTICA - FRAMEWORK λ-CADO]
    Você é o agente cognitivo central de governança de infraestrutura Hadoop.
    
    CONTEXTO DO INCIDENTE:
    {prompt_contexto}
    
    TAREFA:
    Analise os logs e o veredito causal fornecido. Escolha estritamente UMA das seguintes ferramentas MCP de remediação disponíveis para mitigar o problema:
    1. 'ambari_restart_service' (args: {{"service_name": "...", "host": "..."}})
    2. 'hdfs_trigger_rereplication' (args: {{"block_id": "..."}})
    3. 'yarn_kill_corrupted_job' (args: {{"application_id": "..."}})
    
    Responda em formato JSON válido contendo as chaves: 'explicacao_rca' e 'ferramenta_mcp_escolhida' (com sub-chaves 'nome' e 'argumentos').
    """
    try:
        res = requests.post(OLLAMA_URL, json={"model": "gemma", "prompt": prompt_final, "stream": False, "format": "json"}, timeout=15.0)
        if res.status_code == 200:
            return json.loads(res.json().get("response", "{}"))
    except Exception as e:
        print(f"[ERR LLM INFERENCE] Falha de inferência no Gemma: {e}")
    return {}

# ============================================================================
# LAÇO MESTRE EXECUTOR (GRAFO DE ESTADOS DO LANGGRAPH SIMULADO NATIVAMENTE)
# ============================================================================
print("--- [Agentic LangGraph Remediation Engine Ativo via MCP & Gemma] ---")
sys.stdout.flush()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
            
        try:
            payload = json.loads(msg.value().decode('utf-8'))
            comp_id = payload["componente_id"]
            governança = payload.get("safety_governance", {})
            veredito_scm = payload.get("causal_verdict", {})
            
            # ----------------------------------------------------------------
            # NÓ DO GRAFO 1: AVALIAÇÃO FORMAL DE POLÍTICA (GUARDRAILS DO CMDP)
            # ----------------------------------------------------------------
            modo_execucao = governança.get("execution_mode", "ASSISTIDO")
            incerteza_u = governança.get("evidential_uncertainty_u", 1.0)
            penalty_score = governança.get("gating_penalty_score", float('inf'))
            
            print(f"\n[GRAFO - NÓ ENTRADA] Processando Anomalia {payload['anomaly_id']} do Componente {comp_id}")
            print(f" -> Modo de Governança Matemática Determinado pelo EDL: {modo_execucao}")
            sys.stdout.flush()
            
            # ----------------------------------------------------------------
            # NÓ DO GRAFO 2: DESVIO CONDICIONAL (ROUTING POR RISCO)
            # ----------------------------------------------------------------
            if "ASSISTIDO" in modo_execucao:
                # Caso de Alta Incerteza Epistêmica ou Violação de Restrição Dual Online (\lambda)
                payload["action_taken"] = acionar_barreira_cognitiva_humana(
                    payload, 
                    justificativa=f"Incerteza Epistêmica ({incerteza_u:.4f}) ou Penalidade Lagrangiana ({penalty_score:.4f}) acima dos limites de segurança do SLA."
                )
                consumer.commit(message=msg, asynchronous=False)
                continue
                
            # ----------------------------------------------------------------
            # NÓ DO GRAFO 3: PROCESSAMENTO COGNITIVO LOCAL (INFERÊNCIA GEMMA)
            # ----------------------------------------------------------------
            # O SCM provou a causa, passamos o veredito semântico e as evidências para a LLM
            contexto_str = json.dumps({
                "componente_afetado": comp_id,
                "causa_raiz_tecnica_provada_pelo_scm": veredito_scm.get("root_cause_candidate"),
                "confianca_causal_scm": veredito_scm.get("causal_confidence"),
                "evidencias_semanticas_logs": payload.get("EVIDENCIAS_SEMANTICAS", {}).get("logs_janela", []),
                "evidencias_semanticas_traces": payload.get("EVIDENCIAS_SEMANTICAS", {}).get("traces_falhos_janela", [])
            }, indent=2)
            
            print(f" -> Acionando Nó de Planejamento Cognitivo (Gemma Local + GPU)...")
            sys.stdout.flush()
            start_inference = time.time()
            
            plano_agêntico = gerar_plano_cognitivo_gemma(payload, contexto_str)
            latency_inf = time.time() - start_inference
            
            print(f" -> Raciocínio Concluído pelo Gemma em {latency_inf:.2f}s (Resposta à sua RQ4).")
            sys.stdout.flush()
            
            # ----------------------------------------------------------------
            # NÓ DO GRAFO 4: AUTOMAÇÃO DE RESPOSTA ATIVA (DESPACHO EXECUTOR MCP)
            # ----------------------------------------------------------------
            ferramenta = plano_agêntico.get("ferramenta_mcp_escolhida", {})
            tool_name = ferramenta.get("nome")
            tool_args = ferramenta.get("argumentos")
            
            if tool_name and tool_args:
                print(f" -> [NÓ EXECUÇÃO AUTÔNOMA] Despachando comando via MCP: '{tool_name}' com argumentos {tool_args}")
                resultado_mcp = executar_ferramenta_via_mcp(tool_name, tool_args)
                payload["action_taken"] = {
                    "status": "EXECUTADO_AUTONOMAMENTE",
                    "explicacao_cognitiva": plano_agêntico.get("explicacao_rca"),
                    "mcp_output": resultado_mcp,
                    "inference_latency_seconds": latency_inf
                }
                print(f"[SUCESSO CLOSED-LOOP] Mitigação concluída para {comp_id}. Resposta MCP: {resultado_mcp}")
            else:
                # Fallback de segurança se a LLM falhar em gerar a estrutura JSON correta
                payload["action_taken"] = acionar_barreira_cognitiva_humana(
                    payload, 
                    justificativa="Falha gramatical do modelo generativo ao estruturar os argumentos da ferramenta MCP."
                )
            
            # ----------------------------------------------------------------
            # NÓ FINAL: COMPLEMENTAÇÃO DA MALHA FECHADA E COMMIT
            # ----------------------------------------------------------------
            # Transação de mensagens imutáveis concluída de ponta a ponta
            consumer.commit(message=msg, asynchronous=False)
            sys.stdout.flush()
            
        except Exception as e:
            print(f"[ERRO NO MOTOR AGÊNTICO DE REMEDIAÇÃO]: {e}")
            sys.stdout.flush()

except KeyboardInterrupt:
    print("\n[AVISO] Encerramento do motor agêntico solicitado pelo operador.")
finally:
    consumer.close()

