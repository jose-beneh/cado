import os
import json
import sys
import time
import math
import torch
import torch.nn as nn
from confluent_kafka import Consumer, Producer, KafkaError

# ============================================================================
# CONFIGURAÇÕES DE AMBIENTE E INFRAESTRUTURA
# ============================================================================
KAFKA_URL = os.getenv("KAFKA_BROKER", "kafka:29092")

conf_consumer = {
    'bootstrap.servers': KAFKA_URL,
    'group.id': 'evidential-gating-group-v3',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = Consumer(conf_consumer)
consumer.subscribe(["evidential_gating_jobs"])

# Tópico de saída para o despacho seguro (Se passar pela barreira, vai para a LLM/Agente)
producer_out = Producer({'bootstrap.servers': KAFKA_URL, 'acks': 1})

# ============================================================================
# FORMALIZAÇÃO DO PROTÓTIPO DE REDE NEURAL EVIDENCIAL (EDL - DIRICHLET)
# ============================================================================
class RedeVocacionalEvidencial(nn.Module):
    """
    Substitui a camada Softmax convencional pela extração de massas de crença
    e parametrização de uma distribuição de Dirichlet multidimensional.
    """
    def __init__(self, input_dim=4, num_classes=5):
        super(RedeVocacionalEvidencial, self).__init__()
        self.num_classes = num_classes
        self.dense_layer = nn.Sequential(
            nn.Linear(input_dim, 16),
            nn.ReLU(),
            nn.Linear(16, num_classes)
        )
        # Softplus garante saídas estritamente não-negativas para o vetor de evidências e >= 0
        self.activation_evidence = nn.Softplus()

    def forward(self, x):
        raw_outputs = self.dense_layer(x)
        evidencias = self.activation_evidence(raw_outputs)
        # Parâmetros Dirichlet: alpha_k = e_k + 1
        alphas = evidencias + 1.0
        # Intensidade total de evidência observada: S = soma(alpha_k)
        S = torch.sum(alphas, dim=-1, keepdim=True)
        return evidencias, alphas, S

# Inicialização do modelo EDL com pesos pré-treinados simulados/salvos
# Classes de Diagnóstico K: [0: Falha de Rede, 1: Disco Saciado, 2: JVM Heap Blast, 3: RPC Lock, 4: Desconhecido]
NUM_CATEGORIAS_DIAGNOSTICAS = 5
modelo_edl = RedeVocacionalEvidencial(input_dim=4, num_classes=NUM_CATEGORIAS_DIAGNOSTICAS)
modelo_edl.eval() # Modo de inferência online

# ============================================================================
# NÚCLEO DE DECISÃO ADAPTATIVA (RELAXAÇÃO LAGRANGIANA / CMDP ONLINE)
# ============================================================================
class OtimizadorCMDPOnline:
    """
    Controlador Primal-Dual baseado na descida do subgradiente para atualização
    contínua do multiplicador lambda. Regulador de risco em tempo real.
    """
    def __init__(self, limite_delta=0.45, learning_rate_dual=0.05):
        self.lambda_t = 1.0  # Estado inicial do multiplicador de Lagrange (Neutro/Moderado)
        self.delta = limite_delta  # Teto máximo tolerável de custo operacional/risco de violação de SLA
        self.lr_dual = learning_rate_dual

    def atualizar_multiplicador_lambda(self, custo_esperado_risco):
        """
        Subida do subgradiente online do multiplicador:
        lambda_{t+1} = max(0, lambda_t + lr * (Custo - Delta))
        """
        gradiente = custo_esperado_risco - self.delta
        self.lambda_t = max(0.0, self.lambda_t + self.lr_dual * gradiente)
        return self.lambda_t

# Instanciação do núcleo de Pesquisa Operacional da tese
controlador_cmdp = OtimizadorCMDPOnline(limite_delta=0.40, learning_rate_dual=0.08)

# ============================================================================
# LAÇO MESTRE PRINCIPAL DE PROMOÇÃO DE SEGURANÇA (GATING)
# ============================================================================
print("--- [Evidential EDL & CMDP Gating Optimizer Ativo] ---")
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
            snapshot = payload.get("telemetry_snapshot", {})
            veredito_scm = payload.get("causal_verdict", {})
            
            # 1. Montagem do Vetor de Features de Incerteza (Derivado do Snapshot Multimodal)
            # Entrada: [Uso de Memória Normalizado, Diversidade de Logs, Erros de Traces, Latência RPC]
            features_raw = [
                float(snapshot.get("jvm_memory_usage", 0.0)) / 1e9, # Escala em GB
                float(snapshot.get("log_diversity_raw", 0.0)),
                float(snapshot.get("trace_errors", 0.0)),
                float(snapshot.get("trace_latency", 10.0)) / 100.0
            ]
            input_tensor = torch.tensor([features_raw], dtype=torch.float32)
            
            # 2. Inferência Causal de Incerteza Subjetiva (EDL PyTorch)
            with torch.no_grad():
                evidencias, alphas, S = modelo_edl(input_tensor)
                
            # Extração da Massa de Incerteza Epistêmica Global Residual: u = K / S
            S_val = float(S.item())
            incerteza_epistemica_u = float(NUM_CATEGORIAS_DIAGNOSTICAS / S_val)
            
            # Cálculo do Coeficiente Dinâmico de Confiança Analítica: tau_t = 1 - u
            tau_t = max(0.01, 1.0 - incerteza_epistemica_u)
            
            # 3. Formulação do Acoplamento Teórico: Incerteza Evidencial atuando no Risco
            # O custo percebido da ação de autorrecuperação aumenta se a confiança decrescer
            confianca_scm = float(veredito_scm.get("causal_confidence", 0.0))
            
            # Equação de Risco Operacional Proposta na Tese: Custo = 1 - (Confiança_SCM * Confiança_EDL)
            custo_percebido_risco = float(1.0 - (confianca_scm * tau_t))
            
            # 4. Atualização Online do Multiplicador de Lagrange Lambda_t
            lambda_atual = controlador_cmdp.atualizar_multiplicador_lambda(custo_percebido_risco)
            
            # 5. MECANISMO DE BARREIRA OPERACIONAL (GATING COGNITIVO / AUTONOMIA GRADUADA)
            # Função objetivo dual modificada: se o custo penalizado por lambda estourar, barra a autonomia
            limiar_seguranca_gatilho = lambda_atual * custo_percebido_risco
            
            status_governanca = "AUTONOMO"
            # Se o risco matemático for excessivo ou a incerteza epistêmica for muito alta, anula o despacho
            if limiar_seguranca_gatilho > 1.2 or incerteza_epistemica_u > 0.65:
                status_governanca = "ASSISTIDO (Humano-no-Loop Obrigatório)"
            
            # Anexa os metadados de garantias formais de segurança ao Payload Imutável
            payload["safety_governance"] = {
                "evidential_uncertainty_u": incerteza_epistemica_u,
                "confidence_coefficient_tau": tau_t,
                "calculated_operational_cost": custo_percebido_risco,
                "lagrange_multiplier_lambda": lambda_atual,
                "gating_penalty_score": limiar_seguranca_gatilho,
                "execution_mode": status_governanca
            }
            
            print(f"[GOVERNANÇA] Comp: {comp_id} | Incerteza Epistêmica: {incerteza_epistemica_u:.4f} | Lambda: {lambda_atual:.4f} | Veredito: {status_governanca}")
            sys.stdout.flush()
            
            # 6. Despacho Seguro para a Esteira Final (A IA Agêntica/LLM consumirá este tópico)
            producer_out.produce("llm_diagnostics_jobs", key=comp_id, value=json.dumps(payload).encode('utf-8'))
            producer_out.flush()
            
            # Confirmação manual transacional no Kafka
            consumer.commit(message=msg, asynchronous=False)
            
        except Exception as e:
            print(f"[ERRO CRÍTICO EDL/CMDP]: {e}")
            sys.stdout.flush()

except KeyboardInterrupt:
    print("\n[AVISO] Desligamento do módulo evidential solicitado pelo operador.")
finally:
    consumer.close()

