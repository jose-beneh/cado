import os
from neo4j import GraphDatabase

# Busca as variáveis do sistema (Docker) ou usa os padrões locais de fallback se rodar no host
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://192.168.3.212:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "53144310")  # Senha atualizada conforme seu Compose

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS),max_connection_pool_size=50,connection_timeout=30)

def get_neo4j_driver():
    return driver

