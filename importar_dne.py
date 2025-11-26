import os
import mysql.connector
from dotenv import load_dotenv

# --------------------------
# Carregar variáveis do .env
# --------------------------
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
DNE_PATH = os.getenv("DNE_PATH")

# --------------------------
# Conexão inicial (sem DB)
# --------------------------
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    allow_local_infile=True
)
cursor = conn.cursor()

# --------------------------
# Criar banco de dados
# --------------------------
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
cursor.execute(f"USE {MYSQL_DATABASE}")

print(f"Banco {MYSQL_DATABASE} pronto.")

# -------------------------------------------------------------------
# Dicionário com tabelas + estrutura EXATA do DNE (layout delimitado)
# -------------------------------------------------------------------
tabelas = {
    "ECT_PAIS": """
        CREATE TABLE IF NOT EXISTS ECT_PAIS (
            PAIS_CODIGO INT,
            PAIS_NOME VARCHAR(200),
            SIGLA VARCHAR(10)
        )
    """,
    "LOG_BAIRRO": """
        CREATE TABLE IF NOT EXISTS LOG_BAIRRO (
            BAIRRO_COD INT,
            BAIRRO_NOME VARCHAR(200),
            BAIRRO_ABREV VARCHAR(50),
            LOCALIDADE_COD INT
        )
    """,
    "LOG_CPC": """
        CREATE TABLE IF NOT EXISTS LOG_CPC (
            CPC_COD INT,
            LOCALIDADE_COD INT,
            LOGRADOURO_COD INT,
            NUM_INICIAL VARCHAR(20),
            NUM_FINAL VARCHAR(20),
            PAR_IMPAR CHAR(1),
            NOME VARCHAR(200)
        )
    """,
    "LOG_FAIXA_BAIRRO": """
        CREATE TABLE IF NOT EXISTS LOG_FAIXA_BAIRRO (
            BAIRRO_COD INT,
            NUM_INICIAL VARCHAR(20),
            NUM_FINAL VARCHAR(20),
            PAR_IMPAR CHAR(1)
        )
    """,
    "LOG_FAIXA_CPC": """
        CREATE TABLE IF NOT EXISTS LOG_FAIXA_CPC (
            CPC_COD INT,
            NUM_INICIAL VARCHAR(20),
            NUM_FINAL VARCHAR(20),
            PAR_IMPAR CHAR(1)
        )
    """,
    "LOG_FAIXA_LOCALIDADE": """
        CREATE TABLE IF NOT EXISTS LOG_FAIXA_LOCALIDADE (
            LOCALIDADE_COD INT,
            NUM_INICIAL VARCHAR(20),
            NUM_FINAL VARCHAR(20),
            PAR_IMPAR CHAR(1)
        )
    """,
    "LOG_FAIXA_UF": """
        CREATE TABLE IF NOT EXISTS LOG_FAIXA_UF (
            UF CHAR(2),
            NUM_INICIAL VARCHAR(20),
            NUM_FINAL VARCHAR(20),
            PAR_IMPAR CHAR(1)
        )
    """,
    "LOG_FAIXA_UOP": """
        CREATE TABLE IF NOT EXISTS LOG_FAIXA_UOP (
            UOP_COD INT,
            NUM_INICIAL VARCHAR(20),
            NUM_FINAL VARCHAR(20),
            PAR_IMPAR CHAR(1)
        )
    """,
    "LOG_GRANDE_USUARIO": """
        CREATE TABLE IF NOT EXISTS LOG_GRANDE_USUARIO (
            GU_COD INT,
            GU_NOME VARCHAR(200),
            LOCALIDADE_COD INT,
            UF CHAR(2),
            CEP CHAR(8)
        )
    """,
    "LOG_LOCALIDADE": """
        CREATE TABLE IF NOT EXISTS LOG_LOCALIDADE (
            LOCALIDADE_COD INT,
            LOCALIDADE_NOME VARCHAR(200),
            LOCALIDADE_ABREV VARCHAR(50),
            MUNICIPIO_COD INT,
            UF CHAR(2),
            CEP CHAR(8)
        )
    """,
    "LOG_LOGRADOURO_AC": """
        CREATE TABLE IF NOT EXISTS LOG_LOGRADOURO_AC (
            LOG_COD INT,
            LOG_NOME VARCHAR(200),
            TIPO VARCHAR(50),
            BAIRRO_INI INT,
            BAIRRO_FIM INT,
            LOCALIDADE_COD INT,
            CEP CHAR(8)
        )
    """,
}

# --------------------------
# Criar tabelas no MySQL
# --------------------------
print("Criando tabelas...")

for nome, ddl in tabelas.items():
    cursor.execute(ddl)
    print(f"Tabela criada: {nome}")

conn.commit()

# --------------------------
# Importar arquivos TXT
# --------------------------
print("\nImportando arquivos...")

for tabela in tabelas:
    arquivo = os.path.join(DNE_PATH, f"{tabela}.TXT")

    if not os.path.exists(arquivo):
        print(f"⚠ Arquivo NÃO encontrado: {arquivo}")
        continue

    query = f"""
        LOAD DATA LOCAL INFILE '{arquivo.replace("\\\\", "/")}'
        INTO TABLE {tabela}
        FIELDS TERMINATED BY ';'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES;
    """

    try:
        cursor.execute(query)
        conn.commit()
        print(f"Importado: {tabela}")
    except Exception as e:
        print(f"Erro ao importar {tabela}: {e}")

print("\nProcesso concluído.")

cursor.close()
conn.close()
