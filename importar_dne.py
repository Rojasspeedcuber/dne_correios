import os
import mysql.connector
from dotenv import load_dotenv

# --------------------------------------
# Carregar variáveis do arquivo .env
# --------------------------------------
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
DNE_PATH = os.getenv("DNE_PATH")

# --------------------------------------
# Conectar ao MySQL (sem DB primeiro)
# --------------------------------------
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    allow_local_infile=True
)
cursor = conn.cursor()

# --------------------------------------
# Criar banco de dados
# --------------------------------------
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
cursor.execute(f"USE {MYSQL_DATABASE}")
print(f"Banco '{MYSQL_DATABASE}' carregado.\n")

# --------------------------------------
# Tabelas fixas (não logradouro)
# --------------------------------------
tabelas_fixas = {
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
}

# Criar tabelas fixas
print("Criando tabelas principais...")
for nome, ddl in tabelas_fixas.items():
    cursor.execute(ddl)
    print(f" - OK: {nome}")

conn.commit()
print("\nTabelas principais criadas.\n")

# ------------------------------------------------------
# Criar tabela única para LOG_LOGRADOURO (todos estados)
# ------------------------------------------------------
cursor.execute("""
    CREATE TABLE IF NOT EXISTS LOG_LOGRADOURO (
        LOG_COD INT,
        LOG_NOME VARCHAR(200),
        TIPO VARCHAR(50),
        BAIRRO_INI INT,
        BAIRRO_FIM INT,
        LOCALIDADE_COD INT,
        CEP CHAR(8),
        UF CHAR(2)
    )
""")

print("Tabela unificada LOG_LOGRADOURO criada.\n")
conn.commit()

# --------------------------------------
# Importar arquivos fixos primeiro
# --------------------------------------
print("Importando arquivos básicos...")

for tabela in tabelas_fixas:
    arquivo = os.path.join(DNE_PATH, f"{tabela}.TXT")

    if not os.path.exists(arquivo):
        print(f"   ⚠ Arquivo não encontrado: {arquivo}")
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
        print(f"   ✔ Importado: {tabela}")
    except Exception as e:
        print(f"   ❌ Erro ao importar {tabela}: {e}")

# ------------------------------------------------------
# Importar TODOS os arquivos LOG_LOGRADOURO_XX.TXT
# ------------------------------------------------------
print("\nImportando LOG_LOGRADOURO_XX para a tabela unificada...")

for file in os.listdir(DNE_PATH):
    if file.startswith("LOG_LOGRADOURO_") and file.endswith(".TXT"):
        uf = file.split("_")[2].split(".")[0]  # extrai o UF
        caminho = os.path.join(DNE_PATH, file).replace("\\", "/")

        query = f"""
            LOAD DATA LOCAL INFILE '{caminho}'
            INTO TABLE LOG_LOGRADOURO
            FIELDS TERMINATED BY ';'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (LOG_COD, LOG_NOME, TIPO, BAIRRO_INI, BAIRRO_FIM, LOCALIDADE_COD, CEP)
            SET UF = '{uf}';
        """

        try:
            cursor.execute(query)
            conn.commit()
            print(f"   ✔ Importado: {file} → UF={uf}")
        except Exception as e:
            print(f"   ❌ Erro ao importar {file}: {e}")

print("\nPROCESSO FINALIZADO COM SUCESSO.")

cursor.close()
conn.close()
