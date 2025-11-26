import os
import mysql.connector
from dotenv import load_dotenv
import tempfile
import shutil

# Carregar .env
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "dne")
DNE_PATH = os.getenv("DNE_PATH")  # <-- caminho deve estar com / ou em raw string

# Conexão com MySQL
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    allow_local_infile=True
)
cur = conn.cursor()


# -------------------------------------------------------------------
# UTILITÁRIO: converter arquivo ISO-8859-1 → UTF-8
# -------------------------------------------------------------------
def converter_para_utf8(caminho_original):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")

    with open(caminho_original, "r", encoding="latin1", errors="replace") as src:
        with open(temp_file.name, "w", encoding="utf8") as dst:
            shutil.copyfileobj(src, dst)

    return temp_file.name


# -------------------------------------------------------------------
# Função para importar arquivo via LOAD DATA INFILE
# -------------------------------------------------------------------
def importar_arquivo(tabela, arquivo):
    caminho_abs = os.path.join(DNE_PATH, arquivo)

    if not os.path.exists(caminho_abs):
        print(f"   ❌ Arquivo NÃO encontrado: {caminho_abs}")
        return

    # converter encoding
    arquivo_utf8 = converter_para_utf8(caminho_abs)

    try:
        sql = f"""
            LOAD DATA LOCAL INFILE '{arquivo_utf8.replace("\\", "/")}'
            INTO TABLE {tabela}
            CHARACTER SET utf8
            FIELDS TERMINATED BY '@'
            LINES TERMINATED BY '\\n'
        """
        cur.execute(sql)
        conn.commit()
        print(f"   ✔ Importado: {arquivo}")

    except Exception as e:
        print(f"   ❌ Erro ao importar {arquivo}: {e}")

    finally:
        os.remove(arquivo_utf8)


# -------------------------------------------------------------------
# IMPORTAÇÃO DOS ARQUIVOS PRINCIPAIS
# -------------------------------------------------------------------
arquivos_principais = {
    "ECT_PAIS": "ECT_PAIS.TXT",
    "LOG_BAIRRO": "LOG_BAIRRO.TXT",
    "LOG_CPC": "LOG_CPC.TXT",
    "LOG_FAIXA_BAIRRO": "LOG_FAIXA_BAIRRO.TXT",
    "LOG_FAIXA_CPC": "LOG_FAIXA_CPC.TXT",
    "LOG_FAIXA_LOCALIDADE": "LOG_FAIXA_LOCALIDADE.TXT",
    "LOG_FAIXA_UF": "LOG_FAIXA_UF.TXT",
    "LOG_FAIXA_UOP": "LOG_FAIXA_UOP.TXT",
    "LOG_GRANDE_USUARIO": "LOG_GRANDE_USUARIO.TXT",
    "LOG_LOCALIDADE": "LOG_LOCALIDADE.TXT"
}

print("\nImportando arquivos principais...\n")
for tabela, arquivo in arquivos_principais.items():
    importar_arquivo(tabela, arquivo)

# -------------------------------------------------------------------
# IMPORTAR LOGRADOURO DE TODOS OS ESTADOS
# -------------------------------------------------------------------
print("\nImportando LOG_LOGRADOURO_XX ...\n")

ufs = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA",
    "PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
]

for uf in ufs:
    nome_arq = f"LOG_LOGRADOURO_{uf}.TXT"
    importar_arquivo("LOG_LOGRADOURO", nome_arq)

print("\nPROCESSO FINALIZADO.\n")
cur.close()
conn.close()
