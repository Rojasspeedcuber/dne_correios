#!/usr/bin/env python3
"""
Importador definitivo do DNE (versão delimitada) para MySQL.
- Usa .env para configuração
- Cria DB se necessário
- Cria tabelas principais
- Importa todos os .TXT da pasta DNE_PATH
- Detecta delimitador automaticamente
- Tenta LOAD DATA LOCAL INFILE com CHARACTER SET latin1 (compatível DNE)
- Em caso de falha, realiza importação manual linha-a-linha via Python
"""

import os
import sys
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from collections import Counter

# ---------------------------
# Carregar configuração (.env)
# ---------------------------
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "dne")
DNE_PATH = os.getenv("DNE_PATH")

if not DNE_PATH:
    print("ERRO: variável DNE_PATH não informada no .env")
    sys.exit(1)

DNE_PATH = DNE_PATH.replace("\\", "/")  # normalizar para barras

# ---------------------------
# Funções utilitárias
# ---------------------------
def conectar_sem_db():
    return mysql.connector.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, port=MYSQL_PORT
    )

def conectar_com_db():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        allow_local_infile=True
    )

def garantir_database():
    try:
        conn = conectar_sem_db()
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS `{}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;".format(MYSQL_DATABASE))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[OK] Banco '{MYSQL_DATABASE}' garantido/criado.")
    except Error as e:
        print("[ERRO] Não foi possível criar/verificar o banco:", e)
        sys.exit(1)

def detectar_delimitador(caminho):
    """
    Lê a primeira linha não-vazia do arquivo e escolhe o delimitador
    dentre os candidatos que ocorrem com maior frequência.
    """
    candidatos = ['@', ';', '|', '\t', ',']
    try:
        with open(caminho, "r", encoding="latin1", errors="replace") as f:
            for linha in f:
                linha = linha.strip()
                if linha:
                    counts = {c: linha.count(c) for c in candidatos}
                    # Escolhe o que tem maior contagem (se >0)
                    delim, cnt = max(counts.items(), key=lambda x: x[1])
                    if cnt > 0:
                        return delim
                    else:
                        return ';'  # fallback
    except Exception as e:
        print(f"[WARN] Não foi possível detectar delimitador em {caminho}: {e}")
        return ';'

def executar_sql(cur, sql, params=None, commit=False):
    try:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        if commit:
            cur.connection.commit()
        return True, None
    except Exception as e:
        return False, e

# ---------------------------
# Criação das tabelas
# (esquemas genéricos; ajuste se desejar colunas mais específicas)
# ---------------------------
def criar_tabelas(cur):
    ddl = {}

    ddl["ECT_PAIS"] = """
    CREATE TABLE IF NOT EXISTS ECT_PAIS (
        PAIS_CODIGO INT,
        PAIS_NOME VARCHAR(200),
        SIGLA VARCHAR(10)
    );"""

    ddl["LOG_BAIRRO"] = """
    CREATE TABLE IF NOT EXISTS LOG_BAIRRO (
        BAIRRO_COD INT,
        UFE_SG CHAR(2),
        LOC_NU INT,
        BAIRRO_NO VARCHAR(200)
    );"""

    ddl["LOG_CPC"] = """
    CREATE TABLE IF NOT EXISTS LOG_CPC (
        CPC_NU INT,
        UFE_SG CHAR(2),
        LOC_NU INT,
        LOG_NU INT,
        CTC_NO VARCHAR(200),
        CEP CHAR(8)
    );"""

    ddl["LOG_FAIXA_BAIRRO"] = """
    CREATE TABLE IF NOT EXISTS LOG_FAIXA_BAIRRO (
        BAIRRO_NU INT,
        CEP_INI CHAR(8),
        CEP_FIM CHAR(8)
    );"""

    ddl["LOG_FAIXA_CPC"] = """
    CREATE TABLE IF NOT EXISTS LOG_FAIXA_CPC (
        CPC_NU INT,
        CEP_INI CHAR(8),
        CEP_FIM CHAR(8)
    );"""

    ddl["LOG_FAIXA_LOCALIDADE"] = """
    CREATE TABLE IF NOT EXISTS LOG_FAIXA_LOCALIDADE (
        LOC_NU INT,
        CEP_INI CHAR(8),
        CEP_FIM CHAR(8)
    );"""

    ddl["LOG_FAIXA_UF"] = """
    CREATE TABLE IF NOT EXISTS LOG_FAIXA_UF (
        UFE_SG CHAR(2),
        CEP_INI CHAR(8),
        CEP_FIM CHAR(8)
    );"""

    ddl["LOG_FAIXA_UOP"] = """
    CREATE TABLE IF NOT EXISTS LOG_FAIXA_UOP (
        UOP_NU INT,
        CEP_INI CHAR(8),
        CEP_FIM CHAR(8)
    );"""

    ddl["LOG_GRANDE_USUARIO"] = """
    CREATE TABLE IF NOT EXISTS LOG_GRANDE_USUARIO (
        GU_NU INT,
        UFE_SG CHAR(2),
        LOC_NU INT,
        LOG_NU INT,
        GU_NO VARCHAR(200),
        CEP CHAR(8)
    );"""

    ddl["LOG_LOCALIDADE"] = """
    CREATE TABLE IF NOT EXISTS LOG_LOCALIDADE (
        LOC_NU INT,
        UFE_SG CHAR(2),
        LOC_NO VARCHAR(200),
        CEP CHAR(8)
    );"""

    # tabela unificada para todos os LOG_LOGRADOURO_XX.TXT
    ddl["LOG_LOGRADOURO"] = """
    CREATE TABLE IF NOT EXISTS LOG_LOGRADOURO (
        LOG_NU INT,
        UFE_SG CHAR(2),
        LOC_NU INT,
        BAIRRO_NU_INI INT,
        BAIRRO_NU_FIM INT,
        LOG_NO VARCHAR(300),
        CEP CHAR(8),
        UF CHAR(2)
    );"""

    ddl["LOG_LOGRADOURO_AC"] = """
    CREATE TABLE IF NOT EXISTS LOG_LOGRADOURO_AC (
        placeholder INT
    );"""  # mantemos se quiser compatibilidade com arquivos individuais (não usado)

    # criar todas
    for nome, sql in ddl.items():
        ok, err = executar_sql(cur, sql)
        if not ok:
            print(f"[ERRO] criar tabela {nome}: {err}")
            sys.exit(1)
        else:
            print(f"[OK] Tabela criada/verificada: {nome}")

# ---------------------------
# Import: tenta LOAD DATA; se falhar, faz import manual
# ---------------------------
def importar_arquivo_via_load(cur, caminho, tabela, delim, extra_set=None):
    """
    Tenta importar usando LOAD DATA LOCAL INFILE com CHARACTER SET latin1.
    :param extra_set: string para colocar após colunas, ex: "SET UF='AC'"
    """
    caminho = caminho.replace("\\", "/")
    set_clause = f" {extra_set}" if extra_set else ""
    sql = f"""
        LOAD DATA LOCAL INFILE '{caminho}'
        INTO TABLE {tabela}
        CHARACTER SET latin1
        FIELDS TERMINATED BY '{delim}'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        {set_clause};
    """
    ok, err = executar_sql(cur, sql, commit=True)
    return ok, err

def importar_arquivo_manual(cur, caminho, tabela, delim, uf_value=None):
    """
    Caso LOAD DATA falhe, importamos manualmente: lemos cada linha e inserimos com INSERT.
    Este método é mais lento, mas tolerante.
    """
    caminho = caminho.replace("\\", "/")
    inserir_sql = None

    # Montar SQL de inserção genérico por tabela
    if tabela == "LOG_LOGRADOURO":
        inserir_sql = ("INSERT INTO LOG_LOGRADOURO (LOG_NU, UFE_SG, LOC_NU, BAIRRO_NU_INI, BAIRRO_NU_FIM, LOG_NO, CEP, UF) "
                       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
    elif tabela == "ECT_PAIS":
        inserir_sql = "INSERT INTO ECT_PAIS (PAIS_CODIGO, PAIS_NOME, SIGLA) VALUES (%s,%s,%s)"
    elif tabela == "LOG_BAIRRO":
        inserir_sql = "INSERT INTO LOG_BAIRRO (BAIRRO_COD, UFE_SG, LOC_NU, BAIRRO_NO) VALUES (%s,%s,%s,%s)"
    elif tabela == "LOG_CPC":
        inserir_sql = "INSERT INTO LOG_CPC (CPC_NU, UFE_SG, LOC_NU, LOG_NU, CTC_NO, CEP) VALUES (%s,%s,%s,%s,%s,%s)"
    elif tabela == "LOG_GRANDE_USUARIO":
        inserir_sql = "INSERT INTO LOG_GRANDE_USUARIO (GU_NU, UFE_SG, LOC_NU, LOG_NU, GU_NO, CEP) VALUES (%s,%s,%s,%s,%s,%s)"
    elif tabela == "LOG_LOCALIDADE":
        inserir_sql = "INSERT INTO LOG_LOCALIDADE (LOC_NU, UFE_SG, LOC_NO, CEP) VALUES (%s,%s,%s,%s)"
    else:
        # tentativa genérica: inserir toda a linha crua em coluna LOG_NO caso exista
        # se tabela desconhecida, pula
        print(f"[WARN] Inserção manual não suportada para tabela {tabela}. Pulando.")
        return False, "manual-insert-not-supported"

    batch = []
    total = 0
    try:
        with open(caminho, "r", encoding="latin1", errors="replace") as f:
            header = next(f, None)  # pular header
            for linha in f:
                linha = linha.rstrip("\r\n")
                if not linha:
                    continue
                cols = linha.split(delim)
                # normalizar tamanho
                # Para LOG_LOGRADOURO esperamos pelo menos 7 colunas (ajuste conforme seu leiaute)
                if tabela == "LOG_LOGRADOURO":
                    # preencher com None se faltar
                    while len(cols) < 7:
                        cols.append(None)
                    # mapear primeiro 7 campos: LOG_NU, UFE_SG, LOC_NU, BAIRRO_NU_INI, BAIRRO_NU_FIM, LOG_NO, CEP
                    vals = (
                        to_int(cols[0]),
                        safe_str(cols[1]),
                        to_int(cols[2]),
                        to_int(cols[3]),
                        to_int(cols[4]),
                        safe_str(cols[5]),
                        safe_str(cols[6]),
                        uf_value
                    )
                elif tabela == "ECT_PAIS":
                    while len(cols) < 3: cols.append(None)
                    vals = (to_int(cols[0]), safe_str(cols[1]), safe_str(cols[2]))
                elif tabela == "LOG_BAIRRO":
                    while len(cols) < 4: cols.append(None)
                    vals = (to_int(cols[0]), safe_str(cols[1]), to_int(cols[2]), safe_str(cols[3]))
                elif tabela == "LOG_CPC":
                    while len(cols) < 6: cols.append(None)
                    vals = (to_int(cols[0]), safe_str(cols[1]), to_int(cols[2]), to_int(cols[3]), safe_str(cols[4]), safe_str(cols[5]))
                elif tabela == "LOG_GRANDE_USUARIO":
                    while len(cols) < 6: cols.append(None)
                    vals = (to_int(cols[0]), safe_str(cols[1]), to_int(cols[2]), to_int(cols[3]), safe_str(cols[4]), safe_str(cols[5]))
                elif tabela == "LOG_LOCALIDADE":
                    while len(cols) < 4: cols.append(None)
                    vals = (to_int(cols[0]), safe_str(cols[1]), safe_str(cols[2]), safe_str(cols[3]))
                else:
                    continue

                batch.append(vals)
                total += 1

                if len(batch) >= 1000:
                    cur = cur_global
                    cur.executemany(inserir_sql, batch)
                    cur.connection.commit()
                    batch = []

            # inserir o que restou
            if batch:
                cur = cur_global
                cur.executemany(inserir_sql, batch)
                cur.connection.commit()

        return True, f"Importação manual concluída ({total} linhas)."
    except Exception as e:
        return False, e

def safe_str(x):
    if x is None:
        return None
    return x.strip() if isinstance(x, str) else str(x)

def to_int(x):
    try:
        return int(x) if x not in (None, '', 'NULL') else None
    except:
        return None

# ---------------------------
# Processo principal
# ---------------------------
if __name__ == "__main__":
    print("=== Iniciando importador definitivo do DNE ===")
    # 1) garantir DB
    garantir_database()

    # 2) conectar
    try:
        conn = conectar_com_db()
    except Error as e:
        print("[ERRO] Falha ao conectar ao MySQL:", e)
        sys.exit(1)

    cur = conn.cursor()
    # exposto para funções manuais
    cur_global = cur

    # 3) criar tabelas
    criar_tabelas(cur)

    # 4) enumerar arquivos .TXT no diretório
    if not os.path.isdir(DNE_PATH):
        print(f"[ERRO] Diretório DNE_PATH não existe: {DNE_PATH}")
        sys.exit(1)

    arquivos = [f for f in os.listdir(DNE_PATH) if f.upper().endswith(".TXT")]
    arquivos.sort()

    print(f"\nEncontrados {len(arquivos)} arquivos .TXT em {DNE_PATH}\n")

    resumo = {"importados": [], "falhas": []}

    for arq in arquivos:
        caminho = os.path.join(DNE_PATH, arq)
        print(f">>> Processando: {arq}")

        # identificar tabela alvo
        nome_sem_ext = os.path.splitext(arq)[0].upper()

        if nome_sem_ext.startswith("LOG_LOGRADOURO_"):
            tabela = "LOG_LOGRADOURO"
            uf = nome_sem_ext.split("_")[-1]  # UF
            extra_set = f"SET UF = '{uf}'"
        else:
            tabela = nome_sem_ext
            extra_set = None

        # detectar delimitador
        delim = detectar_delimitador(caminho)
        print(f"    Delimitador detectado: '{delim}'")

        # tentativa LOAD DATA
        ok, err = importar_arquivo_via_load(cur, caminho, tabela, delim, extra_set)
        if ok:
            print(f"    ✔ Importado (LOAD DATA): {arq} -> {tabela}")
            resumo["importados"].append(arq)
            continue
        else:
            print(f"    ❌ LOAD DATA falhou para {arq}. Tentando importação manual. Erro: {err}")

        # tentativa manual (mais lenta)
        try:
            ok2, msg = importar_arquivo_manual(cur, caminho, tabela, delim, uf_value=(uf if nome_sem_ext.startswith("LOG_LOGRADOURO_") else None))
            if ok2:
                print(f"    ✔ Importado (manual): {arq} -> {tabela} ({msg})")
                resumo["importados"].append(arq)
            else:
                print(f"    ❌ Falha na importação manual de {arq}: {msg}")
                resumo["falhas"].append((arq, msg))
        except Exception as e:
            print(f"    ❌ Exceção inesperada importando {arq}: {e}")
            resumo["falhas"].append((arq, str(e)))

    # 5) resumo final
    print("\n=== Resumo ===")
    print(f"Total arquivos: {len(arquivos)}")
    print(f"Importados com sucesso: {len(resumo['importados'])}")
    if resumo['importados']:
        for x in resumo['importados']:
            print("  -", x)
    print(f"Falhas: {len(resumo['falhas'])}")
    for f, e in resumo['falhas']:
        print("  -", f, ":", e)

    cur.close()
    conn.close()

    print("\nProcesso finalizado.")
