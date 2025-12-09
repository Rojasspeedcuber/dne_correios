import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
DNE_PATH = os.getenv("DNE_PATH")

# String de conexão SQLAlchemy
engine = create_engine(f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}')

# --- DEFINIÇÃO DOS LAYOUTS (Baseado no seu arquivo .doc) ---
# O arquivo .doc indica que o separador é '@' 
# Abaixo, mapeamos os nomes das colunas conforme a documentação para cada arquivo.

LAYOUTS = {
    # [cite: 8] Tabela de Localidades (Municípios, Distritos)
    'LOG_LOCALIDADE': [
        'LOC_NU', 'UFE_SG', 'LOC_NO', 'CEP', 'LOC_IN_SIT', 
        'LOC_IN_TIPO_LOC', 'LOC_NU_SUB', 'LOC_NO_ABREV', 'MUN_NU'
    ],
    
    #  Tabela de Bairros
    'LOG_BAIRRO': [
        'BAI_NU', 'UFE_SG', 'LOC_NU', 'BAI_NO', 'BAI_NO_ABREV'
    ],
    
    #  Tabela de Logradouros (Base para os arquivos LOG_LOGRADOURO_XX)
    'LOG_LOGRADOURO': [
        'LOG_NU', 'UFE_SG', 'LOC_NU', 'BAI_NU_INI', 'BAI_NU_FIM', 
        'LOG_NO', 'LOG_COMPLEMENTO', 'CEP', 'TLO_TX', 'LOG_STA_TLO', 'LOG_NO_ABREV'
    ],
    
    # [cite: 11] Caixa Postal Comunitária
    'LOG_CPC': [
        'CPC_NU', 'UFE_SG', 'LOC_NU', 'CPC_NO', 'CPC_ENDERECO', 'CEP'
    ],
    
    # [cite: 5] Faixa de UF
    'LOG_FAIXA_UF': [
        'UFE_SG', 'UFE_CEP_INI', 'UFE_CEP_FIM'
    ],
    
    #  Faixa de Localidade
    'LOG_FAIXA_LOCALIDADE': [
        'LOC_NU', 'LOC_CEP_INI', 'LOC_CEP_FIM', 'LOC_TIPO_FAIXA'
    ],
    
    # Faixa de Bairro
    'LOG_FAIXA_BAIRRO': [
        'BAI_NU', 'FCB_CEP_INI', 'FCB_CEP_FIM'
    ],
    
    # [cite: 15] Grandes Usuários
    'LOG_GRANDE_USUARIO': [
        'GRU_NU', 'UFE_SG', 'LOC_NU', 'BAI_NU', 'LOG_NU', 
        'GRU_NO', 'GRU_ENDERECO', 'CEP', 'GRU_NO_ABREV'
    ],
    
    # [cite: 17] Unidades Operacionais
    'LOG_UNID_OPER': [
        'UOP_NU', 'UFE_SG', 'LOC_NU', 'BAI_NU', 'LOG_NU', 
        'UOP_NO', 'UOP_ENDERECO', 'CEP', 'UOP_IN_CP', 'UOP_NO_ABREV'
    ],
     
    # [cite: 19] Países (Tabela auxiliar)
    'ECT_PAIS': [
        'PAI_SG', 'PAI_SG_ALTERNATIVA', 'PAI_NO_PORTUGUES', 
        'PAI_NO_INGLES', 'PAI_NO_FRANCES', 'PAI_ABREVIATURA'
    ]
    # Adicione outros layouts (LOG_VAR_*, LOG_FAIXA_CPC) conforme necessário seguindo o padrão acima
}

def process_file(filepath, table_name, columns):
    """Lê o arquivo txt e insere no banco de dados."""
    print(f"Processando {os.path.basename(filepath)} para a tabela {table_name}...")
    
    try:
        # Lê o arquivo em chunks para não estourar a memória (arquivos de SP e MG são grandes)
        # encoding='latin1' é o padrão comum para arquivos legados do governo BR
        # sep='@' conforme instrução do doc 
        # header=None pois os arquivos txt são dados crus
        chunk_size = 50000
        for chunk in pd.read_csv(filepath, sep='@', header=None, names=columns, 
                                 encoding='latin1', dtype=str, chunksize=chunk_size):
            
            # Insere no MySQL. 'append' adiciona dados se a tabela já existir.
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
            
        print(f"-> {table_name} importado com sucesso.")
        
    except Exception as e:
        print(f"Erro ao processar {filepath}: {e}")

def main():
    # 1. Processar arquivos estáticos (que não terminam em UF)
    # Ex: LOG_LOCALIDADE.TXT, LOG_BAIRRO.TXT
    for filename, columns in LAYOUTS.items():
        if filename == 'LOG_LOGRADOURO': continue # Pula logradouro pois é tratado no passo 2
        
        # Procura o arquivo exato
        full_path = os.path.join(DNE_PATH, f"{filename}.TXT")
        # Tenta extensão minúscula também caso o sistema de arquivos seja case-sensitive
        if not os.path.exists(full_path):
             full_path = os.path.join(DNE_PATH, f"{filename}.txt")
             
        if os.path.exists(full_path):
            process_file(full_path, filename.lower(), columns)
        else:
            print(f"Arquivo {filename} não encontrado no diretório.")

    # 2. Processar Logradouros (LOG_LOGRADOURO_XX.TXT)
    # O DNE separa logradouros por estado (SP, RJ, etc) 
    # Vamos consolidar tudo numa tabela única 'log_logradouro'
    log_columns = LAYOUTS['LOG_LOGRADOURO']
    
    # Busca todos os arquivos que começam com LOG_LOGRADOURO_
    logradouro_files = glob.glob(os.path.join(DNE_PATH, "LOG_LOGRADOURO_*.TXT"))
    logradouro_files += glob.glob(os.path.join(DNE_PATH, "LOG_LOGRADOURO_*.txt"))
    
    print(f"\nEncontrados {len(logradouro_files)} arquivos de Logradouro.")
    
    for filepath in logradouro_files:
        # Verifica se não é um arquivo de variação ou outro similar
        # Queremos apenas LOG_LOGRADOURO_UF.TXT
        filename = os.path.basename(filepath).upper()
        
        # Lógica simples para garantir que estamos pegando os arquivos de UF (ex: LOG_LOGRADOURO_SP.TXT)
        # Ignora LOG_VAR_LOG ou LOG_LOCALIDADE se caírem no glob por engano
        if "LOG_LOGRADOURO_" in filename and len(filename.split('.')[0].split('_')[-1]) == 2:
            process_file(filepath, 'log_logradouro', log_columns)

if __name__ == "__main__":
    main()