import os
import psycopg2

# Conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="iraucuba_backups",
    user="postgres",
    password="20221612"
)

# Diretório onde você deseja salvar o arquivo
diretorio = "S2410/SQL_S2410"

# Certifique-se de que o diretório exista, se não existir
if not os.path.exists(diretorio):
    os.makedirs(diretorio)

# Nome do arquivo
nome_arquivo = "insert_hist_S2410.sql"

# Caminho completo até o arquivo
caminho_arquivo = os.path.join(diretorio, nome_arquivo)

# Criação do objeto cursor
cursor = conn.cursor()

# Execução da consulta
cursor.execute(
    "SELECT * FROM esocial.historico WHERE evento='S2410' AND status='P'")
# Recuperação dos resultados da consulta
results = cursor.fetchall()

# Geração do script de insert
with open(caminho_arquivo, 'w') as f:
    for result in results:
        insert_query = f"""INSERT INTO esocial.historico (idevento, evento, status, criado_por, alterado_por, message, protocolo, cnpj, nr_recibo) VALUES ('{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}');"""
        f.write(insert_query + '\n')
        print(insert_query)

# Fechamento do cursor e da conexão
cursor.close()
conn.close()
