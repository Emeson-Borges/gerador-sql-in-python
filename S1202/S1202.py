import psycopg2
import os

# Conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="Quixada_2509_backups",
    user="postgres",
    password="20221612"
)

# Diretório onde você deseja salvar o arquivo
diretorio = "S1202/SQL_S1202"

# Certifique-se de que o diretório exista, se não existir
if not os.path.exists(diretorio):
    os.makedirs(diretorio)

# Nome do arquivo
nome_arquivo = "insert_S1202.sql"

# Caminho completo até o arquivo
caminho_arquivo = os.path.join(diretorio, nome_arquivo)

# Criação do objeto cursor
cursor = conn.cursor()

# Execução da consulta
cursor.execute("SELECT * FROM esocial.s1202 WHERE perapur='2023-07' and idevento in\
               (select idevento from esocial.historico WHERE evento='S1200' AND status='P' and cnpj='23444748000189')")
# Recuperação dos resultados da consulta
results = cursor.fetchall()

# Geração do script de insert
with open(caminho_arquivo, 'w') as f:
    for result in results:
        insert_query = f"INSERT INTO esocial.s1202 (idevento, indretif, nrrecibo, indapuracao, perapur, indguia, tpamb, procemi, verproc, tpinsc, nrinsc, cpftrab, nmtrab, dtnascto, tpinscsucessaovinc,\
        nrinscsucessaovinc, matricantsucessaovinc, dtadmsucessaovinc, observacaosucessaovinc, situacao, tipo, criado_por, alterado_por) \
        VALUES ('{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}', '{result[7]}', '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}', '{result[12]}', '{result[13]}', '{result[14]}', '{result[15]}', '{result[16]}', '{result[17]}', '{result[18]}', '{result[19]}', {result[20]}, '{result[12]}', '{result[22]}', '{result[23]}');"
        
        f.write(insert_query + '\n')
        print(insert_query)

# Fechamento do cursor e da conexão
cursor.close()
conn.close()
