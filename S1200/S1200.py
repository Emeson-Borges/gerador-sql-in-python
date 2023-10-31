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
diretorio = "S1200/SQL_S1200"

# Certifique-se de que o diretório exista, se não existir
if not os.path.exists(diretorio):
    os.makedirs(diretorio)

# Nome do arquivo
nome_arquivo = "insert_S1200.sql"

# Caminho completo até o arquivo
caminho_arquivo = os.path.join(diretorio, nome_arquivo)

# Criação do objeto cursor
cursor = conn.cursor()

# Execução da consulta
cursor.execute("SELECT * FROM esocial.s1200 WHERE perapur='2023-07' and idevento in\
               (select idevento from esocial.historico WHERE evento='S1200' AND status='P' and cnpj='23444748000189')")
# Recuperação dos resultados da consulta
results = cursor.fetchall()

# Geração do script de insert
with open(caminho_arquivo, 'w') as f:
    for result in results:
        def format_value(value):
            return f"{value}" if value is not None else 'NULL'
        
        insert_query = f"INSERT INTO esocial.s1200 (idevento, indretif, nrrecibo, indapuracao, perapur, indguia, tpamb, procemi, verproc, tpinsc, nrinsc, cpftrab, nmtrab, dtnascto, tpinscsucessaovinc,\
        nrinscsucessaovinc, matricantsucessaovinc, dtadmsucessaovinc, observacaosucessaovinc, situacao, tipo, criado_por, alterado_por) \
        VALUES ({result[1]!r}, {result[2]!r}, {format_value(result[3])!r}, {result[4]!r}, {result[5]!r}, {result[6]!r}, {result[7]!r}, {result[8]!r}, {result[9]!r}, {result[10]!r}, {result[11]!r}, {result[12]!r}, {result[13]!r}, {result[14]!r}, {result[15]!r}, {result[16]!r}, {result[17]!r}, {result[18]!r}, {result[19]!r}, {result[20]}, {result[12]!r}, {result[22]!r}, {result[23]});"
        
        # Substitua as aspas em branco (vazias) por NULL
        insert_query = insert_query.replace("''", "NULL")
        
        f.write(insert_query + '\n')
        print(insert_query)

# Fechamento do cursor e da conexão
cursor.close()
conn.close()
