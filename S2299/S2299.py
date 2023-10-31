import os

import psycopg2

# Conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="novoprogresso_backups",
    user="postgres",
    password="20221612"
)

# Diretório onde você deseja salvar o arquivo
diretorio = "S2299/SQL_S2299"

# Certifique-se de que o diretório exista, se não existir
if not os.path.exists(diretorio):
    os.makedirs(diretorio)

# Nome do arquivo
nome_arquivo = "insert_S2299.sql"

# Caminho completo até o arquivo
caminho_arquivo = os.path.join(diretorio, nome_arquivo)

# Criação do objeto cursor
cursor = conn.cursor()

# Execução da consulta
cursor.execute("SELECT * FROM esocial.s2299 WHERE idevento in (select idevento from esocial.historico WHERE evento='S2299' AND status='P')")
# Recuperação dos resultados da consulta
results = cursor.fetchall()

# Geração do script de insert

with open(caminho_arquivo, 'w') as f:
    for result in results:
        insert_query = f"""INSERT INTO esocial.s2299 (idevento,indretif,nrrecibo,indguia,tpamb,procemi,verproc,tpinsc,nrinsc,cpftrab,matricula,mtvdeslig,dtdeslig,dtavprv,indpagtoapi,dtprojfimapi,pensalim,percaliment,vralim,nrproctrab,tpinsc_sucessaovinc,nrinsc_sucessaovinc,cpfsubstituto_transftit,dtnascto_transftit,novocpf_mudancacpf,dtfimquar,situacao,tipo,criado_por,alterado_por)
            VALUES (
                {'NULL' if result[1] is None else f"'{result[1]}'"},
                {'NULL' if result[2] is None else f"'{result[2]}'"},
                {'NULL' if result[3] is None else f"'{result[3]}'"},
                {'NULL' if result[4] is None else f"'{result[4]}'"},
                {'NULL' if result[5] is None else f"'{result[5]}'"},
                {'NULL' if result[6] is None else f"'{result[6]}'"},
                {'NULL' if result[7] is None else f"'{result[7]}'"},
                {'NULL' if result[8] is None else f"'{result[8]}'"},
                {'NULL' if result[9] is None else f"'{result[9]}'"},
                {'NULL' if result[10] is None else f"'{result[10]}'"},
                {'NULL' if result[11] is None else f"'{result[11]}'"},
                {'NULL' if result[12] is None else f"'{result[12]}'"},
                {'NULL' if result[13] is None else f"'{result[13]}'"},
                {'NULL' if result[14] is None else f"'{result[14]}'"},
                {'NULL' if result[15] is None else f"'{result[15]}'"},
                {'NULL' if result[16] is None else f"'{result[16]}'"},
                {'NULL' if result[17] is None else f"'{result[17]}'"},
                {'NULL' if result[18] is None else f"'{result[18]}'"},
                {'NULL' if result[19] is None else f"'{result[19]}'"},
                {'NULL' if result[20] is None else f"'{result[20]}'"},
                {'NULL' if result[21] is None else f"'{result[21]}'"},
                {'NULL' if result[22] is None else f"'{result[22]}'"},
                {'NULL' if result[23] is None else f"'{result[23]}'"},
                {'NULL' if result[24] is None else f"'{result[24]}'"},
                {'NULL' if result[25] is None else f"'{result[25]}'"},
                {'NULL' if result[26] is None else f"'{result[26]}'"},
                {'NULL' if result[27] is None else f"'{result[27]}'"},
                {'NULL' if result[28] is None else f"{result[28]!r}"},
                {'NULL' if result[29] is None else f"'{result[29]}'"},
                {'NULL' if result[30] is None else f"'{result[30]}'"}
            );"""
        f.write(insert_query + '\n')
        print(insert_query)

#with open(caminho_arquivo, 'w') as f:
    #for result in results:
        #columns = ",".join([f"{col}" for col in result])
        #values = ",".join([f"'{val}'" if val is not None else "NULL" for val in result])
        #insert_query = f"INSERT INTO esocial.s2299 ({columns}) VALUES ({values});"
        #f.write(insert_query + '\n')
        #print(insert_query)

# Fechamento do cursor e da conexão
cursor.close()
conn.close()
