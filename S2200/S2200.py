import psycopg2
import os

# Conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="cmtamboril_backups",
    user="postgres",
    password="20221612"
)

# Diretório onde você deseja salvar o arquivo
diretorio = "S2200/SQL_S2200"

# Certifique-se de que o diretório exista, se não existir
if not os.path.exists(diretorio):
    os.makedirs(diretorio)

# Nome do arquivo
nome_arquivo = "insert_S2200.sql"

# Caminho completo até o arquivo
caminho_arquivo = os.path.join(diretorio, nome_arquivo)

# Criação do objeto cursor
cursor = conn.cursor()

# Execução da consulta
cursor.execute("SELECT * FROM esocial.s2200 WHERE idevento in (select idevento from esocial.historico WHERE evento='S2200' AND status='P')")
# Recuperação dos resultados da consulta
results = cursor.fetchall()

# Geração do script de insert
with open(caminho_arquivo, 'w') as f:
    for result in results:
        insert_query = f"INSERT INTO esocial.s2200 (idevento,indretif,nrrecibo,tpamb,procemi,verproc,tpinsc,nrinsc,cpftrab,nmtrab,sexo,racacor,estciv,grauinstr,nmsoc,dtnascto,paisnascto,paisnac,tplograd,dsclograd,nrlograd,complemento,bairro,cep,codmunic,uf,tmpresid,conding,deffisica,defvisual,defauditiva,defmental,defintelectual,reabreadap,infocota,observacao_infodeficiencia,foneprinc,emailprinc,matricula,tpregtrab,tpregprev,cadini,dtadm,tpadmissao,indadmissao,nrproctrab,tpregjor,natatividade,dtbase,cnpjsindcategprof,dtopcfgts,hipleg,justcontr,tpinsc_ideestabvinc,nrinsc_ideestabvinc,tpinsc_aprend,tpprov,dtexercicio,tpplanrp,indtetorgps,indabonoperm,dtiniabono,nmcargo,cbocargo,dtingrcargo,nmfuncao,cbofuncao,acumcargo,codcateg,vrsalfx,undsalfixo,dscsalvar,tpcontr,dtterm,claussec,objdet,tpinsc_localtrabgeral,nrinsc_localtrabgeral,desccomp_localtrabgeral,tplograd_localtempdom,dsclograd_localtempdom,nrlograd_localtempdom,complemento_localtempdom,bairro_localtempdom,cep_localtempdom,codmunic_localtempdom,uf_localtempdom,qtdhrssem,tpjornada,tmpparc,homoturno,dscjorn,nrprocjud,tpinsc_sucessaovinc,matricant_sucessaovinc,dttransf_sucessaovinc,observacao_sucessaovinc,cpfant,matricant,dtaltcpf,observacao_mudancacpf,dtiniaafast,codmotafast,dtdeslig,dtinicessao,situacao,tipo,criado_por,alterado_por)
        VALUES ('{result[1]}', '{result[2]}', '{result[3]}', '{result[4]}', '{result[5]}', '{result[6]}', '{result[7]}', '{result[8]}', '{result[9]}', '{result[10]}', '{result[11]}', '{result[12]}', '{result[13]}', '{result[14]}', '{result[15]}', '{result[16]}', '{result[17]}', '{result[18]}', '{result[19]}', '{result[20]}', '{result[12]}', '{result[22]}', '{result[23]}', '{result[24]}', '{result[25]}', '{result[26]}', '{result[27]}', '{result[28]}', '{result[29]}', '{result[30]}', '{result[31]}', '{result[32]}', '{result[33]}', '{result[34]}', '{result[35]}', '{result[36]}', '{result[37]}', '{result[38]}', '{result[39]}', '{result[40]}', '{result[41]}', '{result[42]}', '{result[43]}', '{result[44]}', '{result[45]}', '{result[46]}', '{result[47]}', '{result[48]}', '{result[49]}', '{result[50]}', '{result[51]}', '{result[52]}', '{result[53]}', '{result[54]}', '{result[55]}', '{result[56]}', '{result[57]}', '{result[58]}', '{result[59]}', '{result[60]}', '{result[61]}', '{result[62]}', '{result[63]}', '{result[64]}', '{result[65]}', '{result[66]}', '{result[67]}', '{result[68]}', '{result[69]}', '{result[70]}', '{result[71]}', '{result[72]}', '{result[73]}', '{result[74]}', '{result[75]}', '{result[76]}', '{result[77]}', '{result[78]}', '{result[79]}', '{result[80]}', '{result[81]}', '{result[82]}', '{result[83]}', '{result[84]}', '{result[85]}', '{result[86]}', '{result[87]}', '{result[88]}', '{result[89]}', '{result[90]}', '{result[91]}', '{result[92]}', '{result[93]}', '{result[94]}', '{result[95]}', '{result[96]}', '{result[97]}', '{result[98]}', '{result[99]}', '{result[100]}', '{result[101]}', '{result[102]}', '{result[103]}', '{result[104]}', '{result[105]}', '{result[106]}', '{result[107]}', '{result[108]}', '{result[109]}');"
        f.write(insert_query + '\n')
        print(insert_query)

# Fechamento do cursor e da conexão
cursor.close()
conn.close()
