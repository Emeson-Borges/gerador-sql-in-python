import psycopg2
import os

# Conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="novoprogresso_backups",
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
        insert_query = f"""INSERT INTO esocial.s2200 (idevento,indretif,nrrecibo,tpamb,procemi,verproc,tpinsc,nrinsc,cpftrab,nmtrab,sexo,racacor,estciv,grauinstr,nmsoc,dtnascto,paisnascto,paisnac,tplograd,dsclograd,nrlograd,complemento,bairro,cep,codmunic,uf,tmpresid,conding,deffisica,defvisual,defauditiva,defmental,defintelectual,reabreadap,infocota,observacao_infodeficiencia,foneprinc,emailprinc,matricula,tpregtrab,tpregprev,cadini,dtadm,tpadmissao,indadmissao,nrproctrab,tpregjor,natatividade,dtbase,cnpjsindcategprof,dtopcfgts,hipleg,justcontr,tpinsc_ideestabvinc,nrinsc_ideestabvinc,tpinsc_aprend,tpprov,dtexercicio,tpplanrp,indtetorgps,indabonoperm,dtiniabono,nmcargo,cbocargo,dtingrcargo,nmfuncao,cbofuncao,acumcargo,codcateg,vrsalfx,undsalfixo,dscsalvar,tpcontr,dtterm,claussec,objdet,tpinsc_localtrabgeral,nrinsc_localtrabgeral,desccomp_localtrabgeral,tplograd_localtempdom,dsclograd_localtempdom,nrlograd_localtempdom,complemento_localtempdom,bairro_localtempdom,cep_localtempdom,codmunic_localtempdom,uf_localtempdom,qtdhrssem,tpjornada,tmpparc,homoturno,dscjorn,nrprocjud,tpinsc_sucessaovinc,matricant_sucessaovinc,dttransf_sucessaovinc,observacao_sucessaovinc,cpfant,matricant,dtaltcpf,observacao_mudancacpf,dtiniaafast,codmotafast,dtdeslig,dtinicessao,situacao,tipo,criado_por,alterado_por)
        VALUES ('{result[1]!r}', '{result[2]!r}', '{result[3]!r}', '{result[4]!r}', '{result[5]!r}', '{result[6]!r}', '{result[7]!r}', '{result[8]!r}', '{result[9]!r}', '{result[10]!r}', '{result[11]!r}', '{result[12]!r}', '{result[13]!r}', '{result[14]!r}', '{result[15]!r}', '{result[16]!r}', '{result[17]!r}', '{result[18]!r}', '{result[19]!r}', '{result[20]!r}', '{result[12]!r}', '{result[22]!r}', '{result[23]!r}', '{result[24]!r}', '{result[25]!r}', '{result[26]!r}', '{result[27]!r}', '{result[28]!r}', '{result[29]!r}', '{result[30]!r}', '{result[31]!r}', '{result[32]!r}', '{result[33]!r}', '{result[34]!r}', '{result[35]!r}', '{result[36]!r}', '{result[37]!r}', '{result[38]!r}', '{result[39]!r}', '{result[40]!r}', '{result[41]!r}', '{result[42]!r}', '{result[43]!r}', '{result[44]!r}', '{result[45]!r}', '{result[46]!r}', '{result[47]!r}', '{result[48]!r}', '{result[49]!r}', '{result[50]!r}', '{result[51]!r}', '{result[52]!r}', '{result[53]!r}', '{result[54]!r}', '{result[55]!r}', '{result[56]!r}', '{result[57]!r}', '{result[58]!r}', '{result[59]!r}', '{result[60]!r}', '{result[61]!r}', '{result[62]!r}', '{result[63]!r}', '{result[64]!r}', '{result[65]!r}', '{result[66]!r}', '{result[67]!r}', '{result[68]!r}', '{result[69]!r}', '{result[70]!r}', '{result[71]!r}', '{result[72]!r}', '{result[73]!r}', '{result[74]!r}', '{result[75]!r}', '{result[76]!r}', '{result[77]!r}', '{result[78]!r}', '{result[79]!r}', '{result[80]!r}', '{result[81]!r}', '{result[82]!r}', '{result[83]!r}', '{result[84]!r}', '{result[85]!r}', '{result[86]!r}', '{result[87]!r}', '{result[88]!r}', '{result[89]!r}', '{result[90]!r}', '{result[91]!r}', '{result[92]!r}', '{result[93]!r}', '{result[94]!r}', '{result[95]!r}', '{result[96]!r}', '{result[97]!r}', '{result[98]!r}', '{result[99]!r}', '{result[100]!r}', '{result[101]!r}', '{result[102]!r}', '{result[103]!r}', '{result[104]!r}', '{result[105]!r}', '{result[106]!r}', '{result[107]!r}', '{result[108]!r}', '{result[109]!r}');"""
        f.write(insert_query + '\n')
        print(insert_query)

# Fechamento do cursor e da conexão
cursor.close()
conn.close()
