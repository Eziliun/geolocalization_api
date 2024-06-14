import os
import requests
import cx_Oracle
from db.db_oracle_config import DbOracleConfig
from dotenv import load_dotenv
from datetime import datetime

def preencher_tabela_geo_clientes(db_config, codcliente, nome, cgc, latitude=None, longitude=None):
    date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    sql_insert = """
        INSERT INTO SIGA.GEO_CLIENTES (CODCLIENTE, RAZAO, CNPJ, LATITUDE, LONGITUDE, DATE_TIME)
        VALUES (:1, :2, :3, :4, :5, TO_TIMESTAMP(:6, 'YYYY-MM-DD HH24:MI:SS'))
    """
    execution_status, result = db_config.execute(sql_insert, (codcliente, nome, cgc, latitude, longitude, date_time))

    if execution_status:
        print("Dados inseridos com sucesso na tabela SIGA.GEO_CLIENTES.")
    else:
        print("Erro ao inserir dados na tabela SIGA.GEO_CLIENTES:", result)


def obter_coordenadas_endereco(db_config, endereco):
    if verificar_limite_alcancado(db_config):
        print("Limite de requests alcançado. Não será feita a chamada à API.")
        return None, None

    load_dotenv()
    map_api_key = os.getenv("MAP_API_KEY")
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{endereco}.json?access_token={map_api_key}"

    try:
        response = requests.get(url)
        data = response.json()
        if data['features']:
            coordinates = data['features'][0]['geometry']['coordinates']
            atualizar_contagem_requests(db_config)
            return coordinates[1], coordinates[0]
        else:
            print("Endereço não encontrado:", endereco)
            return None, None
    except Exception as e:
        print("Erro ao obter coordenadas do endereço:", str(e))
        return None, None


def atualizar_contagem_requests(db_config):
    mes = datetime.now().strftime('%Y-%m')
    sql_select = "SELECT contagem_mes FROM siga.MONITORAMENTO_API_GEOCLIENTE WHERE mes = :1"
    execution_status, result = db_config.execute(sql_select, (mes,))

    if execution_status and result:
        sql_update = """
            UPDATE siga.MONITORAMENTO_API_GEOCLIENTE
            SET contagem_mes = contagem_mes + 1,
                ultima_atualizacao = SYSTIMESTAMP,
                limite_alcancado = CASE WHEN contagem_mes + 1 >= 90000 THEN 'S' ELSE 'N' END
            WHERE mes = :1
        """
        db_config.execute(sql_update, (mes,))
    else:
        sql_insert = """
            INSERT INTO siga.MONITORAMENTO_API_GEOCLIENTE (mes, contagem_mes, ultima_atualizacao, limite_alcancado)
            VALUES (:1, 1, SYSTIMESTAMP, 'N')
        """
        db_config.execute(sql_insert, (mes,))
    return True


def verificar_limite_alcancado(db_config):
    mes_atual = datetime.now().strftime('%Y-%m')
    sql_select = "SELECT limite_alcancado, contagem_mes FROM siga.MONITORAMENTO_API_GEOCLIENTE WHERE mes = :1"
    execution_status, result = db_config.execute(sql_select, (mes_atual,))
    if execution_status and result:
        limite_alcancado = result[0]['LIMITE_ALCANCADO']
        requests_totais = result[0]['CONTAGEM_MES']
        if limite_alcancado == 'S' and requests_totais >= 90000:
            print("Limite de requests já foi alcançado para este mês. Interrompendo o código.")
            return True
        else:
            return False

    elif execution_status and not result :
        atualizar_contagem_requests(db_config)
        return False
    else:
        print("Erro ao verificar o limite alcançado na tabela siga.MONITORAMENTO_API_GEOCLIENTE.")
    return True


def main():
    db_config = DbOracleConfig()

    if verificar_limite_alcancado(db_config):
        print("Limite de requests alcançado. Encerrando a execução do script.")
        return

    sql_query = f"""
        SELECT distinct 
            codcliente,
            a1_nome,
            a1_cgc,
            a1_end,
            a1_mun,
            a1_est,
            a1_bairro,
            flag_exist,
            date_time,
            diff_hours
        FROM
            (
                SELECT
                    a.codcliente,
                    a1_nome,
                    a1_cgc,
                    a1_end,
                    a1_mun,
                    a1_est,
                    a1_bairro,
                    CASE
                        WHEN g.codcliente IS NULL THEN
                            0
                        ELSE
                            1
                    END                                         flag_exist,
                    nvl(MAX(date_time), sysdate)  AS date_time,
                    nvl(EXTRACT(DAY FROM(sysdate - MAX(date_time))),99999999) AS diff_hours
                FROM
                    siga.mv_jsl_fv_cliente  a
                    LEFT OUTER JOIN siga.geo_clientes g ON a.codcliente = g.codcliente
                    left outer join siga.MONITORAMENTO_API_GEOCLIENTE m on mes = to_char(sysdate, 'yyyy-mm') 
                WHERE
                       nvl(m.contagem_mes,0) < 90000
                       and nvl(m.limite_alcancado,'N') = 'N'
                       and a1_ystatus in ('A','B','O')  
                GROUP BY
                    a.codcliente,
                    a1_nome,
                    a1_cgc,
                    a1_end,
                    a1_mun,
                    a1_est,
                    a1_bairro,
                    CASE
                        WHEN g.codcliente IS NULL THEN
                                0
                        ELSE
                            1
                    END
            )
            where   ( flag_exist = 0  or (flag_exist = 1  and diff_hours >= 1000  ) )
        ORDER BY
            flag_exist,
            diff_hours desc,
            date_time desc ,
            codcliente ASC 
        """

    execution_status, result = db_config.execute(sql_query)

    if execution_status:
            for row in result:
                if verificar_limite_alcancado(db_config):
                    print("Limite de requests alcançado durante o processamento. Encerrando a execução do script.")
                    return

                codcliente = row['CODCLIENTE']
                nome = row['A1_NOME']
                cgc = row['A1_CGC']
                endereco = f"{row['A1_END']}, {row['A1_MUN']} {row['A1_EST']}, {row['A1_BAIRRO']}"
                endereco = ' '.join(endereco.split())

                try:
                    print("Nome:", nome)
                    print("CGC:", cgc)
                    print("Código do Cliente:", codcliente)
                    print("Endereço:", endereco)

                    latitude, longitude = obter_coordenadas_endereco(db_config, endereco)
                    if latitude is not None and longitude is not None:
                        preencher_tabela_geo_clientes(db_config, codcliente, nome, cgc, latitude, longitude)
                    else:
                        preencher_tabela_geo_clientes(db_config, codcliente, nome, cgc)
                except Exception as e:
                    print(f"Erro ao processar o cliente {codcliente}: {str(e)}")
                    preencher_tabela_geo_clientes(db_config, codcliente, nome, cgc)
                    continue
    else:
            print("Nenhum resultado retornado pela consulta SQL.")


if __name__ == '__main__':
    main()
