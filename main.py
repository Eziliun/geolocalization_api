import os
import requests
import cx_Oracle
from db.db_oracle_config import DbOracleConfig
from dotenv import load_dotenv
from datetime import datetime


def preencher_tabela_geo_clientes(db_config, codcliente, nome, cgc, latitude=None, longitude=None):
    date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if not cliente_existe(db_config, codcliente):
        sql_insert = """
            INSERT INTO GEO_CLIENTES (CODCLIENTE, RAZAO, CNPJ, LATITUDE, LONGITUDE, DATE_TIME)
            VALUES (:1, :2, :3, :4, :5, TO_TIMESTAMP(:6, 'YYYY-MM-DD HH24:MI:SS'))
        """
        execution_status, result = db_config.execute(sql_insert,
                                                     (codcliente, nome, cgc, latitude, longitude, date_time))

        if execution_status:
            print("Dados inseridos com sucesso na tabela GEO_CLIENTES.")
        else:
            print("Erro ao inserir dados na tabela GEO_CLIENTES:", result)
    else:
        print(f"Cliente com CODCLIENTE {codcliente} já existe na tabela GEO_CLIENTES. Pulando inserção.")

    atualizar_contagem_requests(db_config)


def cliente_existe(db_config, codcliente):
    sql_check = "SELECT 1 FROM GEO_CLIENTES WHERE CODCLIENTE = :1"
    execution_status, result = db_config.execute(sql_check, (codcliente,))
    if execution_status:
        return bool(result)
    else:
        print("Erro ao verificar existência do cliente na tabela GEO_CLIENTES:", result)
        return False


def obter_ultimo_codcliente(db_config):
    sql_last_cod = "SELECT MAX(CODCLIENTE) AS ULTIMO_CODCLIENTE FROM GEO_CLIENTES"
    execution_status, result = db_config.execute(sql_last_cod)
    if execution_status and result:
        return result[0]['ULTIMO_CODCLIENTE']
    else:
        print("Erro ao obter o último CODCLIENTE da tabela GEO_CLIENTES:", result)
        return None


def obter_coordenadas_endereco(endereco):
    load_dotenv()
    map_api_key = os.getenv("MAP_API_KEY")
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{endereco}.json?access_token={map_api_key}"

    try:
        response = requests.get(url)
        data = response.json()
        if data['features']:
            coordinates = data['features'][0]['geometry']['coordinates']
            return coordinates[1], coordinates[0]
        else:
            print("Endereço não encontrado:", endereco)
            return None, None
    except Exception as e:
        print("Erro ao obter coordenadas do endereço:", str(e))
        return None, None


def atualizar_contagem_requests(db_config):
    mes = datetime.now().strftime('%Y-%m')
    sql_select = "SELECT contagem_mes FROM MONITORAMENTO_API_GEOCLIENTE WHERE mes = :1"
    execution_status, result = db_config.execute(sql_select, (mes,))

    if execution_status and result:
        sql_update = """
            UPDATE MONITORAMENTO_API_GEOCLIENTE
            SET contagem_mes = contagem_mes + 1,
                ultima_atualizacao = SYSTIMESTAMP,
                limite_alcancado = CASE WHEN contagem_mes + 1 >= 100000 THEN 'S' ELSE 'N' END
            WHERE mes = :1
        """
        db_config.execute(sql_update, (mes,))
    else:
        sql_insert = """
            INSERT INTO MONITORAMENTO_API_GEOCLIENTE (mes, contagem_mes, ultima_atualizacao, limite_alcancado)
            VALUES (:1, 1, SYSTIMESTAMP, 'N')
        """
        db_config.execute(sql_insert, (mes,))

def verificar_limite_alcancado(db_config):
    mes_atual = datetime.now().strftime('%Y-%m')
    sql_select = "SELECT limite_alcancado FROM MONITORAMENTO_API_GEOCLIENTE WHERE mes = :1"
    execution_status, result = db_config.execute(sql_select, (mes_atual,))
    if execution_status and result:
        limite_alcancado = result[0]['LIMITE_ALCANCADO']
        if limite_alcancado == 'S':
            print("Limite de requests já foi alcançado para este mês. Interrompendo o código.")
            return True
    else:
        print("Erro ao verificar o limite alcançado na tabela MONITORAMENTO_API_GEOCLIENTE.")
    return False


def main():
    cx_Oracle.init_oracle_client(lib_dir=r'C:\Oracle\instantclient_21_13')
    db_config = DbOracleConfig()

    if verificar_limite_alcancado(db_config):
        return

    ultimo_codcliente = obter_ultimo_codcliente(db_config)
    if ultimo_codcliente is not None:
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
                    EXTRACT(DAY FROM(sysdate - MAX(date_time))) AS diff_hours
                FROM
                    siga.mv_jsl_fv_cliente  a
                    LEFT OUTER JOIN dev_wilian.geo_clientes g ON a.codcliente = g.codcliente
                WHERE
                        vendmct <> vendmct2 
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
            where   ( flag_exist = 0  or (flag_exist = 1  and diff_hours >= 1  ) )
        ORDER BY
            flag_exist,
            date_time,
            codcliente ASC
        """

        execution_status, result = db_config.execute(sql_query)

        if execution_status:
            for row in result:
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

                    latitude, longitude = obter_coordenadas_endereco(endereco)
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
