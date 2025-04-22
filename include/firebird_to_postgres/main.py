import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from typing import List

from include.firebird_to_postgres.extract_load import ExtractLoadProcess


def main(list_tables: List[tuple], write_mode: str):
    load_dotenv()

    source_user = os.getenv('FIREBIRD_USER')
    source_password = os.getenv('FIREBIRD_PASSWORD')
    source_host = os.getenv('FIREBIRD_HOST')
    source_port = os.getenv('FIREBIRD_PORT')

    if schema == 'fn9':
        source_db_path = os.getenv('FIREBIRD_DB_PATH_FN9')
    elif schema == 'mgp':
        source_db_path = os.getenv('FIREBIRD_DB_PATH_MGP')

    destination_user = os.getenv('POSTGRES_USER')
    destination_password = os.getenv('POSTGRES_PASSWORD')
    destination_host = os.getenv('POSTGRES_HOST')
    destination_port = os.getenv('POSTGRES_PORT')
    destination_database = os.getenv('POSTGRES_DB')

    pipeline = ExtractLoadProcess(write_mode=write_mode)

    # Criando engines
    source_engine = pipeline.firebird_engine(
        user=source_user,
        password=source_password,
        host=source_host,
        port=source_port,
        db_path=source_db_path,
    )

    destination_engine = pipeline.postgres_engine(
        user=destination_user,
        password=destination_password,
        host=destination_host,
        port=destination_port,
        database=destination_database,
    )

    try:
        for schema, table in list_tables:
            print(f'Iniciando pipeline da tabela: {table}')

            source = pipeline.extract_from_source(
                engine=source_engine, query=f'SELECT * FROM {table}'
            )
            print(f'Dados Extraídos com sucesso da source: {table}')

            if pipeline.write_mode == 'append':

                df_cdc = pipeline.change_data_capture(
                    df=source, column='datatlz'
                )

                if df_cdc.shape[0] > 0:

                    df_cleaned = pipeline.remove_null_chars(df_cdc)

                    print(f'{table} ingerida com sucesso')

                else:
                    print(f'Não há novos registros, pulando inserção: {table}')

            elif pipeline.write_mode == 'replace':

                df_cleaned = pipeline.remove_null_chars(source)

            pipeline.load_to_destination(
                engine=destination_engine,
                df=df_cleaned,
                schema=schema,
                table=table,
            )

    except Exception as e:
        print(f'Erro durante o pipeline: {e}')

    finally:
        # Fechando as engines para liberar os recursos
        pipeline.close_engine(source_engine)
        pipeline.close_engine(destination_engine)
        print('Conexões fechadas.')


if __name__ == '__main__':

    list_schema = ['fn9', 'mgp']

    for schema in list_schema:

        list_schema_tables = [
            (schema, 'FRCTRC'),
            (schema, 'TBCLI'),
            (schema, 'TBFIL'),
            (schema, 'TBCID'),
            (schema, 'TBPRO'),
            (schema, 'FACTRC'),
            (schema, 'CPTIT'),
            (schema, 'TBHIS'),
        ]

        main(list_tables=list_schema_tables, write_mode='replace')
