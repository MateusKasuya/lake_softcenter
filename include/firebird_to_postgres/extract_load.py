import os
import sys

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta

from include.firebird_to_postgres.source import DbEngine


class ExtractLoadProcess(DbEngine):
    """
    Classe responsável por extrair dados de um banco Firebird e carregá-los em um banco PostgreSQL.
    """

    def __init__(self, write_mode: str):
        """
        write_mode : str, opcional
            Modo de escrita no banco de dados. Pode ser "append" para ativar o cdc ou "overwrite" para dar full load.
        """
        self.write_mode = write_mode

    def extract_from_source(self, engine: Engine, query: str) -> pd.DataFrame:
        """
        Extrai dados do banco Firebird e retorna um DataFrame.

        Parâmetros:
        ----------
        engine : sqlalchemy.engine.Engine
            Engine SQLAlchemy para conexão com o banco de dados.
        query : str
            Query SQL a ser executada.

        Retorno:
        -------
        pd.DataFrame
            DataFrame contendo os dados extraídos.
        """
        try:
            with engine.connect() as conn:
                return pd.read_sql(query, conn)
        except SQLAlchemyError as e:
            raise ConnectionError(f'Erro ao ler dados do banco de origem: {e}')

    def change_data_capture(
        self, df: pd.DataFrame, column: str
    ) -> pd.DataFrame:
        """
        Realiza a captura de mudanças nos dados (Change Data Capture - CDC) filtrando registros
        que foram modificados nas últimas 24 horas.

        Parâmetros:
        ----------
        df : pd.DataFrame
            DataFrame contendo os dados originais.
        column : str
            Nome da coluna de data que será utilizada para filtrar as mudanças.

        Retorno:
        -------
        pd.DataFrame
            DataFrame contendo apenas os registros modificados nas últimas 24 horas.

        Observações:
        -----------
        - A coluna informada deve estar no formato datetime (`pd.to_datetime(df[column])`).
        - Apenas registros com datas entre ontem e hoje são mantidos.
        """
        hoje = datetime.today().date()
        ontem = hoje - timedelta(days=1)
        df_cdc = df.loc[
            (df[column].dt.date >= ontem) & (df[column].dt.date <= hoje), :
        ]
        print(f'{df_cdc.shape[0]} novas linhas foram detectadas')

        return df_cdc

    def remove_null_chars(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove caracteres nulos ('\x00') de colunas do tipo string (object) em um DataFrame,
        sem converter valores que não sejam do tipo str.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados a serem tratados.

        Returns:
            pd.DataFrame: DataFrame com os caracteres nulos removidos das colunas do tipo string.
        """
        for col in df.select_dtypes(include='object'):
            df[col] = df[col].apply(
                lambda x: x.replace('\x00', '') if isinstance(x, str) else x
            )

        return df

    def load_to_destination(
        self, engine: Engine, df: pd.DataFrame, schema: str, table: str
    ):
        """
        Carrega um DataFrame para um banco de dados PostgreSQL.

        Parâmetros:
        ----------
        engine : sqlalchemy.engine.Engine
            Engine SQLAlchemy para conexão com o banco de dados.
        df : pd.DataFrame
            DataFrame a ser carregado.
        schema: str
            Schema de destino
        table : str
            Nome da tabela de destino.

        Retorno:
        -------
        None
        """
        try:
            with engine.begin() as conn:
                if self.write_mode == 'replace':
                    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
                    print(f'Dropando tabela {schema}.{table} com CASCADE...')
                    conn.execute(
                        text(
                            f'DROP TABLE IF EXISTS {schema}."{table}" CASCADE'
                        )
                    )
                    print(
                        f'Drop da tabela {schema}.{table} realizado com sucesso'
                    )

                df.to_sql(
                    name=table,
                    con=conn,
                    if_exists=self.write_mode,
                    index=False,
                    schema=schema,
                )

                print(f'Tabela {schema}.{table} carregada com sucesso')

        except SQLAlchemyError as e:
            raise ConnectionError(
                f'Erro ao gravar dados no banco destino: {e}'
            )
