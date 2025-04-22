from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from include.firebird_to_postgres.extract_load import ExtractLoadProcess


@pytest.fixture
def extract_load():
    return ExtractLoadProcess(write_mode='append')


def test_extract_from_source(extract_load):
    mock_engine = MagicMock()
    mock_conn = mock_engine.connect.return_value.__enter__.return_value
    mock_df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})

    with patch('pandas.read_sql', return_value=mock_df) as mock_read_sql:
        result_df = extract_load.extract_from_source(
            mock_engine, 'SELECT * FROM table'
        )
        mock_read_sql.assert_called_once()
        assert result_df.equals(mock_df)


def test_extract_from_source_failure(extract_load):
    mock_engine = MagicMock()
    mock_engine.connect.side_effect = SQLAlchemyError('Erro na conexão')

    with pytest.raises(
        ConnectionError,
        match='Erro ao ler dados do banco de origem: Erro na conexão',
    ):
        extract_load.extract_from_source(mock_engine, 'SELECT * FROM table')


def test_change_data_capture(extract_load):
    hoje = datetime.today().date()
    ontem = hoje - timedelta(days=1)
    df = pd.DataFrame(
        {
            'id': [1, 2, 3, 4],
            'datatlz': [
                pd.Timestamp(ontem),
                pd.Timestamp(hoje),
                pd.Timestamp('2023-01-01'),
                pd.Timestamp(hoje),
            ],
        }
    )

    result_df = extract_load.change_data_capture(df, 'datatlz')

    assert len(result_df) == 3
    assert all(result_df['datatlz'].dt.date >= ontem)
    assert all(result_df['datatlz'].dt.date <= hoje)


def test_load_to_destination(extract_load):
    mock_engine = MagicMock()
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})

    with patch('pandas.DataFrame.to_sql') as mock_to_sql:
        extract_load.load_to_destination(
            mock_engine, df, 'myschema', 'mytable'
        )
        mock_to_sql.assert_called_once_with(
            name='mytable',
            con=mock_conn,
            if_exists='append',
            index=False,
            schema='myschema',
        )


def test_load_to_destination_failure(extract_load):
    mock_engine = MagicMock()
    df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})

    with patch(
        'pandas.DataFrame.to_sql',
        side_effect=SQLAlchemyError('Erro ao inserir dados'),
    ):
        with pytest.raises(
            ConnectionError,
            match='Erro ao gravar dados no banco destino: Erro ao inserir dados',
        ):
            extract_load.load_to_destination(
                mock_engine, df, 'myschema', 'mytable'
            )
