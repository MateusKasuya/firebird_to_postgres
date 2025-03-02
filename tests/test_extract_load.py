from unittest import mock

import pandas as pd
import pytest

from src.utils.extract_load import ExtractLoadProcess


@pytest.fixture
def mock_extract_process():
    """Cria uma instância mockada de ExtractLoadProcess."""
    process = ExtractLoadProcess(source_db_path='fake_path')
    process.source_engine = mock.MagicMock()  # Simula a engine do banco
    return process


def test_extract_from_source_success(mock_extract_process):
    """Teste de sucesso: verifica se a extração retorna um DataFrame válido."""

    # Mock do DataFrame de retorno
    expected_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

    # Configurando a conexão e o retorno da query
    with mock.patch('pandas.read_sql', return_value=expected_df):
        df = mock_extract_process.extract_from_source('SELECT * FROM tabela')

    # Verificações
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.equals(expected_df)


def test_extract_from_source_failure(mock_extract_process):
    """Teste de falha: verifica se a exceção é levantada corretamente."""

    # Simula erro ao conectar ao banco
    with mock.patch(
        'pandas.read_sql', side_effect=Exception('Falha na conexão')
    ):
        with pytest.raises(
            ConnectionError,
            match='Erro ao ler dados do banco de origem: Falha na conexão',
        ):
            mock_extract_process.extract_from_source('SELECT * FROM tabela')
