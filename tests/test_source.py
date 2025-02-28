from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from src.utils.source import firebird_engine


@pytest.fixture
def mock_engine():
    """Mocka a engine do SQLAlchemy para Firebird."""
    with patch('src.utils.source.create_engine') as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        yield mock_engine


def test_firebird_connection(mock_engine):
    """
    Testa se a função firebird_engine retorna um objeto Engine corretamente.
    """
    db_path = 'mock_db.fdb'

    # Executa a função
    engine = firebird_engine(db_path)

    # Verifica se a engine foi chamada corretamente
    assert engine == mock_engine


def test_firebird_connection_execution(mock_engine):
    """
    Testa se uma query pode ser executada na conexão mockada.
    """
    db_path = 'mock_db.fdb'
    engine = firebird_engine(db_path)

    # Mockando uma conexão
    mock_connection = mock_engine.connect.return_value.__enter__.return_value
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [('Cliente 1',), ('Cliente 2',)]
    mock_connection.execute.return_value = mock_result

    # Executando a query mockada
    result = mock_connection.execute('SELECT * FROM TBCLI')
    rows = result.fetchall()

    # Verificações
    assert isinstance(rows, list)
    assert len(rows) == 2
    assert rows[0] == ('Cliente 1',)


def test_invalid_firebird_connection():
    """
    Testa se uma conexão inválida gera erro corretamente.
    """
    with patch(
        'src.utils.source.create_engine',
        side_effect=OperationalError('Erro de conexão', None, None),
    ):
        with pytest.raises(ConnectionError):
            firebird_engine('invalid_path.fdb')
