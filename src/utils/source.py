from sqlalchemy import Engine, create_engine


def firebird_engine(db_path: str) -> Engine:
    """
    Cria e retorna uma conexão com um banco de dados Firebird usando SQLAlchemy.

    Parâmetros:
    ----------
    db_path : str
        Caminho absoluto para o arquivo do banco de dados Firebird (.fdb).

    Retorno:
    -------
    Engine
        Objeto SQLAlchemy `Engine` que pode ser usado para interagir com o banco Firebird.

    """

    try:
        # Criar engine SQLAlchemy para o Firebird
        engine = create_engine(
            f'firebird+fdb://SYSDBA:masterkey@localhost:3050/{db_path}'
        )
        return engine
    except Exception as e:
        raise ConnectionError(f'Erro ao conectar ao banco Firebird: {e}')
