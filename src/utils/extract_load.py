import os
import sys

import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
)

from src.utils.source import firebird_engine


class ExtractLoadProcess:
    def __init__(self, source_db_path: str):
        self.source_engine = firebird_engine(db_path=source_db_path)

    def extract_from_source(self, query: str) -> pd.DataFrame:
        """
        Extrai dados do banco Firebird e retorna um DataFrame.

        :param query: Query SQL a ser executada.
        :return: DataFrame contendo os dados extraídos.
        """
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            raise ConnectionError(f'Erro ao ler dados do banco de origem: {e}')

    def load_to_destination(self):
        pass


if __name__ == '__main__':

    source_engine = ExtractLoadProcess(
        source_db_path='C:/Users/MateusKasuya/documents/softcenter/extract_load/data/fn9.fdb'
    )
    df = source_engine.extract_from_source('SELECT * FROM FRCTRC')
    print(df.head())
