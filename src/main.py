import os
import sys
print("OK")
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.extract_load import ExtractLoadProcess


def main():
    load_dotenv()

    source_user = os.getenv('FIREBIRD_USER')
    source_password = os.getenv('FIREBIRD_PASSWORD')
    source_host = os.getenv('FIREBIRD_HOST')
    source_port = os.getenv('FIREBIRD_PORT')
    source_db_path = os.getenv('FIREBIRD_DB_PATH')

    destination_user = os.getenv('POSTGRES_USER')
    destination_password = os.getenv('POSTGRES_PASSWORD')
    destination_host = os.getenv('POSTGRES_HOST')
    destination_port = os.getenv('POSTGRES_PORT')
    destination_database = os.getenv('POSTGRES_DB')

    pipeline = ExtractLoadProcess()

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

    list_tables = [
        'FRCTRC',
        'TBCLI',
        'TBFIL',
        'TBMVP',
        'TBCID',
        'TBPRO',
        'TBPROP',
        'TBVEI',
        'TBMOT',
    ]

    try:
        for table in list_tables:
            print(f'Iniciando pipeline da tabela: {table}')

            source = pipeline.extract_from_source(
                engine=source_engine, query=f'SELECT * FROM {table}'
            )
            print(f'Dados Extraídos com sucesso da source: {table}')

            df_cdc = pipeline.change_data_capture(df=source, column='datatlz')

            if df_cdc.shape[0] > 0:

                pipeline.load_to_destination(
                        engine=destination_engine, df=df_cdc, table=table
                    )

                print(f'{table} ingerida com sucesso')

            else:
                print(f'Não há novos registros, pulando inserção: {table}')

    except Exception as e:
        print(f'Erro durante o pipeline: {e}')

    finally:
        # Fechando as engines para liberar os recursos
        pipeline.close_engine(source_engine)
        pipeline.close_engine(destination_engine)
        print('Conexões fechadas.')


if __name__ == '__main__':
    main()
