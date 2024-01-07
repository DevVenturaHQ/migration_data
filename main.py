import logging
from sqlalchemy import create_engine, MetaData, Table

def migrate_data(source_config, dest_config, table_mappings):
    try:
        # Conectar-se à base de dados de origem
        source_engine = create_engine(source_config['db_url'])
        source_conn = source_engine.connect()

        # Conectar-se à base de dados de destino
        dest_engine = create_engine(dest_config['db_url'])
        dest_conn = dest_engine.connect()

        for source_table, dest_table in table_mappings.items():
            migrate_table_data(source_conn, dest_conn, source_table, dest_table)

        # Fechar as conexões
        source_conn.close()
        dest_conn.close()

        logging.info("Migração de dados concluída com sucesso.")

    except Exception as e:
        logging.error(f"Erro durante a migração de dados: {e}")

def migrate_table_data(source_conn, dest_conn, source_table_name, dest_table_name):
    try:
        # Obter metadados da tabela de origem
        source_metadata = MetaData()
        source_table = Table(source_table_name, source_metadata, autoload_with=source_conn)

        # Obter metadados da tabela de destino
        dest_metadata = MetaData()
        dest_table = Table(dest_table_name, dest_metadata, autoload_with=dest_conn)

        # Selecionar dados da tabela de origem
        select_query = source_table.select()
        result = source_conn.execute(select_query)
        data_to_migrate = result.fetchall()

        # Inserir dados na tabela de destino
        dest_conn.execute(dest_table.insert(), data_to_migrate)

        logging.info(f"Migração da tabela {source_table_name} para {dest_table_name} concluída com sucesso.")

    except Exception as e:
        logging.error(f"Erro durante a migração da tabela {source_table_name} para {dest_table_name}: {e}")

if __name__ == "__main__":
    # Configurações da base de dados de origem
    source_config = {
        'db_url': 'mysql://seu_usuario:senha@localhost/banco_origem'
    }

    # Configurações da base de dados de destino
    dest_config = {
        'db_url': 'mysql://seu_usuario:senha@localhost/banco_destino'
    }

    # Mapeamento de tabelas entre a base de dados de origem e a base de dados de destino
    table_mappings = {
        'tabela_origem_1': 'tabela_destino_1',
        'tabela_origem_2': 'tabela_destino_2',
        # Adicione mais tabelas conforme necessário
    }

    # Configurações de logging
    logging.basicConfig(level=logging.INFO)

    # Executar a migração de dados
    migrate_data(source_config, dest_config, table_mappings)
