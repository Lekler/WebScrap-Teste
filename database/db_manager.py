import os
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, Float, MetaData, Table, text

import config


def get_engine():
    """Cria e retorna uma conexão com o banco de dados SQLite"""
    # Usar SQLite para simplicidade
    db_path = os.path.join(config.OUTPUT_FOLDER, f"{config.DB_NAME}.db")
    return create_engine(f'sqlite:///{db_path}')


def create_tables(engine):
    """Cria a tabela para armazenar os dados do Rol de Procedimentos"""
    metadata = MetaData()

    # Definição da tabela
    rol_table = Table(
        'rol_procedimentos', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('procedimento', String),
        Column('rn', String),
        Column('vigencia', String),
        Column('od', String),
        Column('amb', String),
        Column('hco', String),
        Column('hso', String),
        Column('ref', String),
        Column('pac', String),
        Column('dut', String),
        Column('subgrupo', String),
        Column('grupo', String),
        Column('capitulo', String)
    )

    # Cria as tabelas no banco de dados
    metadata.create_all(engine)
    return rol_table


def insert_into_database(df):
    """Insere os dados do DataFrame no banco de dados"""
    engine = get_engine()
    table = create_tables(engine)

    # Prepara os dados para inserção
    data_to_insert = []
    for _, row in df.iterrows():
        record = {}
        for column in table.columns.keys():
            if column != 'id':  # Ignora a coluna ID (autoincremento)
                # Converte o nome da coluna para maiúsculo para comparar com o DataFrame
                df_column = column.upper()
                record[column] = row.get(df_column, None)

        data_to_insert.append(record)

    # Insere os dados no banco de dados
    with engine.connect() as conn:
        for record in data_to_insert:
            insert_stmt = table.insert().values(**record)
            conn.execute(insert_stmt)
        conn.commit()

    print(f"Inseridos {len(data_to_insert)} registros no banco de dados")


def get_data_from_database():
    """Recupera os dados do banco de dados"""
    engine = get_engine()

    query = text("SELECT * FROM rol_procedimentos")
    with engine.connect() as conn:
        result = conn.execute(query)
        data = [dict(row) for row in result]

    return pd.DataFrame(data)