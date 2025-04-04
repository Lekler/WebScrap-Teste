import pandas as pd
from sqlalchemy.orm import sessionmaker
import logging

from Teste.database.models import setup_database, RolProcedimento

logger = logging.getLogger(__name__)


def save_to_database(df):
    """Salva os dados do DataFrame no banco de dados"""
    engine = setup_database()
    if not engine:
        logger.error("Não foi possível configurar o banco de dados")
        return False

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Conta linhas no DataFrame
        total_rows = len(df)
        logger.info(f"Iniciando inserção de {total_rows} registros no banco de dados")

        # Contador para log de progresso
        inserted_count = 0

        # Percorre cada linha do DataFrame
        for index, row in df.iterrows():
            # Cria novo objeto RolProcedimento
            rol = RolProcedimento(
                procedimento=str(row.get('PROCEDIMENTO', '')),
                rn=str(row.get('RN', '')),
                vigencia=str(row.get('VIGÊNCIA', '')),
                od=str(row.get('OD', '')),
                amb=str(row.get('AMB', '')),
                hco=str(row.get('HCO', '')),
                hso=str(row.get('HSO', '')),
                ref=str(row.get('REF', '')),
                pac=str(row.get('PAC', '')),
                dut=str(row.get('DUT', '')),
                subgrupo=str(row.get('SUBGRUPO', '')),
                grupo=str(row.get('GRUPO', '')),
                capitulo=str(row.get('CAPÍTULO', ''))
            )

            # Adiciona à sessão
            session.add(rol)

            # A cada 100 registros, faz commit e log
            inserted_count += 1
            if inserted_count % 100 == 0:
                session.commit()
                logger.info(f"Inseridos {inserted_count}/{total_rows} registros")

        # Commit final
        session.commit()
        logger.info(f"Inserção concluída: {inserted_count} registros inseridos no banco de dados")
        return True

    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao inserir dados no banco de dados: {str(e)}")
        return False

    finally:
        session.close()


def query_database():
    """Recupera todos os registros do banco de dados"""
    engine = setup_database()
    if not engine:
        logger.error("Não foi possível configurar o banco de dados")
        return None

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Recupera todos os registros
        registros = session.query(RolProcedimento).all()
        logger.info(f"Recuperados {len(registros)} registros do banco de dados")

        # Converte para um DataFrame
        dados = []
        for r in registros:
            dados.append({
                'id': r.id,
                'procedimento': r.procedimento,
                'rn': r.rn,
                'vigencia': r.vigencia,
                'od': r.od,
                'amb': r.amb,
                'hco': r.hco,
                'hso': r.hso,
                'ref': r.ref,
                'pac': r.pac,
                'dut': r.dut,
                'subgrupo': r.subgrupo,
                'grupo': r.grupo,
                'capitulo': r.capitulo
            })

        return pd.DataFrame(dados)

    except Exception as e:
        logger.error(f"Erro ao consultar banco de dados: {str(e)}")
        return None

    finally:
        session.close()