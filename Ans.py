import os
import time
import requests
import zipfile
import logging
from pathlib import Path
import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Configurações
BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
OUTPUT_DIR = BASE_DIR / "output"
DOWNLOADS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# URLs e padrões
SITE_URL = "https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos"
ANEXO_I_PATTERN = "Anexo_I_Rol"
ANEXO_II_PATTERN = "Anexo_II_DUT"
ANEXO_I_NAME = "Anexo_I.pdf"
ANEXO_II_NAME = "Anexo_II.pdf"
ANEXOS_ZIP = "Anexos_ANS.zip"
OUTPUT_CSV = "Rol_Procedimentos.csv"
OUTPUT_ZIP = "Teste_Alexandre.zip"
DB_URL = f"sqlite:///{OUTPUT_DIR}/ans_rol.db"

# Mapeamento de abreviações
ABBREVIATIONS = {
    "OD": "Seg. Odontológica",
    "AMB": "Seg. Ambulatorial",
    "HCO": "Seg. Hospitalar Com Obstetrícia",
    "HSO": "Seg. Hospitalar Sem Obstetrícia",
    "REF": "Plano Referência",
    "PAC": "Procedimento de Alta Complexidade",
    "DUT": "Diretrizes de Utilização"
}

# Configuração de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(BASE_DIR / "app.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


# =================== FUNÇÕES DE WEB SCRAPING ===================

def setup_driver():
    """Configura e retorna o driver do Selenium"""
    chrome_options = Options()
    # Descomente para execução sem interface
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    return driver


def download_file(url, output_path):
    """Faz o download de um arquivo da URL para o caminho especificado"""
    try:
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Arquivo baixado com sucesso: {output_path}")
            return True
        else:
            logger.error(f"Erro ao baixar o arquivo: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Exceção ao baixar arquivo: {str(e)}")
        return False


def find_and_download_anexos():
    """Encontra e baixa os anexos I e II do site da ANS"""
    driver = setup_driver()

    try:
        # Acessa o site
        logger.info(f"Acessando o site: {SITE_URL}")
        driver.get(SITE_URL)
        time.sleep(5)  # Aguarda o carregamento completo da página

        # Encontra todos os links da página
        links = driver.find_elements(By.TAG_NAME, "a")

        # URLs dos anexos
        anexo_i_url = None
        anexo_ii_url = None

        logger.info(f"Buscando links dos anexos entre {len(links)} links encontrados")

        # Busca pelos links dos anexos
        for link in links:
            try:
                href = link.get_attribute("href")
                if href:
                    # Identifica o anexo I (PDF)
                    if ANEXO_I_PATTERN in href and href.endswith(".pdf"):
                        anexo_i_url = href
                        logger.info(f"Anexo I encontrado: {href}")

                    # Identifica o anexo II (PDF)
                    if ANEXO_II_PATTERN in href and href.endswith(".pdf"):
                        anexo_ii_url = href
                        logger.info(f"Anexo II encontrado: {href}")
            except Exception as e:
                logger.warning(f"Erro ao processar link: {str(e)}")

        if not anexo_i_url or not anexo_ii_url:
            logger.error("Não foi possível encontrar os links dos anexos")
            return None, None

        # Faz o download dos anexos
        anexo_i_path = DOWNLOADS_DIR / ANEXO_I_NAME
        anexo_ii_path = DOWNLOADS_DIR / ANEXO_II_NAME

        logger.info(f"Baixando Anexo I: {anexo_i_url}")
        download_file(anexo_i_url, anexo_i_path)

        logger.info(f"Baixando Anexo II: {anexo_ii_url}")
        download_file(anexo_ii_url, anexo_ii_path)

        return anexo_i_path, anexo_ii_path

    except Exception as e:
        logger.error(f"Erro no processo de scraping: {str(e)}")
        return None, None
    finally:
        driver.quit()


def compress_files(file_paths, output_zip=None):
    """Compacta uma lista de arquivos em um único arquivo ZIP"""
    if output_zip is None:
        output_zip = OUTPUT_DIR / ANEXOS_ZIP

    try:
        with zipfile.ZipFile(output_zip, 'w') as zipf:
            for file_path in file_paths:
                file_path = Path(file_path)
                zipf.write(file_path, file_path.name)

        logger.info(f"Arquivos compactados em: {output_zip}")
        return output_zip
    except Exception as e:
        logger.error(f"Erro ao compactar arquivos: {str(e)}")
        return None


# =================== FUNÇÕES DE PROCESSAMENTO PDF ===================

def extract_pdf_data(pdf_path):
    """
    Extrai dados do PDF utilizando Docling
    """
    try:
        from docling.document_converter import DocumentConverter
        from docling_core.types.doc import TableItem

        logger.info(f"Processando o PDF: {pdf_path}")

        # Converter o PDF usando Docling
        converter = DocumentConverter()
        result = converter.convert(pdf_path)

        # Lista para armazenar todas as tabelas encontradas
        all_tables = []

        logger.info("Extraindo tabelas do documento...")

        # Itera por todos os itens no documento
        for item, _ in result.document.iterate_items():
            if isinstance(item, TableItem):
                # Converte a tabela para DataFrame
                try:
                    table_df = item.export_to_dataframe()
                    all_tables.append(table_df)
                except Exception as e:
                    logger.warning(f"Erro ao converter tabela para DataFrame: {str(e)}")

        logger.info(f"Total de {len(all_tables)} tabelas encontradas no documento")

        # Identifica e processa tabelas do Rol
        rol_tables = []
        for i, table in enumerate(all_tables):
            # Identifica se é uma tabela do Rol de Procedimentos
            if len(table.columns) >= 8:
                # Converte cabeçalhos para string para busca
                headers = [str(col).upper() for col in table.columns]

                # Verifica cabeçalhos relevantes
                has_relevant_headers = any('PROCEDIMENTO' in h for h in headers) or \
                                       any('GRUPO' in h for h in headers)

                if has_relevant_headers:
                    rol_tables.append(table)
                    logger.info(f"Tabela {i} identificada como relevante: {len(table)} linhas")

        # Unifica as tabelas
        if rol_tables:
            combined_df = pd.concat(rol_tables, ignore_index=True)

            # Limpa e padroniza
            df = combined_df.dropna(how='all')

            # Renomeia as colunas com base em identificação de conteúdo
            column_names = identify_columns(df)
            if column_names:
                df.rename(columns=column_names, inplace=True)

            # Substitui abreviações pelas descrições completas
            for col, full_name in ABBREVIATIONS.items():
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: full_name if pd.notna(x) and x == col else x)

            return df
        else:
            logger.warning("Nenhuma tabela do Rol encontrada no PDF")
            return None

    except Exception as e:
        logger.error(f"Erro ao processar PDF: {str(e)}")
        return None


def identify_columns(df):
    """Identifica as colunas da tabela com base no conteúdo"""
    mapping = {}

    for i, col in enumerate(df.columns):
        col_content = df[col].dropna().astype(str).str.upper()

        if any(col_content.str.contains('PROCED')):
            mapping[col] = 'PROCEDIMENTO'
        elif any(col_content.str.contains('RN')):
            mapping[col] = 'RN'
        elif any(col_content.str.contains('VIG')):
            mapping[col] = 'VIGÊNCIA'
        elif any(col_content.str.contains('AMB')):
            mapping[col] = 'AMB'
        elif any(col_content.str.contains('OD')):
            mapping[col] = 'OD'
        elif any(col_content.str.contains('HCO')):
            mapping[col] = 'HCO'
        elif any(col_content.str.contains('HSO')):
            mapping[col] = 'HSO'
        elif any(col_content.str.contains('REF')):
            mapping[col] = 'REF'
        elif any(col_content.str.contains('PAC')):
            mapping[col] = 'PAC'
        elif any(col_content.str.contains('DUT')):
            mapping[col] = 'DUT'
        elif any(col_content.str.contains('GRUP')):
            mapping[col] = 'GRUPO'
        elif any(col_content.str.contains('SUBGRUP')):
            mapping[col] = 'SUBGRUPO'
        elif any(col_content.str.contains('CAP')):
            mapping[col] = 'CAPÍTULO'

    return mapping


def save_to_csv(df, csv_path=None):
    """Salva o DataFrame em um arquivo CSV"""
    if csv_path is None:
        csv_path = OUTPUT_DIR / OUTPUT_CSV

    try:
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"Dados salvos em: {csv_path}")
        return csv_path
    except Exception as e:
        logger.error(f"Erro ao salvar CSV: {str(e)}")
        return None


def create_output_zip(csv_path=None, zip_path=None):
    """Cria um arquivo ZIP contendo o CSV"""
    if csv_path is None:
        csv_path = OUTPUT_DIR / OUTPUT_CSV

    if zip_path is None:
        zip_path = OUTPUT_DIR / OUTPUT_ZIP

    try:
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            zipf.write(csv_path, Path(csv_path).name)

        logger.info(f"CSV compactado em: {zip_path}")
        return zip_path
    except Exception as e:
        logger.error(f"Erro ao compactar CSV: {str(e)}")
        return None


# =================== FUNÇÕES DO BANCO DE DADOS ===================

Base = declarative_base()


class RolProcedimento(Base):
    """Modelo para a tabela de procedimentos do Rol"""
    __tablename__ = 'rol_procedimentos'

    id = Column(Integer, primary_key=True, autoincrement=True)
    procedimento = Column(String(500))
    rn = Column(String(100))
    vigencia = Column(String(100))
    od = Column(String(100))
    amb = Column(String(100))
    hco = Column(String(100))
    hso = Column(String(100))
    ref = Column(String(100))
    pac = Column(String(100))
    dut = Column(String(100))
    subgrupo = Column(String(200))
    grupo = Column(String(200))
    capitulo = Column(String(200))


def setup_database():
    """Configura a conexão com o banco de dados e cria as tabelas"""
    try:
        engine = create_engine(DB_URL)
        Base.metadata.create_all(engine)
        logger.info(f"Banco de dados configurado com sucesso: {DB_URL}")
        return engine
    except Exception as e:
        logger.error(f"Erro ao configurar banco de dados: {str(e)}")
        return None


def save_to_database(df):
    """Salva os dados do DataFrame no banco de dados"""
    engine = setup_database()
    if not engine:
        logger.error("Não foi possível configurar o banco de dados")
        return False

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        total_rows = len(df)
        logger.info(f"Iniciando inserção de {total_rows} registros no banco de dados")

        inserted_count = 0

        for index, row in df.iterrows():
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

            session.add(rol)

            inserted_count += 1
            if inserted_count % 100 == 0:
                session.commit()
                logger.info(f"Inseridos {inserted_count}/{total_rows} registros")

        session.commit()
        logger.info(f"Inserção concluída: {inserted_count} registros inseridos no banco de dados")
        return True

    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao inserir dados no banco de dados: {str(e)}")
        return False

    finally:
        session.close()


# =================== FUNÇÃO PRINCIPAL ===================

def main():
    """Função principal do programa"""
    logger.info("Iniciando o processo de extração e processamento dos dados da ANS")

    try:
        # ETAPA 1: WEB SCRAPING
        logger.info("ETAPA 1: WEB SCRAPING")

        # 1.1 Baixar os anexos do site da ANS
        anexo_i_path, anexo_ii_path = find_and_download_anexos()

        if not anexo_i_path or not anexo_ii_path:
            logger.error("Não foi possível baixar os anexos. Abortando.")
            return False

        # 1.2 Compactar os anexos em um único arquivo
        anexos_zip = compress_files([anexo_i_path, anexo_ii_path])
        if not anexos_zip:
            logger.error("Não foi possível compactar os anexos. Continuando com a próxima etapa...")

        # ETAPA 2: TRANSFORMAÇÃO DE DADOS
        logger.info("ETAPA 2: TRANSFORMAÇÃO DE DADOS")

        # 2.1 Extrair tabela do PDF
        rol_df = extract_pdf_data(anexo_i_path)

        if rol_df is None:
            logger.error("Não foi possível processar o PDF. Abortando.")
            return False

        # 2.2 Salvar em CSV
        csv_path = save_to_csv(rol_df)

        # 2.3 Compactar o CSV
        zip_path = create_output_zip(csv_path)

        # 2.4 Salvar no banco de dados
        logger.info("Salvando dados no banco de dados...")
        db_result = save_to_database(rol_df)

        if not db_result:
            logger.warning("Não foi possível salvar os dados no banco de dados.")

        logger.info("Processo concluído com sucesso!")
        return True

    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)