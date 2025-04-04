import pandas as pd
import logging
from pathlib import Path
import zipfile

from docling.document_converter import DocumentConverter
from docling_core.types.doc import TableItem, DocItemLabel

from config.settings import OUTPUT_DIR, OUTPUT_CSV, OUTPUT_ZIP, ABBREVIATIONS

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_tables_from_pdf(pdf_path):
    """
    Extrai tabelas de um PDF usando Docling
    Retorna uma lista de DataFrames pandas
    """
    logger.info(f"Processando o PDF: {pdf_path}")

    try:
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
                    logger.debug(f"Tabela encontrada com {len(table_df)} linhas e {len(table_df.columns)} colunas")
                except Exception as e:
                    logger.warning(f"Erro ao converter tabela para DataFrame: {str(e)}")

        logger.info(f"Total de {len(all_tables)} tabelas encontradas no documento")
        return all_tables

    except Exception as e:
        logger.error(f"Erro ao processar o PDF: {str(e)}")
        return []


def identify_rol_tables(tables):
    """
    Identifica quais tabelas contêm dados do Rol de Procedimentos
    Retorna as tabelas relevantes
    """
    relevant_tables = []

    for i, table in enumerate(tables):
        # Verifica se é uma tabela do Rol de Procedimentos
        # Critérios: número de colunas, cabeçalhos específicos

        # Se tiver mais de 8 colunas e algumas colunas chave
        if len(table.columns) >= 8:
            # Converte todos os cabeçalhos para string para busca
            headers = [str(col).upper() for col in table.columns]

            # Verifica se contém cabeçalhos relacionados ao Rol
            has_relevant_headers = any('PROCEDIMENTO' in h for h in headers) or \
                                   any('RN' in h for h in headers) or \
                                   any('GRUPO' in h for h in headers)

            if has_relevant_headers:
                relevant_tables.append(table)
                logger.info(f"Tabela {i} identificada como relevante: {len(table)} linhas")

    return relevant_tables


def process_rol_tables(tables):
    """
    Processa e unifica as tabelas do Rol de Procedimentos
    Retorna um DataFrame limpo e uniformizado
    """
    if not tables:
        logger.warning("Nenhuma tabela relevante encontrada para processar")
        return None

    # Unifica as tabelas
    combined_df = pd.concat(tables, ignore_index=True)

    # Limpa e padroniza os dados
    processed_df = clean_table_data(combined_df)

    return processed_df


def clean_table_data(df):
    """Limpa e padroniza os dados da tabela"""
    # Remove linhas vazias
    df = df.dropna(how='all')

    # Identifica colunas pela posição e conteúdo
    # Isso é uma simplificação - a detecção real precisaria ser mais robusta
    column_mapping = identify_columns(df)

    # Renomeia as colunas
    if column_mapping:
        df = df.rename(columns=column_mapping)

    # Padroniza os nomes das colunas
    std_cols = ['PROCEDIMENTO', 'RN', 'VIGÊNCIA', 'OD', 'AMB', 'HCO', 'HSO',
                'REF', 'PAC', 'DUT', 'SUBGRUPO', 'GRUPO', 'CAPÍTULO']

    # Garante que todas as colunas necessárias existem
    for col in std_cols:
        if col not in df.columns:
            df[col] = None

    # Substitui abreviações pelas descrições completas
    for col, full_name in ABBREVIATIONS.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda x: full_name if pd.notna(x) and x == col else x)

    # Remove linhas duplicadas
    df = df.drop_duplicates()

    return df


def identify_columns(df):
    """
    Tenta identificar as colunas da tabela baseado em seu conteúdo
    Retorna um dicionário de mapeamento das colunas
    """
    mapping = {}

    # Verifica cada coluna para identificar seu conteúdo
    for i, col in enumerate(df.columns):
        col_content = df[col].dropna().astype(str).str.upper()

        # Tenta identificar o tipo de coluna pelo conteúdo
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


def process_anexo_i(pdf_path):
    """
    Processa o PDF do Anexo I, extrai a tabela do Rol de Procedimentos,
    aplica transformações e salva os resultados
    """
    # Extrai todas as tabelas do PDF
    all_tables = extract_tables_from_pdf(pdf_path)

    # Identifica as tabelas relevantes do Rol
    rol_tables = identify_rol_tables(all_tables)

    # Processa e unifica as tabelas
    rol_df = process_rol_tables(rol_tables)

    if rol_df is not None:
        # Salva o DataFrame em CSV
        csv_path = save_to_csv(rol_df)

        # Compacta o CSV
        zip_path = create_output_zip(csv_path)

        return rol_df, csv_path, zip_path
    else:
        logger.error("Não foi possível processar as tabelas do Rol de Procedimentos")
        return None, None, None