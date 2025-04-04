import logging
from pathlib import Path
import sys

current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))


from Teste.utils import find_and_download_anexos, compress_files
from Teste.utils import process_anexo_i
from Teste.database.db_manager import save_to_database

# Configuração de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("../app.log"),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)


def main():
    """Função principal do programa"""
    logger.info("Iniciando o processo de extração e processamento dos dados da ANS")

    try:
        # =================== PARTE 1: WEB SCRAPING ===================
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

        # =================== PARTE 2: TRANSFORMAÇÃO DE DADOS ===================
        logger.info("ETAPA 2: TRANSFORMAÇÃO DE DADOS")

        # 2.1 & 2.2 Extrair tabela do PDF e salvar como CSV
        # 2.4 Substituir abreviações por descrições completas
        rol_df, csv_path, zip_path = process_anexo_i(anexo_i_path)

        if not rol_df is not None:
            logger.error("Não foi possível processar o PDF. Abortando.")
            return False

        # 2.3 Salvar os dados no banco de dados
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