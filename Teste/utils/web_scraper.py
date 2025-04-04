import time
import requests
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import logging
import zipfile

from Teste.config.settings import SITE_URL, DOWNLOADS_DIR, ANEXO_I_PATTERN, ANEXO_II_PATTERN, ANEXO_I_NAME, ANEXO_II_NAME, \
    ANEXOS_ZIP, OUTPUT_DIR

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def setup_driver():
    """Configura e retorna o driver do Selenium"""
    chrome_options = Options()
    # Descomente a linha abaixo para execução sem interface
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