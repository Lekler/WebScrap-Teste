import os
from pathlib import Path

# Diretórios do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
OUTPUT_DIR = BASE_DIR / "output"

# URLs
BASE_URL = "https://www.gov.br/ans/pt-br/"
SITE_URL = BASE_URL + "acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos"

# Padrões de busca
ANEXO_I_PATTERN = "Anexo_I_Rol"
ANEXO_II_PATTERN = "Anexo_II_DUT"

# Nomes de arquivos
ANEXO_I_NAME = "Anexo_I.pdf"
ANEXO_II_NAME = "Anexo_II.pdf"
ANEXOS_ZIP = "Anexos_ANS.zip"
OUTPUT_CSV = "Rol_Procedimentos.csv"
OUTPUT_ZIP = "Teste_Alexandre.zip"

# Configurações de banco de dados
DB_URL = os.getenv("DB_URL", "sqlite:///" + str(OUTPUT_DIR / "ans_rol.db"))

# Criar diretórios se não existirem
DOWNLOADS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Mapeamento de abreviações para descrições completas
ABBREVIATIONS = {
    "OD": "Seg. Odontológica",
    "AMB": "Seg. Ambulatorial",
    "HCO": "Seg. Hospitalar Com Obstetrícia",
    "HSO": "Seg. Hospitalar Sem Obstetrícia",
    "REF": "Plano Referência",
    "PAC": "Procedimento de Alta Complexidade",
    "DUT": "Diretrizes de Utilização"
}