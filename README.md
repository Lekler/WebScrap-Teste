# ANS Rol de Procedimentos - Web Scraping e Processamento de PDF

Este projeto realiza web scraping do site da ANS (Agência Nacional de Saúde Suplementar) para baixar os Anexos I e II do Rol de Procedimentos e Eventos em Saúde, processa o conteúdo dos PDFs para extrair dados tabulares, e salva os resultados em formato estruturado.

## Funcionalidades

1. **Web Scraping**:
   - Acessa o site da ANS: [Atualização do Rol de Procedimentos](https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos)
   - Baixa os Anexos I e II em formato PDF
   - Compacta os anexos em um único arquivo ZIP

2. **Processamento de PDF**:
   - Utiliza Docling para processar o PDF do Anexo I
   - Extrai as tabelas do Rol de Procedimentos (todas as 181 páginas)
   - Identifica e estrutura os dados tabulares

3. **Transformação e Armazenamento de Dados**:
   - Substitui as abreviações das colunas (OD, AMB, etc.) pelas descrições completas
   - Salva os dados em formato CSV
   - Armazena os dados em um banco de dados SQLite
   - Compacta o CSV em um arquivo ZIP

## Requisitos

- Python 3.8+
- Bibliotecas listadas em `requirements.txt`
