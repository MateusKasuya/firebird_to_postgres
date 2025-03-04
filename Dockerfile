# Use a imagem oficial do Python como base
FROM python:3.12

# Define o diretório de trabalho no contêiner
WORKDIR /src

# Instala dependências básicas do sistema
RUN apt-get update && apt-get install -y \
    firebird-dev firebird3.0-server firebird3.0-utils \
    && rm -rf /var/lib/apt/lists/*  # Remove cache para reduzir o tamanho da imagem

# Instala o Poetry
RUN pip install --no-cache-dir poetry

# Copia os arquivos do projeto para dentro do contêiner
COPY . .

# Instala dependências do Poetry
RUN poetry install --only main --no-root

# Define o comando padrão para rodar a aplicação
CMD ["poetry", "run", "python", "-u", "src/main.py"]
