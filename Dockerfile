FROM python:3.10-slim

WORKDIR /app

# Instalar dependências do sistema necessárias para o psycopg2 e outras bibliotecas
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar arquivos de requisitos e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código da aplicação
COPY . .

# Comando para executar o worker
CMD ["python", "worker.py"]