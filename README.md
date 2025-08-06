# Scriptoryum Document Processing Worker

Este worker é responsável por processar documentos enviados para o sistema Scriptoryum, extraindo texto usando a biblioteca Docling e armazenando os resultados no banco de dados PostgreSQL.

## Funcionalidades

- Leitura de documentos da fila Redis
- Download de documentos do Cloudflare R2
- Extração de texto usando Docling
- Atualização do status e conteúdo no banco de dados PostgreSQL
- Gerenciamento de erros e logging

## Requisitos

- Python 3.10+
- Docker e Docker Compose (para execução em container)
- Acesso ao Redis, PostgreSQL e Cloudflare R2

## Configuração

As configurações são feitas através do arquivo `.env` com as seguintes variáveis:

```
# Database Connection
DB_HOST=host_do_postgres
DB_NAME=nome_do_banco
DB_USER=usuario_do_banco
DB_PASSWORD=senha_do_banco
DB_PORT=porta_do_banco

# Redis Connection
REDIS_CONNECTION_STRING=url_do_redis
REDIS_QUEUE=nome_da_fila

# Cloudflare R2 Settings
CLOUDFLARE_BUCKET_NAME=nome_do_bucket
CLOUDFLARE_ACCESS_KEY=chave_de_acesso
CLOUDFLARE_SECRET_KEY=chave_secreta
CLOUDFLARE_SERVICE_URL=url_do_servico
```

## Execução

### Usando Docker Compose

```bash
docker-compose up --build
```

### Execução local (sem Docker)

1. Instale as dependências:

```bash
pip install -r requirements.txt
```

2. Execute o worker:

```bash
python worker.py
```

## Fluxo de Processamento

1. O worker lê documentos da fila Redis
2. Atualiza o status do documento para "ExtractingText"
3. Baixa o documento do Cloudflare R2
4. Extrai o texto usando Docling
5. Atualiza o documento no banco de dados com o texto extraído
6. Atualiza o status para "Processed" ou para um status de erro apropriado

## Tratamento de Erros

O worker possui tratamento de erros para lidar com falhas em diferentes etapas do processamento:

- Falha na conexão com Redis, PostgreSQL ou Cloudflare R2
- Falha no download do documento
- Falha na extração de texto
- Falha na atualização do banco de dados

Todos os erros são registrados no log para facilitar a depuração.