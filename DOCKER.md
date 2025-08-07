# Execução com Docker

Este documento explica como executar o sistema de processamento de documentos usando Docker, incluindo as diferentes configurações disponíveis.

## 🐳 Configurações de Execução

O sistema oferece diferentes modos de execução através da variável de ambiente `RUN_MODE`:

### 1. Modo Completo (Padrão)
```bash
# Executa todos os serviços: worker, embedding generator, reprocessador
docker run -d --name scriptoryum-worker \
  --env-file .env \
  scriptoryum-worker
```

### 2. Apenas Worker
```bash
# Executa apenas o worker principal (sem reprocessador)
docker run -d --name scriptoryum-worker-only \
  --env-file .env \
  -e RUN_MODE=worker-only \
  scriptoryum-worker
```

### 3. Apenas Reprocessador
```bash
# Executa apenas o reprocessador de documentos falhados
docker run -d --name scriptoryum-reprocessor \
  --env-file .env \
  -e RUN_MODE=reprocessor-only \
  scriptoryum-worker
```

### 4. Apenas Monitor
```bash
# Executa apenas o sistema de monitoramento
docker run -d --name scriptoryum-monitor \
  --env-file .env \
  -e RUN_MODE=monitor-only \
  scriptoryum-worker
```

## 🔧 Build da Imagem

```bash
# Build da imagem
docker build -t scriptoryum-worker .
```

## 📋 Docker Compose

### Usando docker-compose.yml (Completo)
```bash
# Executa todos os serviços
docker-compose up -d
```

### Usando docker-compose.single.yml (Flexível)
```bash
# Modo completo
docker-compose -f docker-compose.single.yml --profile full up -d

# Apenas worker
docker-compose -f docker-compose.single.yml --profile worker-only up -d

# Apenas reprocessador
docker-compose -f docker-compose.single.yml --profile reprocessor-only up -d

# Apenas monitor
docker-compose -f docker-compose.single.yml --profile monitor-only up -d
```

## 🔍 Verificação dos Serviços

### Logs dos Containers
```bash
# Ver logs do container
docker logs scriptoryum-worker

# Seguir logs em tempo real
docker logs -f scriptoryum-worker
```

### Status dos Processos
```bash
# Verificar processos rodando no container
docker exec scriptoryum-worker ps aux
```

## ⚙️ Variáveis de Ambiente

Crie um arquivo `.env` com as seguintes variáveis:

```env
# Banco de Dados
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=scriptoryum
POSTGRES_USER=postgres
POSTGRES_PASSWORD=sua_senha

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=sua_senha_redis

# Cloudflare R2
CLOUDFLARE_ACCOUNT_ID=seu_account_id
CLOUDFLARE_ACCESS_KEY_ID=sua_access_key
CLOUDFLARE_SECRET_ACCESS_KEY=sua_secret_key
CLOUDFLARE_BUCKET_NAME=seu_bucket
CLOUDFLARE_ENDPOINT_URL=https://seu_endpoint.r2.cloudflarestorage.com

# Configuração do Sistema
RUN_MODE=full  # full, worker-only, reprocessor-only, monitor-only
```

## 🚀 Recomendações de Uso

### Para Produção
- **Modo Completo**: Recomendado para a maioria dos casos
- Inclui worker, embedding generator e reprocessador
- Garante processamento completo e recuperação de falhas

### Para Desenvolvimento
- **Worker Only**: Para testes de processamento básico
- **Reprocessor Only**: Para testar recuperação de falhas
- **Monitor Only**: Para análise de métricas

### Para Scaling
- Execute múltiplas instâncias do worker com `RUN_MODE=worker-only`
- Execute uma instância do reprocessador com `RUN_MODE=reprocessor-only`
- Execute uma instância do monitor com `RUN_MODE=monitor-only`

## 🔧 Troubleshooting

### Container não inicia
```bash
# Verificar logs de erro
docker logs scriptoryum-worker

# Executar interativamente para debug
docker run -it --env-file .env scriptoryum-worker /bin/bash
```

### Reprocessador não está rodando
```bash
# Verificar se RUN_MODE está correto
docker exec scriptoryum-worker env | grep RUN_MODE

# Verificar processos
docker exec scriptoryum-worker ps aux | grep python
```

### Problemas de conectividade
```bash
# Testar conexão com banco
docker exec scriptoryum-worker python -c "import psycopg2; print('DB OK')"

# Testar conexão com Redis
docker exec scriptoryum-worker python -c "import redis; print('Redis OK')"
```

## 📊 Monitoramento

Para verificar se o reprocessador está funcionando:

1. **Logs do Container**:
   ```bash
   docker logs scriptoryum-worker | grep -i reprocess
   ```

2. **Métricas do Sistema**:
   - O monitor exibe estatísticas de documentos reprocessáveis
   - Verifique os logs para ver a atividade do reprocessador

3. **Banco de Dados**:
   ```sql
   -- Verificar documentos sendo reprocessados
   SELECT status, retry_count, COUNT(*) 
   FROM documents 
   WHERE status = 'TextExtractionFailed' 
   GROUP BY status, retry_count;
   ```

## 🔄 Atualizações

Para atualizar o sistema:

```bash
# Parar container
docker stop scriptoryum-worker

# Rebuild da imagem
docker build -t scriptoryum-worker .

# Reiniciar com nova imagem
docker run -d --name scriptoryum-worker --env-file .env scriptoryum-worker
```