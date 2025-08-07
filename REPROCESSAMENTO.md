# Sistema de Reprocessamento de Documentos

Este documento explica como funciona o sistema de reprocessamento automático de documentos que falharam na extração de texto.

## Visão Geral

O sistema de reprocessamento foi desenvolvido para identificar e reprocessar automaticamente documentos que falharam na extração de texto (status `TextExtractionFailed`). Ele funciona de forma recorrente, verificando periodicamente por documentos com falha e tentando reprocessá-los.

## Componentes

### 1. Reprocessador Principal (`reprocessor.py`)

O componente principal que:
- Verifica periodicamente documentos com status `TextExtractionFailed`
- Reprocessa documentos um por vez para evitar sobrecarga
- Controla o número máximo de tentativas por documento
- Marca documentos como permanentemente falhados após esgotar tentativas

### 2. Script de Execução Standalone (`run_reprocessor.py`)

Script para executar o reprocessador de forma independente com opções configuráveis.

### 3. Integração com Monitor (`monitor.py`)

O monitor foi atualizado para incluir estatísticas sobre:
- Documentos reprocessáveis
- Distribuição de falhas por número de tentativas
- Métricas detalhadas de reprocessamento

## Configuração do Banco de Dados

Antes de usar o sistema, execute o script SQL para adicionar as colunas necessárias:

```sql
-- Execute este comando no seu banco PostgreSQL
\i add_reprocessing_columns.sql
```

Ou execute manualmente:

```sql
-- Adiciona coluna para contar tentativas
ALTER TABLE public.documents 
ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;

-- Adiciona coluna para notas de processamento
ALTER TABLE public.documents 
ADD COLUMN IF NOT EXISTS processing_notes TEXT DEFAULT '';
```

## Como Usar

### Execução Automática (Recomendado)

O reprocessador é iniciado automaticamente quando você executa:

```bash
python run_all.py
```

Ele será executado junto com os outros componentes do sistema.

### Execução Manual

#### Modo Contínuo
```bash
# Executa continuamente com configurações padrão
python run_reprocessor.py

# Executa com intervalo personalizado (60 minutos)
python run_reprocessor.py --interval 60

# Executa com máximo de 5 tentativas por documento
python run_reprocessor.py --max-retries 5
```

#### Execução Única
```bash
# Executa apenas um ciclo de reprocessamento
python run_reprocessor.py --run-once

# Execução única com logs detalhados
python run_reprocessor.py --run-once --verbose
```

## Configurações

### Parâmetros Principais

- **Intervalo de Verificação**: 30 minutos (padrão)
- **Máximo de Tentativas**: 3 tentativas por documento (padrão)
- **Tempo de Espera**: 10 minutos após falha antes de tentar reprocessar
- **Limite de Documentos**: 10 documentos por ciclo

### Variáveis de Ambiente Necessárias

Certifique-se de que estas variáveis estão definidas no arquivo `.env`:

```env
DB_HOST=localhost
DB_NAME=seu_banco
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_PORT=5432
REDIS_CONNECTION_STRING=redis://localhost:6379
REDIS_QUEUE=document_processing_queue
```

## Funcionamento Detalhado

### Fluxo de Reprocessamento

1. **Identificação**: O sistema busca documentos com:
   - Status = `TextExtractionFailed`
   - Última atualização há mais de 10 minutos
   - Número de tentativas menor que o máximo configurado

2. **Reprocessamento**: Para cada documento encontrado:
   - Incrementa o contador de tentativas
   - Tenta reprocessar usando a mesma lógica do worker principal
   - Atualiza o status baseado no resultado

3. **Controle de Tentativas**: 
   - Se bem-sucedido: marca como `Processed`
   - Se falhar e ainda há tentativas: mantém como `TextExtractionFailed`
   - Se esgotar tentativas: marca como `Failed` permanentemente

### Estados dos Documentos

- `TextExtractionFailed`: Falha temporária, pode ser reprocessado
- `Failed`: Falha permanente após esgotar tentativas
- `Processed`: Reprocessamento bem-sucedido

## Monitoramento

### Logs

O sistema gera logs detalhados sobre:
- Documentos encontrados para reprocessamento
- Progresso de cada tentativa
- Estatísticas de cada ciclo
- Erros e falhas

### Métricas no Monitor

O monitor exibe:
- Total de documentos reprocessáveis
- Distribuição de falhas por número de tentativas
- Estatísticas de reprocessamento

Exemplo de log do monitor:
```
Métricas de processamento: Total de documentos: 150, Processados: 120, Falhas: 25, Reprocessáveis: 15
Detalhes de falhas na extração: 10 docs com 0 tentativas, 3 docs com 1 tentativas, 2 docs com 2 tentativas
```

## Troubleshooting

### Problemas Comuns

1. **Erro de coluna não encontrada**
   - Execute o script `add_reprocessing_columns.sql`

2. **Documentos não sendo reprocessados**
   - Verifique se há documentos com status `TextExtractionFailed`
   - Confirme que não esgotaram o número máximo de tentativas
   - Verifique os logs para erros de conexão

3. **Reprocessamento muito lento**
   - Ajuste o intervalo de verificação
   - Aumente o limite de documentos por ciclo (modificar código)

### Verificação Manual

```sql
-- Verifica documentos com falha
SELECT status, retry_count, COUNT(*) 
FROM public.documents 
WHERE status IN ('TextExtractionFailed', 'Failed')
GROUP BY status, retry_count;

-- Verifica documentos reprocessáveis
SELECT COUNT(*) as reprocessable_count
FROM public.documents 
WHERE status = 'TextExtractionFailed' 
  AND COALESCE(retry_count, 0) < 3;
```

## Personalização

### Modificar Configurações

Para alterar as configurações padrão, edite o arquivo `reprocessor.py`:

```python
# Alterar intervalo padrão (em minutos)
check_interval_minutes=60  # ao invés de 30

# Alterar máximo de tentativas
max_retry_attempts=5  # ao invés de 3

# Alterar limite de documentos por ciclo
limit=20  # na função get_failed_documents()
```

### Adicionar Notificações

Você pode estender o sistema para enviar notificações quando:
- Documentos são marcados como permanentemente falhados
- Muitos documentos estão falhando
- Sistema de reprocessamento encontra erros

## Segurança

- O sistema processa apenas um documento por vez para evitar sobrecarga
- Há limites no número de tentativas para evitar loops infinitos
- Documentos permanentemente falhados não são mais reprocessados
- Logs detalhados para auditoria e debugging