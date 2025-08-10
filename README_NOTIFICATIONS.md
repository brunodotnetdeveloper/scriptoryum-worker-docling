# Sistema de Notificações - Worker

Este documento descreve as funcionalidades de notificação implementadas no worker de processamento de documentos.

## Funcionalidades Implementadas

### 1. Notificações de Status de Processamento

O worker agora envia notificações automáticas para os usuários durante o processamento de documentos:

- **Início do processamento**: Notifica quando o processamento de um documento é iniciado
- **Processamento concluído**: Notifica quando um documento é processado com sucesso
- **Erros de processamento**: Notifica sobre falhas durante o processamento
- **Documentos presos**: Notifica quando documentos ficam presos no status de processamento por muito tempo

### 2. Tipos de Notificação

- `info`: Informações gerais (início do processamento)
- `success`: Processamento concluído com sucesso
- `error`: Erros e falhas no processamento

### 3. Configuração

Adicione as seguintes variáveis ao arquivo `.env`:

```env
# Configurações da API principal (para notificações)
MAIN_API_URL=http://localhost:5220
MAIN_API_TOKEN=your_api_token_here
```

### 4. Dependências Adicionadas

- `httpx`: Para fazer requisições HTTP à API de notificações
- `asyncio`: Para executar chamadas assíncronas de notificação

### 5. Métodos Implementados

#### `create_notification(user_id, title, message, type)`
Cria uma notificação via API principal.

#### `get_document_user_id(document_id)`
Obtém o ID do usuário proprietário de um documento.

### 6. Pontos de Integração

As notificações são enviadas nos seguintes momentos:

1. **Início do processamento** (`process_document`)
2. **Falha no download** (`process_document`)
3. **Arquivo muito grande** (`process_document`)
4. **Falha na extração de texto** (`process_document`)
5. **Processamento concluído** (`process_document`)
6. **Documentos presos por timeout** (`check_stuck_documents`)
7. **Erros gerais no processamento** (`run`)

### 7. Tratamento de Erros

- Todas as chamadas de notificação incluem tratamento de erro
- Falhas na criação de notificações não interrompem o processamento
- Logs detalhados para debugging

### 8. Exemplo de Uso

O worker automaticamente enviará notificações quando configurado corretamente. Não é necessária intervenção manual.

### 9. Logs

O sistema registra todas as tentativas de criação de notificação:

```
INFO - Notificação criada com sucesso para usuário {user_id}
ERROR - Erro ao criar notificação: {error_message}
```

## Instalação

1. Instale as dependências:
```bash
pip install -r requirements.txt
```

2. Configure as variáveis de ambiente no arquivo `.env`

3. Execute o worker:
```bash
python worker.py
```

O sistema de notificações será ativado automaticamente durante o processamento de documentos.