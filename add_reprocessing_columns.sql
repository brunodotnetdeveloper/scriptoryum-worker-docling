-- Script para adicionar colunas necessárias para o sistema de reprocessamento
-- Execute este script no banco de dados antes de usar o reprocessador

-- Adiciona coluna para contar tentativas de reprocessamento
ALTER TABLE public.documents 
ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;

-- Adiciona coluna para notas de processamento
ALTER TABLE public.documents 
ADD COLUMN IF NOT EXISTS processing_notes TEXT DEFAULT '';

-- Cria índice para otimizar consultas de documentos com falha
CREATE INDEX IF NOT EXISTS idx_documents_status_retry 
ON public.documents (status, retry_count, updated_at) 
WHERE status IN ('TextExtractionFailed', 'Failed');

-- Cria índice para otimizar consultas por status e data de atualização
CREATE INDEX IF NOT EXISTS idx_documents_status_updated_at 
ON public.documents (status, updated_at);

-- Comentários sobre as novas colunas
COMMENT ON COLUMN public.documents.retry_count IS 'Número de tentativas de reprocessamento do documento';
COMMENT ON COLUMN public.documents.processing_notes IS 'Notas sobre o processamento e reprocessamento do documento';

-- Exibe informações sobre as alterações
SELECT 'Colunas adicionadas com sucesso!' as status;
SELECT column_name, data_type, is_nullable, column_default 
FROM information_schema.columns 
WHERE table_name = 'documents' 
  AND table_schema = 'public'
  AND column_name IN ('retry_count', 'processing_notes')
ORDER BY column_name;