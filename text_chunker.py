import os
import logging
import psycopg2
import datetime
import traceback
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('text_chunker')

# Carrega variáveis de ambiente
load_dotenv()

class TextChunker:
    def __init__(self, chunk_size=1000, chunk_overlap=200):
        # Conexão com o banco de dados PostgreSQL
        self.db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def get_documents_to_chunk(self, limit=5):
        """Obtém documentos que foram processados mas ainda não foram divididos em chunks."""
        try:
            cursor = self.db_conn.cursor()
            
            query = """
            SELECT d.id, d.text_extracted 
            FROM public.documents d
            LEFT JOIN (
                SELECT document_id, COUNT(*) as chunk_count 
                FROM public.document_chunks 
                GROUP BY document_id
            ) c ON d.id = c.document_id
            WHERE d.status = 'Processed' 
            AND d.text_extracted IS NOT NULL 
            AND (c.chunk_count IS NULL OR c.chunk_count = 0)
            LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            documents = cursor.fetchall()
            cursor.close()
            
            return documents
        except Exception as e:
            logger.error(f"Erro ao obter documentos para chunking: {str(e)}")
            self.db_conn.rollback()
            return []
    
    def create_chunks(self, text, document_id):
        """Divide o texto em chunks com sobreposição e salva no banco de dados."""
        try:
            # Se o texto for muito pequeno, cria apenas um chunk
            if len(text) <= self.chunk_size:
                self.save_chunk(document_id, 0, text)
                return 1
            
            chunks = []
            chunk_index = 0
            start = 0
            
            while start < len(text):
                # Define o fim do chunk atual
                end = min(start + self.chunk_size, len(text))
                
                # Se não estamos no final do texto, tenta encontrar um ponto final ou quebra de linha
                # para fazer um corte mais natural
                if end < len(text):
                    # Procura por um ponto final seguido de espaço ou quebra de linha
                    for i in range(end - 1, max(start, end - 100), -1):
                        if text[i] == '.' and (i + 1 >= len(text) or text[i + 1] == ' ' or text[i + 1] == '\n'):
                            end = i + 1
                            break
                        elif text[i] == '\n':
                            end = i + 1
                            break
                
                # Extrai o chunk atual
                chunk_text = text[start:end].strip()
                
                # Salva o chunk se não estiver vazio
                if chunk_text:
                    self.save_chunk(document_id, chunk_index, chunk_text)
                    chunks.append(chunk_text)
                    chunk_index += 1
                
                # Move o início para o próximo chunk, considerando a sobreposição
                start = end - self.chunk_overlap
                
                # Garante que o início não retroceda
                start = max(start, 0)
            
            return len(chunks)
        except Exception as e:
            logger.error(f"Erro ao criar chunks para o documento {document_id}: {str(e)}")
            logger.error(traceback.format_exc())
            self.db_conn.rollback()
            return 0
    
    def save_chunk(self, document_id, chunk_index, content):
        """Salva um chunk no banco de dados."""
        try:
            cursor = self.db_conn.cursor()
            
            query = """
            INSERT INTO public.document_chunks (document_id, chunk_index, content, created_at)
            VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(query, (document_id, chunk_index, content, datetime.datetime.now()))
            self.db_conn.commit()
            cursor.close()
            
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar chunk {chunk_index} do documento {document_id}: {str(e)}")
            self.db_conn.rollback()
            return False
    
    def process_documents(self):
        """Processa documentos que precisam ser divididos em chunks."""
        documents = self.get_documents_to_chunk()
        
        if not documents:
            logger.info("Nenhum documento para chunking encontrado")
            return 0
        
        total_chunks = 0
        
        for doc in documents:
            document_id, text = doc
            
            if not text or text.strip() == "":
                logger.warning(f"Documento {document_id} não possui texto para chunking")
                continue
            
            logger.info(f"Dividindo documento {document_id} em chunks")
            num_chunks = self.create_chunks(text, document_id)
            
            if num_chunks > 0:
                logger.info(f"Documento {document_id} dividido em {num_chunks} chunks")
                total_chunks += num_chunks
            else:
                logger.error(f"Falha ao dividir documento {document_id} em chunks")
        
        return total_chunks

if __name__ == "__main__":
    try:
        chunker = TextChunker()
        num_chunks = chunker.process_documents()
        logger.info(f"Total de {num_chunks} chunks criados")
    except KeyboardInterrupt:
        logger.info("Chunker interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal no chunker: {str(e)}")
        logger.error(traceback.format_exc())