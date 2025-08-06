import os
import logging
import psycopg2
import numpy as np
import time
import traceback
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('embedding_generator')

# Carrega variáveis de ambiente
load_dotenv()

class EmbeddingGenerator:
    def __init__(self):
        # Conexão com o banco de dados PostgreSQL
        self.db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        # Carrega o modelo de embeddings
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        logger.info("Modelo de embeddings carregado")
    
    def get_documents_without_embeddings(self, limit=10):
        """Obtém chunks de documentos que ainda não têm embeddings."""
        try:
            cursor = self.db_conn.cursor()
            
            query = """
            SELECT id, document_id, content 
            FROM public.document_chunks 
            WHERE embedding IS NULL 
            LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            chunks = cursor.fetchall()
            cursor.close()
            
            return chunks
        except Exception as e:
            logger.error(f"Erro ao obter chunks sem embeddings: {str(e)}")
            self.db_conn.rollback()
            return []
    
    def generate_embedding(self, text):
        """Gera um embedding para o texto fornecido."""
        try:
            if not text or text.strip() == "":
                return np.zeros(384)  # Retorna um vetor de zeros para texto vazio
            
            # Gera o embedding
            embedding = self.model.encode(text)
            return embedding
        except Exception as e:
            logger.error(f"Erro ao gerar embedding: {str(e)}")
            return np.zeros(384)  # Retorna um vetor de zeros em caso de erro
    
    def update_chunk_embedding(self, chunk_id, embedding):
        """Atualiza o embedding de um chunk no banco de dados."""
        try:
            cursor = self.db_conn.cursor()
            
            query = """
            UPDATE public.document_chunks 
            SET embedding = %s 
            WHERE id = %s
            """
            
            # Converte o array numpy para uma string formatada para o PostgreSQL
            embedding_str = f"[{','.join(map(str, embedding))}]"
            
            cursor.execute(query, (embedding_str, chunk_id))
            self.db_conn.commit()
            cursor.close()
            
            return True
        except Exception as e:
            logger.error(f"Erro ao atualizar embedding do chunk {chunk_id}: {str(e)}")
            self.db_conn.rollback()
            return False
    
    def process_chunks(self, batch_size=10):
        """Processa chunks sem embeddings em lotes."""
        while True:
            # Obtém chunks sem embeddings
            chunks = self.get_documents_without_embeddings(batch_size)
            
            if not chunks:
                logger.info("Nenhum chunk sem embedding encontrado. Aguardando...")
                time.sleep(10)
                continue
            
            logger.info(f"Processando {len(chunks)} chunks")
            
            for chunk in chunks:
                chunk_id, document_id, content = chunk
                
                try:
                    # Gera o embedding
                    embedding = self.generate_embedding(content)
                    
                    # Atualiza o chunk no banco de dados
                    success = self.update_chunk_embedding(chunk_id, embedding)
                    
                    if success:
                        logger.info(f"Embedding gerado para o chunk {chunk_id} do documento {document_id}")
                    else:
                        logger.error(f"Falha ao atualizar embedding para o chunk {chunk_id}")
                except Exception as e:
                    logger.error(f"Erro ao processar chunk {chunk_id}: {str(e)}")
                    logger.error(traceback.format_exc())
            
            # Pequena pausa entre lotes
            time.sleep(1)

if __name__ == "__main__":
    try:
        generator = EmbeddingGenerator()
        generator.process_chunks()
    except KeyboardInterrupt:
        logger.info("Gerador de embeddings interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal no gerador de embeddings: {str(e)}")
        logger.error(traceback.format_exc())