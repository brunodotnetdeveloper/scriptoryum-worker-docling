import os
import logging
import psycopg2
import numpy as np
import time
import traceback
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# Importa OpenAI para usuários premium (opcional)
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("OpenAI não disponível. Usando apenas sentence-transformers.")

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
        
        # Carrega o modelo de embeddings gratuito (768 dimensões)
        self.model = SentenceTransformer('all-mpnet-base-v2')
        logger.info("Modelo de embeddings carregado: all-mpnet-base-v2 (768 dim) + padding para 1536 dim")
        
        # API key global como fallback (opcional)
        self.fallback_api_key = os.getenv('OPENAI_API_KEY')
        if OPENAI_AVAILABLE and self.fallback_api_key:
            logger.info("OpenAI disponível como upgrade premium")
        else:
            logger.info("Usando apenas embeddings gratuitos com sentence-transformers")
    
    def get_documents_without_embeddings(self, limit=10):
        """Obtém chunks de documentos que ainda não têm embeddings, incluindo o user_id."""
        try:
            cursor = self.db_conn.cursor()
            
            query = """
            SELECT dc.id, dc.document_id, dc.content, d.uploaded_by_user_id
            FROM public.document_chunks dc
            JOIN public.documents d ON dc.document_id = d.id
            WHERE dc.embedding IS NULL 
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
    
    def get_user_openai_api_key(self, user_id):
        """Obtém a API key do OpenAI do usuário baseada no provider padrão."""
        if not OPENAI_AVAILABLE or not user_id:
            return None
            
        try:
            cursor = self.db_conn.cursor()
            
            query = """
            SELECT apc.api_key 
            FROM public.a_i_configurations ac
            JOIN public.a_i_provider_configs apc ON ac.id = apc.a_i_configuration_id
            WHERE ac.user_id = %s 
              AND apc.provider = ac.default_provider 
              AND apc.is_enabled = true
              AND LOWER(apc.provider) = 'openai'
            """
            
            cursor.execute(query, (user_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return result[0]
            else:
                return None
        except Exception as e:
            logger.error(f"Erro ao obter API key do usuário {user_id}: {str(e)}")
            self.db_conn.rollback()
            return None
    
    def generate_embedding_openai(self, text, api_key):
        """Gera embedding usando OpenAI (1536 dimensões nativas)."""
        try:
            client = OpenAI(api_key=api_key)
            response = client.embeddings.create(
                model="text-embedding-ada-002",
                input=text
            )
            return np.array(response.data[0].embedding)
        except Exception as e:
            logger.error(f"Erro ao gerar embedding OpenAI: {str(e)}")
            return None
    
    def generate_embedding_free(self, text):
        """Gera embedding gratuito usando sentence-transformers + padding (768 + 768 = 1536)."""
        try:
            # Gera embedding de 768 dimensões
            embedding_768 = self.model.encode(text)
            
            # Adiciona padding de zeros para chegar a 1536 dimensões
            padding = np.zeros(768)
            embedding_1536 = np.concatenate([embedding_768, padding])
            
            return embedding_1536
        except Exception as e:
            logger.error(f"Erro ao gerar embedding gratuito: {str(e)}")
            return np.zeros(1536)

    def generate_embedding(self, text, user_id=None):
        """Gera embedding de 1536 dimensões - OpenAI se disponível, senão gratuito com padding."""
        try:
            if not text or text.strip() == "":
                return np.zeros(1536)
            
            # Tenta usar OpenAI se usuário tiver configuração
            if user_id and OPENAI_AVAILABLE:
                api_key = self.get_user_openai_api_key(user_id)
                if api_key:
                    embedding = self.generate_embedding_openai(text, api_key)
                    if embedding is not None:
                        logger.info(f"Embedding OpenAI gerado para usuário {user_id}")
                        return embedding
                    else:
                        logger.warning(f"Falha no OpenAI para usuário {user_id}, usando método gratuito")
            
            # Fallback para API key global se disponível
            if OPENAI_AVAILABLE and self.fallback_api_key:
                embedding = self.generate_embedding_openai(text, self.fallback_api_key)
                if embedding is not None:
                    logger.info(f"Embedding OpenAI (fallback) gerado para usuário {user_id}")
                    return embedding
            
            # Usa método gratuito com padding
            embedding = self.generate_embedding_free(text)
            logger.info(f"Embedding gratuito (768+768 padding) gerado para usuário {user_id}")
            return embedding
            
        except Exception as e:
            logger.error(f"Erro ao gerar embedding para usuário {user_id}: {str(e)}")
            return np.zeros(1536)
    
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
                chunk_id, document_id, content, user_id = chunk
                
                try:
                    # Gera o embedding usando a estratégia híbrida
                    embedding = self.generate_embedding(content, user_id)
                    
                    # Atualiza o chunk no banco de dados
                    success = self.update_chunk_embedding(chunk_id, embedding)
                    
                    if success:
                        logger.info(f"Embedding gerado para o chunk {chunk_id} do documento {document_id} (usuário: {user_id})")
                    else:
                        logger.error(f"Falha ao atualizar embedding para o chunk {chunk_id} (usuário: {user_id})")
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