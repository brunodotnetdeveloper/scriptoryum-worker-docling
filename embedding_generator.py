import os
import logging
import psycopg2
import numpy as np
import time
import traceback
from dotenv import load_dotenv
from openai import OpenAI

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
        
        # API key global como fallback (opcional)
        self.fallback_api_key = os.getenv('OPENAI_API_KEY')
        logger.info("Gerador de embeddings inicializado - usando API keys dos usuários")
    
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
                logger.warning(f"API key OpenAI não encontrada para usuário {user_id}")
                return None
        except Exception as e:
            logger.error(f"Erro ao obter API key do usuário {user_id}: {str(e)}")
            self.db_conn.rollback()
            return None
    
    def get_openai_client(self, api_key):
        """Cria um cliente OpenAI com a API key fornecida."""
        try:
            return OpenAI(api_key=api_key)
        except Exception as e:
            logger.error(f"Erro ao criar cliente OpenAI: {str(e)}")
            return None

    def generate_embedding(self, text, user_id=None):
        """Gera um embedding para o texto fornecido usando a API key do usuário."""
        try:
            if not text or text.strip() == "":
                return np.zeros(1536)  # Retorna um vetor de zeros para texto vazio
            
            # Tenta usar a API key do usuário primeiro
            api_key = None
            if user_id:
                api_key = self.get_user_openai_api_key(user_id)
            
            # Se não encontrou API key do usuário, usa fallback
            if not api_key:
                api_key = self.fallback_api_key
                if api_key:
                    logger.info(f"Usando API key fallback para usuário {user_id}")
                else:
                    logger.error(f"Nenhuma API key disponível para usuário {user_id}")
                    return np.zeros(1536)
            else:
                logger.info(f"Usando API key do usuário {user_id}")
            
            # Cria cliente OpenAI com a API key
            openai_client = self.get_openai_client(api_key)
            if not openai_client:
                return np.zeros(1536)
            
            # Gera o embedding usando OpenAI
            response = openai_client.embeddings.create(
                model="text-embedding-ada-002",
                input=text
            )
            
            embedding = np.array(response.data[0].embedding)
            return embedding
        except Exception as e:
            logger.error(f"Erro ao gerar embedding para usuário {user_id}: {str(e)}")
            return np.zeros(1536)  # Retorna um vetor de zeros em caso de erro
    
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
        """Processa chunks sem embeddings em lotes usando API keys dos usuários."""
        while True:
            # Obtém chunks sem embeddings (agora inclui user_id)
            chunks = self.get_documents_without_embeddings(batch_size)
            
            if not chunks:
                logger.info("Nenhum chunk sem embedding encontrado. Aguardando...")
                time.sleep(10)
                continue
            
            logger.info(f"Processando {len(chunks)} chunks")
            
            for chunk in chunks:
                chunk_id, document_id, content, user_id = chunk
                
                try:
                    # Gera o embedding usando a API key do usuário
                    embedding = self.generate_embedding(content, user_id)
                    
                    # Atualiza o chunk no banco de dados
                    success = self.update_chunk_embedding(chunk_id, embedding)
                    
                    if success:
                        logger.info(f"Embedding gerado para o chunk {chunk_id} do documento {document_id} (usuário: {user_id})")
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