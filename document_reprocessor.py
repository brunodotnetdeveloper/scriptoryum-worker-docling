import os
import json
import time
import logging
import datetime
import psycopg2
import redis
from dotenv import load_dotenv
from document_worker import DocumentProcessor, DocumentStatus

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('document_reprocessor')

# Carrega variáveis de ambiente
load_dotenv()

class DocumentReprocessor:
    def __init__(self, check_interval_minutes=30, max_retry_attempts=3):
        """
        Inicializa o reprocessador de documentos.
        
        Args:
            check_interval_minutes: Intervalo em minutos para verificar documentos com falha
            max_retry_attempts: Número máximo de tentativas de reprocessamento por documento
        """
        self.check_interval_minutes = check_interval_minutes
        self.max_retry_attempts = max_retry_attempts
        
        # Conexão com o banco de dados PostgreSQL
        self.db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        # Conexão com o Redis
        self.redis_client = redis.from_url(os.getenv('REDIS_CONNECTION_STRING'))
        self.queue_name = os.getenv('REDIS_QUEUE')
        
        # Instância do processador de documentos para reprocessamento
        self.document_processor = DocumentProcessor()
        
        logger.info(f"Reprocessador iniciado - Intervalo: {check_interval_minutes} minutos, Max tentativas: {max_retry_attempts}")
    
    def get_failed_documents(self, limit=10):
        """
        Busca documentos com status de falha na extração de texto.
        
        Args:
            limit: Número máximo de documentos para retornar
            
        Returns:
            Lista de tuplas com (id, storage_path, file_type, retry_count, updated_at)
        """
        try:
            cursor = self.db_conn.cursor()
            
            # Busca documentos com falha na extração de texto
            # Inclui uma verificação para não reprocessar documentos muito recentemente falhados (últimos 10 minutos)
            time_threshold = datetime.datetime.now() - datetime.timedelta(minutes=10)
            
            query = """
            SELECT id, storage_path, file_type, 
                   COALESCE(retry_count, 0) as retry_count,
                   updated_at
            FROM public.documents 
            WHERE status = %s 
                AND updated_at < %s
                AND COALESCE(retry_count, 0) < %s
            ORDER BY updated_at ASC
            LIMIT %s
            """
            
            cursor.execute(query, (
                DocumentStatus.TEXT_EXTRACTION_FAILED,
                time_threshold,
                self.max_retry_attempts,
                limit
            ))
            
            failed_documents = cursor.fetchall()
            cursor.close()
            
            return failed_documents
        except Exception as e:
            logger.error(f"Erro ao buscar documentos com falha: {str(e)}")
            return []
    
    def increment_retry_count(self, document_id):
        """
        Incrementa o contador de tentativas de reprocessamento do documento.
        
        Args:
            document_id: ID do documento
        """
        try:
            cursor = self.db_conn.cursor()
            
            update_query = """
            UPDATE public.documents 
            SET retry_count = COALESCE(retry_count, 0) + 1,
                updated_at = %s
            WHERE id = %s
            """
            
            cursor.execute(update_query, (datetime.datetime.now(), document_id))
            self.db_conn.commit()
            cursor.close()
            
            logger.info(f"Contador de tentativas incrementado para documento {document_id}")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Erro ao incrementar contador de tentativas para documento {document_id}: {str(e)}")
    
    def mark_as_permanently_failed(self, document_id):
        """
        Marca um documento como permanentemente falhado após esgotar as tentativas.
        
        Args:
            document_id: ID do documento
        """
        try:
            cursor = self.db_conn.cursor()
            
            update_query = """
            UPDATE public.documents 
            SET status = %s,
                updated_at = %s,
                processing_notes = COALESCE(processing_notes, '') || %s
            WHERE id = %s
            """
            
            processing_note = f"\n[{datetime.datetime.now()}] Documento marcado como permanentemente falhado após {self.max_retry_attempts} tentativas de reprocessamento."
            
            cursor.execute(update_query, (
                DocumentStatus.FAILED,
                datetime.datetime.now(),
                processing_note,
                document_id
            ))
            
            self.db_conn.commit()
            cursor.close()
            
            logger.warning(f"Documento {document_id} marcado como permanentemente falhado")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Erro ao marcar documento {document_id} como permanentemente falhado: {str(e)}")
    
    def reprocess_document(self, document_id, storage_path, file_type, retry_count):
        """
        Reprocessa um documento específico.
        
        Args:
            document_id: ID do documento
            storage_path: Caminho do arquivo no storage
            file_type: Tipo do arquivo
            retry_count: Número atual de tentativas
            
        Returns:
            bool: True se o reprocessamento foi bem-sucedido, False caso contrário
        """
        try:
            logger.info(f"Iniciando reprocessamento do documento {document_id} (tentativa {retry_count + 1}/{self.max_retry_attempts})")
            
            # Incrementa o contador de tentativas
            self.increment_retry_count(document_id)
            
            # Cria os dados do documento para reprocessamento
            document_data = {
                'DocumentId': document_id,
                'StoragePath': storage_path,
                'FileType': file_type
            }
            
            # Processa o documento diretamente
            success = self.document_processor.process_document(document_data)
            
            if success:
                logger.info(f"Documento {document_id} reprocessado com sucesso")
                return True
            else:
                logger.warning(f"Falha no reprocessamento do documento {document_id}")
                
                # Verifica se esgotou as tentativas
                if retry_count + 1 >= self.max_retry_attempts:
                    logger.warning(f"Esgotadas as tentativas para documento {document_id}, marcando como permanentemente falhado")
                    self.mark_as_permanently_failed(document_id)
                
                return False
        except Exception as e:
            logger.error(f"Erro durante reprocessamento do documento {document_id}: {str(e)}")
            
            # Verifica se esgotou as tentativas
            if retry_count + 1 >= self.max_retry_attempts:
                logger.warning(f"Esgotadas as tentativas para documento {document_id}, marcando como permanentemente falhado")
                self.mark_as_permanently_failed(document_id)
            
            return False
    
    def run_reprocessing_cycle(self):
        """
        Executa um ciclo de reprocessamento de documentos com falha.
        
        Returns:
            dict: Estatísticas do ciclo de reprocessamento
        """
        try:
            logger.info("Iniciando ciclo de reprocessamento...")
            
            # Busca documentos com falha
            failed_documents = self.get_failed_documents()
            
            if not failed_documents:
                logger.info("Nenhum documento com falha encontrado para reprocessamento")
                return {
                    'total_found': 0,
                    'reprocessed_successfully': 0,
                    'reprocessing_failed': 0,
                    'permanently_failed': 0
                }
            
            logger.info(f"Encontrados {len(failed_documents)} documentos para reprocessamento")
            
            stats = {
                'total_found': len(failed_documents),
                'reprocessed_successfully': 0,
                'reprocessing_failed': 0,
                'permanently_failed': 0
            }
            
            # Reprocessa cada documento individualmente
            for doc_id, storage_path, file_type, retry_count, updated_at in failed_documents:
                logger.info(f"Reprocessando documento {doc_id} (última atualização: {updated_at})")
                
                success = self.reprocess_document(doc_id, storage_path, file_type, retry_count)
                
                if success:
                    stats['reprocessed_successfully'] += 1
                else:
                    stats['reprocessing_failed'] += 1
                    
                    # Verifica se foi marcado como permanentemente falhado
                    if retry_count + 1 >= self.max_retry_attempts:
                        stats['permanently_failed'] += 1
                
                # Aguarda um pouco entre reprocessamentos para não sobrecarregar o sistema
                time.sleep(2)
            
            logger.info(f"Ciclo de reprocessamento concluído: {stats}")
            return stats
        except Exception as e:
            logger.error(f"Erro durante ciclo de reprocessamento: {str(e)}")
            return {
                'total_found': 0,
                'reprocessed_successfully': 0,
                'reprocessing_failed': 0,
                'permanently_failed': 0,
                'error': str(e)
            }
    
    def run(self):
        """
        Executa o reprocessador em loop contínuo.
        """
        logger.info(f"Iniciando reprocessador de documentos (verificação a cada {self.check_interval_minutes} minutos)...")
        
        try:
            while True:
                # Executa um ciclo de reprocessamento
                stats = self.run_reprocessing_cycle()
                
                # Log das estatísticas
                if stats.get('error'):
                    logger.error(f"Erro no ciclo de reprocessamento: {stats['error']}")
                else:
                    logger.info(
                        f"Estatísticas do ciclo: "
                        f"Encontrados: {stats['total_found']}, "
                        f"Reprocessados com sucesso: {stats['reprocessed_successfully']}, "
                        f"Falhas no reprocessamento: {stats['reprocessing_failed']}, "
                        f"Marcados como permanentemente falhados: {stats['permanently_failed']}"
                    )
                
                # Aguarda o intervalo especificado antes do próximo ciclo
                logger.info(f"Aguardando {self.check_interval_minutes} minutos para o próximo ciclo...")
                time.sleep(self.check_interval_minutes * 60)
                
        except KeyboardInterrupt:
            logger.info("Reprocessador interrompido pelo usuário")
        except Exception as e:
            logger.error(f"Erro fatal no reprocessador: {str(e)}")
        finally:
            # Fecha conexões
            try:
                self.db_conn.close()
                logger.info("Conexão com banco de dados fechada")
            except Exception:
                pass

if __name__ == "__main__":
    try:
        # Configurações padrão: verifica a cada 30 minutos, máximo 3 tentativas por documento
        reprocessor = DocumentReprocessor(
            check_interval_minutes=30,
            max_retry_attempts=3
        )
        reprocessor.run()
    except KeyboardInterrupt:
        logger.info("Reprocessador interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal no reprocessador: {str(e)}")