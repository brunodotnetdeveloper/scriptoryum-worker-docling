import os
import time
import psutil
import logging
import psycopg2
import datetime
import platform
import json
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('system_monitor')

# Carrega variáveis de ambiente
load_dotenv()

class SystemMonitor:
    def __init__(self, log_interval=60):
        # Intervalo de log em segundos
        self.log_interval = log_interval
        
        # Conexão com o banco de dados PostgreSQL
        self.db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        
        # Informações do sistema
        self.system_info = {
            'hostname': platform.node(),
            'platform': platform.system(),
            'platform_release': platform.release(),
            'platform_version': platform.version(),
            'architecture': platform.machine(),
            'processor': platform.processor(),
            'python_version': platform.python_version(),
        }
        
        logger.info(f"Monitor iniciado em {self.system_info['hostname']} ({self.system_info['platform']})")
    
    def get_system_metrics(self):
        """Coleta métricas do sistema."""
        try:
            # Uso de CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Uso de memória
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used = memory.used / (1024 * 1024)  # MB
            memory_total = memory.total / (1024 * 1024)  # MB
            
            # Uso de disco
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            disk_used = disk.used / (1024 * 1024 * 1024)  # GB
            disk_total = disk.total / (1024 * 1024 * 1024)  # GB
            
            # Processos
            process_count = len(psutil.pids())
            
            # Tempo de atividade do sistema
            boot_time = datetime.datetime.fromtimestamp(psutil.boot_time())
            uptime = (datetime.datetime.now() - boot_time).total_seconds() / 3600  # horas
            
            return {
                'timestamp': datetime.datetime.now(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_used_mb': memory_used,
                'memory_total_mb': memory_total,
                'disk_percent': disk_percent,
                'disk_used_gb': disk_used,
                'disk_total_gb': disk_total,
                'process_count': process_count,
                'uptime_hours': uptime
            }
        except Exception as e:
            logger.error(f"Erro ao coletar métricas do sistema: {str(e)}")
            return None
    
    def get_document_processing_metrics(self):
        """Coleta métricas de processamento de documentos."""
        try:
            cursor = self.db_conn.cursor()
            
            # Total de documentos
            cursor.execute("SELECT COUNT(*) FROM public.documents")
            total_documents = cursor.fetchone()[0]
            
            # Documentos por status
            cursor.execute("""
                SELECT status, COUNT(*) 
                FROM public.documents 
                GROUP BY status
            """)
            status_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Tempo médio de processamento (para documentos processados)
            cursor.execute("""
                SELECT AVG(EXTRACT(EPOCH FROM (updated_at - processing_started_at))) 
                FROM public.documents 
                WHERE status = 'Processed' AND processing_started_at IS NOT NULL
            """)
            avg_processing_time = cursor.fetchone()[0] or 0
            
            # Total de chunks
            cursor.execute("SELECT COUNT(*) FROM public.document_chunks")
            total_chunks = cursor.fetchone()[0]
            
            # Chunks com embeddings
            cursor.execute("SELECT COUNT(*) FROM public.document_chunks WHERE embedding IS NOT NULL")
            chunks_with_embeddings = cursor.fetchone()[0]
            
            cursor.close()
            
            return {
                'timestamp': datetime.datetime.now(),
                'total_documents': total_documents,
                'status_counts': status_counts,
                'avg_processing_time_seconds': avg_processing_time,
                'total_chunks': total_chunks,
                'chunks_with_embeddings': chunks_with_embeddings,
                'embedding_completion_percent': (chunks_with_embeddings / total_chunks * 100) if total_chunks > 0 else 0
            }
        except Exception as e:
            logger.error(f"Erro ao coletar métricas de processamento: {str(e)}")
            return None
    
    def log_metrics(self):
        """Registra as métricas no log."""
        system_metrics = self.get_system_metrics()
        if system_metrics:
            logger.info(f"Métricas do sistema: CPU: {system_metrics['cpu_percent']}%, "  
                       f"Memória: {system_metrics['memory_percent']}% ({system_metrics['memory_used_mb']:.2f}MB / {system_metrics['memory_total_mb']:.2f}MB), "
                       f"Disco: {system_metrics['disk_percent']}% ({system_metrics['disk_used_gb']:.2f}GB / {system_metrics['disk_total_gb']:.2f}GB)")
        
        processing_metrics = self.get_document_processing_metrics()
        if processing_metrics:
            logger.info(f"Métricas de processamento: Total de documentos: {processing_metrics['total_documents']}, "
                       f"Processados: {processing_metrics['status_counts'].get('Processed', 0)}, "
                       f"Falhas: {processing_metrics['status_counts'].get('Failed', 0) + processing_metrics['status_counts'].get('TextExtractionFailed', 0)}, "
                       f"Tempo médio: {processing_metrics['avg_processing_time_seconds']:.2f}s, "
                       f"Chunks: {processing_metrics['total_chunks']}, "
                       f"Embeddings: {processing_metrics['chunks_with_embeddings']} ({processing_metrics['embedding_completion_percent']:.2f}%)")
    
    def run(self):
        """Executa o monitor em loop."""
        logger.info("Iniciando monitoramento do sistema...")
        
        try:
            while True:
                self.log_metrics()
                time.sleep(self.log_interval)
        except KeyboardInterrupt:
            logger.info("Monitor interrompido pelo usuário")
        except Exception as e:
            logger.error(f"Erro no monitor: {str(e)}")

if __name__ == "__main__":
    try:
        monitor = SystemMonitor(log_interval=60)  # Log a cada 60 segundos
        monitor.run()
    except Exception as e:
        logger.error(f"Erro fatal no monitor: {str(e)}")