import os
import json
import time
import logging
import datetime
import psycopg2
import redis
import boto3
import tempfile
import traceback
import io
from dotenv import load_dotenv
from enum import Enum
from botocore.client import Config
import pytesseract
from PIL import Image
import fitz  # PyMuPDF para PDFs
import cv2
import numpy as np

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('document_worker')

# Carrega variáveis de ambiente
load_dotenv()

# Enumeração para status do documento
class DocumentStatus(str, Enum):
    UPLOADED = "Uploaded"
    QUEUED = "Queued"
    PENDING = "Pending"
    EXTRACTING_TEXT = "ExtractingText"
    ANALYZING_CONTENT = "AnalyzingContent"
    PROCESSED = "Processed"
    TEXT_EXTRACTION_FAILED = "TextExtractionFailed"
    CONTENT_ANALYSIS_FAILED = "ContentAnalysisFailed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    PARTIALLY_PROCESSED = "PartiallyProcessed"
    ENTITIES_EXTRACTION_FAILED = "EntitiesExtractionFailed"
    RISKS_ANALYSIS_FAILED = "RisksAnalysisFailed"
    INSIGHTS_GENERATION_FAILED = "InsightsGenerationFailed"
    ANALYZED = "Analyzed"

# Enumeração para tipos de arquivo
class FileType(str, Enum):
    PDF = "PDF"
    DOCX = "DOCX"
    DOC = "DOC"
    TXT = "TXT"
    RTF = "RTF"
    PNG = "PNG"
    JPG = "JPG"
    JPEG = "JPEG"
    TIFF = "TIFF"
    XLS = "XLS"
    XLSX = "XLSX"
    PPT = "PPT"
    PPTX = "PPTX"
    CSV = "CSV"
    JSON = "JSON"
    XML = "XML"
    HTML = "HTML"
    OTHER = "OTHER"

class DocumentProcessor:
    def __init__(self):
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
        
        # Configuração do Cloudflare R2 (compatível com S3)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('CLOUDFLARE_SERVICE_URL'),
            aws_access_key_id=os.getenv('CLOUDFLARE_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('CLOUDFLARE_SECRET_KEY'),
            config=Config(signature_version='s3v4')
        )
        self.bucket_name = os.getenv('CLOUDFLARE_BUCKET_NAME')
        
        # Configuração do Tesseract OCR
        tesseract_path = os.getenv('TESSERACT_PATH')
        if tesseract_path:
            pytesseract.pytesseract.tesseract_cmd = tesseract_path
        
        # Tempo máximo (em minutos) que um documento pode ficar no status ExtractingText
        self.max_extracting_time_minutes = 30
    
    def update_document_status(self, document_id, status, text_extracted=None, summary=None):
        """Atualiza o status do documento no banco de dados."""
        try:
            cursor = self.db_conn.cursor()
            
            update_query = """
            UPDATE public.documents 
            SET status = %s, updated_at = %s
            """
            
            params = [status, datetime.datetime.now()]
            
            # Comentando a atualização do campo processing_started_at pois está causando erros
            # if status == DocumentStatus.EXTRACTING_TEXT:
            #     update_query += ", processing_started_at = %s"
            #     params.append(datetime.datetime.now())
            
            # Se temos texto extraído, atualize o campo text_extracted
            if text_extracted is not None:
                update_query += ", text_extracted = %s"
                params.append(text_extracted)
            
            # Se temos um resumo, atualize o campo summary
            if summary is not None:
                update_query += ", summary = %s"
                params.append(summary)
            
            update_query += " WHERE id = %s"
            params.append(document_id)
            
            cursor.execute(update_query, params)
            self.db_conn.commit()
            cursor.close()
            logger.info(f"Status do documento {document_id} atualizado para {status}")
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Erro ao atualizar status do documento {document_id}: {str(e)}")
            raise
    
    def download_document(self, storage_path, file_type):
        """Baixa o documento do Cloudflare R2 para um arquivo temporário."""
        try:
            # Mapeia os tipos de arquivo do enum FileType para suas extensões
            file_type_mapping = {
                "PDF": "pdf",
                "DOCX": "docx", 
                "DOC": "doc",
                "TXT": "txt",
                "RTF": "rtf",
                "ODT": "odt",
                "HTML": "html",
                "XML": "xml",
                "XLS": "xls",
                "XLSX": "xlsx",
                "JSON": "json"
            }
            
            # Converte file_type para string se for um número (compatibilidade com versões antigas)
            if isinstance(file_type, int):
                logger.info(f"Convertendo file_type de inteiro {file_type} para string")
                file_type = "PDF"  # Valor padrão para casos desconhecidos
            
            # Normaliza o file_type para maiúsculo
            file_type_upper = str(file_type).upper()
            
            # Obtém a extensão correspondente ou usa 'pdf' como padrão
            extension = file_type_mapping.get(file_type_upper, "pdf")
            
            logger.info(f"Tipo de arquivo: {file_type} -> Extensão: {extension}")
            
            # Cria um arquivo temporário com a extensão correta
            suffix = f".{extension}"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp_file:
                temp_path = temp_file.name
            
            # Baixa o arquivo do R2 para o arquivo temporário
            self.s3_client.download_file(
                self.bucket_name,
                storage_path,
                temp_path
            )
            
            logger.info(f"Documento baixado para {temp_path}")
            return temp_path
        except Exception as e:
            logger.error(f"Erro ao baixar documento {storage_path}: {str(e)}")
            raise
    
    def extract_text_from_image(self, image_path):
        """Extrai texto de uma imagem usando Tesseract OCR."""
        try:
            # Abre a imagem
            image = Image.open(image_path)
            
            # Converte para RGB se necessário
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Extrai o texto usando Tesseract
            text = pytesseract.image_to_string(image, lang='por+eng')
            
            return text.strip()
        except Exception as e:
            logger.error(f"Erro ao extrair texto da imagem: {str(e)}")
            return ""
    
    def extract_text_from_pdf(self, pdf_path):
        """Extrai texto de um PDF usando PyMuPDF e OCR quando necessário."""
        try:
            text = ""
            doc = fitz.open(pdf_path)
            
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                
                # Primeiro tenta extrair texto diretamente
                page_text = page.get_text()
                
                # Se não há texto ou muito pouco texto, usa OCR
                if len(page_text.strip()) < 50:
                    # Converte a página para imagem
                    mat = fitz.Matrix(2.0, 2.0)  # Aumenta a resolução
                    pix = page.get_pixmap(matrix=mat)
                    img_data = pix.tobytes("png")
                    
                    # Converte para PIL Image
                    image = Image.open(io.BytesIO(img_data))
                    
                    # Extrai texto usando OCR
                    ocr_text = pytesseract.image_to_string(image, lang='por+eng')
                    text += f"\n--- Página {page_num + 1} (OCR) ---\n{ocr_text}\n"
                else:
                    text += f"\n--- Página {page_num + 1} ---\n{page_text}\n"
            
            doc.close()
            return text.strip()
        except Exception as e:
            logger.error(f"Erro ao extrair texto do PDF: {str(e)}")
            return ""
    
    def extract_text(self, file_path):
        """Extrai texto do documento usando Tesseract OCR e PyMuPDF."""
        try:
            # Determina o tipo de arquivo pela extensão
            file_extension = os.path.splitext(file_path)[1].lower()
            
            text = ""
            
            if file_extension == '.pdf':
                text = self.extract_text_from_pdf(file_path)
            elif file_extension in ['.png', '.jpg', '.jpeg', '.tiff', '.bmp', '.gif']:
                text = self.extract_text_from_image(file_path)
            else:
                # Para outros tipos de arquivo, tenta ler como texto simples
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        text = f.read()
                except UnicodeDecodeError:
                    # Se falhar com UTF-8, tenta outras codificações
                    try:
                        with open(file_path, 'r', encoding='latin-1') as f:
                            text = f.read()
                    except Exception:
                        logger.warning(f"Não foi possível ler o arquivo {file_path} como texto")
                        text = ""
            
            # Limita o tamanho do texto para evitar problemas com documentos muito grandes
            max_text_size = 10 * 1024 * 1024  # 10MB em bytes
            if len(text) > max_text_size:
                logger.warning(f"Texto extraído muito grande ({len(text)} bytes), truncando para {max_text_size} bytes")
                text = text[:max_text_size]
                text += "\n\n[TEXTO TRUNCADO DEVIDO AO TAMANHO EXCESSIVO]"
            
            # Gera um resumo simples (primeiros 1000 caracteres)
            summary = text[:1000] + "..." if len(text) > 1000 else text
            
            logger.info(f"Texto extraído com sucesso: {len(text)} caracteres")
            return text, summary
        except Exception as e:
            logger.error(f"Erro ao extrair texto do documento: {str(e)}")
            logger.error(traceback.format_exc())
            # Retorna um texto de erro e um resumo indicando a falha
            error_text = f"ERRO NA EXTRAÇÃO DE TEXTO: {str(e)}"
            return error_text, error_text
    
    def process_document(self, document_data):
        """Processa um documento da fila."""
        document_id = document_data.get('DocumentId')
        storage_path = document_data.get('StoragePath')
        file_type = document_data.get('FileType')
        temp_file_path = None
        
        # Validação básica dos dados do documento
        if not document_id:
            logger.error("DocumentId não encontrado nos dados do documento")
            return False
        
        if not storage_path:
            logger.error(f"StoragePath não encontrado para o documento {document_id}")
            try:
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
            except Exception:
                pass
            return False
        
        try:
            # Atualiza o status para ExtractingText
            self.update_document_status(document_id, DocumentStatus.EXTRACTING_TEXT)
            
            # Baixa o documento
            try:
                temp_file_path = self.download_document(storage_path, file_type)
            except Exception as download_error:
                logger.error(f"Erro ao baixar documento {document_id}: {str(download_error)}")
                logger.error(traceback.format_exc())
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                return False
            
            # Verifica se o arquivo foi baixado corretamente
            if not temp_file_path or not os.path.exists(temp_file_path):
                logger.error(f"Arquivo temporário não encontrado para o documento {document_id}")
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                return False
            
            # Verifica o tamanho do arquivo
            try:
                file_size = os.path.getsize(temp_file_path)
                logger.info(f"Tamanho do arquivo do documento {document_id}: {file_size} bytes")
                
                # Se o arquivo for muito grande (mais de 100MB), marca como falha
                max_file_size = 100 * 1024 * 1024  # 100MB em bytes
                if file_size > max_file_size:
                    logger.error(f"Documento {document_id} muito grande ({file_size} bytes), excede o limite de {max_file_size} bytes")
                    self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                    return False
            except Exception as size_error:
                logger.error(f"Erro ao verificar tamanho do arquivo do documento {document_id}: {str(size_error)}")
            
            # Extrai o texto
            text, summary = self.extract_text(temp_file_path)
            
            # Verifica se o texto contém mensagem de erro
            if text.startswith("ERRO NA EXTRAÇÃO DE TEXTO"):
                logger.error(f"Falha na extração de texto do documento {document_id}: {text}")
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                return False
            
            # Atualiza o documento com o texto extraído e status Processed
            try:
                self.update_document_status(
                    document_id, 
                    DocumentStatus.PROCESSED, 
                    text_extracted=text,
                    summary=summary
                )
            except Exception as update_error:
                logger.error(f"Erro ao atualizar documento {document_id} com texto extraído: {str(update_error)}")
                logger.error(traceback.format_exc())
                # Tenta novamente com um texto truncado se o erro for relacionado ao tamanho
                if len(text) > 1000000:  # Se o texto for maior que 1MB
                    logger.warning(f"Tentando atualizar documento {document_id} com texto truncado")
                    truncated_text = text[:1000000] + "\n\n[TEXTO TRUNCADO DEVIDO AO TAMANHO EXCESSIVO]"
                    try:
                        self.update_document_status(
                            document_id, 
                            DocumentStatus.PROCESSED, 
                            text_extracted=truncated_text,
                            summary=summary
                        )
                    except Exception as truncate_error:
                        logger.error(f"Erro ao atualizar documento {document_id} com texto truncado: {str(truncate_error)}")
                        self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                        return False
            
            logger.info(f"Documento {document_id} processado com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao processar documento {document_id}: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Atualiza o status para falha
            try:
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
            except Exception as update_error:
                logger.error(f"Erro ao atualizar status de falha: {str(update_error)}")
            
            return False
        finally:
            # Remove o arquivo temporário se existir
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.info(f"Arquivo temporário {temp_file_path} removido")
                except Exception as e:
                    logger.error(f"Erro ao remover arquivo temporário: {str(e)}")
    
    def check_stuck_documents(self):
        """Verifica documentos que estão presos no status ExtractingText por muito tempo."""
        try:
            cursor = self.db_conn.cursor()
            
            # Calcula o tempo limite para considerar um documento como preso
            time_limit = datetime.datetime.now() - datetime.timedelta(minutes=self.max_extracting_time_minutes)
            
            # Busca documentos que estão no status ExtractingText por mais tempo que o limite
            query = """
            SELECT id, storage_path, file_type 
            FROM public.documents 
            WHERE status = %s AND updated_at < %s
            LIMIT 10
            """
            
            cursor.execute(query, (DocumentStatus.EXTRACTING_TEXT, time_limit))
            stuck_documents = cursor.fetchall()
            cursor.close()
            
            if stuck_documents:
                logger.warning(f"Encontrados {len(stuck_documents)} documentos presos no status ExtractingText")
                
                for doc in stuck_documents:
                    document_id, storage_path, file_type = doc
                    logger.warning(f"Marcando documento {document_id} como falha (preso no status ExtractingText)")
                    
                    try:
                        self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                        logger.info(f"Status do documento {document_id} atualizado para falha")
                    except Exception as e:
                        logger.error(f"Erro ao atualizar status do documento {document_id}: {str(e)}")
            
            return len(stuck_documents)
        except Exception as e:
            logger.error(f"Erro ao verificar documentos presos: {str(e)}")
            logger.error(traceback.format_exc())
            return 0
    
    def run(self):
        """Executa o worker em loop, processando documentos da fila."""
        logger.info("Iniciando worker de processamento de documentos...")
        
        # Contador para verificação periódica de documentos presos
        check_counter = 0
        
        while True:
            try:
                # A cada 20 iterações, verifica documentos presos
                check_counter += 1
                if check_counter >= 20:
                    logger.info("Verificando documentos presos...")
                    self.check_stuck_documents()
                    check_counter = 0
                
                # Tenta obter um documento da fila
                queue_item = self.redis_client.lpop(self.queue_name)
                
                if queue_item:
                    try:
                        # Converte o item da fila para um dicionário
                        document_data = json.loads(queue_item)
                        document_id = document_data.get('DocumentId')
                        logger.info(f"Documento obtido da fila: {document_id}")
                        
                        # Processa o documento com timeout de segurança
                        try:
                            # Processa o documento
                            self.process_document(document_data)
                        except Exception as doc_error:
                            # Captura erros específicos do processamento do documento
                            logger.error(f"Erro ao processar documento {document_id}: {str(doc_error)}")
                            logger.error(traceback.format_exc())
                            
                            # Tenta atualizar o status para falha, mas não interrompe o worker
                            try:
                                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                                logger.info(f"Status do documento {document_id} atualizado para falha")
                            except Exception as update_error:
                                logger.error(f"Erro ao atualizar status de falha para documento {document_id}: {str(update_error)}")
                    except json.JSONDecodeError as json_error:
                        logger.error(f"Erro ao decodificar JSON do item da fila: {str(json_error)}")
                        logger.error(f"Item da fila: {queue_item}")
                    except Exception as item_error:
                        logger.error(f"Erro ao processar item da fila: {str(item_error)}")
                        logger.error(traceback.format_exc())
                else:
                    # Se não há documentos na fila, aguarda um pouco
                    logger.info("Nenhum documento na fila. Aguardando...")
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Erro no loop principal: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(10)  # Aguarda um pouco mais em caso de erro
                # Tenta reconectar ao Redis se a conexão foi perdida
                try:
                    self.redis_client = redis.from_url(os.getenv('REDIS_CONNECTION_STRING'))
                    logger.info("Reconectado ao Redis")
                except Exception as redis_error:
                    logger.error(f"Erro ao reconectar ao Redis: {str(redis_error)}")
                # Tenta reconectar ao banco de dados se a conexão foi perdida
                try:
                    if self.db_conn.closed:
                        self.db_conn = psycopg2.connect(
                            host=os.getenv('DB_HOST'),
                            database=os.getenv('DB_NAME'),
                            user=os.getenv('DB_USER'),
                            password=os.getenv('DB_PASSWORD'),
                            port=os.getenv('DB_PORT')
                        )
                        logger.info("Reconectado ao banco de dados")
                except Exception as db_error:
                    logger.error(f"Erro ao reconectar ao banco de dados: {str(db_error)}")

if __name__ == "__main__":
    try:
        processor = DocumentProcessor()
        processor.run()
    except KeyboardInterrupt:
        logger.info("Worker interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal no worker: {str(e)}")
        logger.error(traceback.format_exc())