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
from sentence_transformers import SentenceTransformer
import re
import httpx
import asyncio

# Import condicional do OpenAI
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

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
        
        # Configuração da API principal para notificações
        self.main_api_url = os.getenv('MAIN_API_URL', 'http://localhost:5220')
        self.main_api_token = os.getenv('MAIN_API_TOKEN')
        
        # Inicializa o modelo de embeddings (768 dimensões -> 1536 com padding)
        try:
            # Carrega o modelo de embeddings gratuito (768 dimensões)
            self.embedding_model = SentenceTransformer('all-mpnet-base-v2')
            logger.info("Modelo de embeddings carregado: all-mpnet-base-v2 (768 dim) + padding para 1536 dim")
            
            # Configuração para OpenAI (opcional)
            self.fallback_api_key = os.getenv('OPENAI_API_KEY')
            if OPENAI_AVAILABLE and self.fallback_api_key:
                logger.info("OpenAI disponível como opção premium para embeddings")
            else:
                logger.info("Usando apenas embeddings gratuitos com sentence-transformers")
        except Exception as e:
            logger.error(f"Erro ao carregar modelo de embeddings: {str(e)}")
            self.embedding_model = None
    
    def ensure_db_connection(self):
        """Garante que a conexão com o banco de dados está ativa, reconectando se necessário."""
        try:
            # Testa a conexão executando uma query simples
            if self.db_conn.closed:
                logger.warning("Conexão com banco de dados está fechada, reconectando...")
                self._reconnect_db()
            else:
                # Testa se a conexão está realmente funcionando
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
        except (psycopg2.OperationalError, psycopg2.InterfaceError, AttributeError) as e:
            logger.warning(f"Conexão com banco de dados perdida: {str(e)}, reconectando...")
            self._reconnect_db()
        except Exception as e:
            logger.error(f"Erro inesperado ao verificar conexão: {str(e)}")
            self._reconnect_db()
    
    def _reconnect_db(self):
        """Reconecta ao banco de dados."""
        try:
            if hasattr(self, 'db_conn') and not self.db_conn.closed:
                self.db_conn.close()
        except:
            pass
        
        self.db_conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        logger.info("Reconectado ao banco de dados com sucesso")
    
    def execute_with_retry(self, operation_func, max_retries=3, *args, **kwargs):
        """Executa uma operação de banco de dados com retry automático em caso de falha de conexão."""
        for attempt in range(max_retries):
            try:
                self.ensure_db_connection()
                return operation_func(*args, **kwargs)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.warning(f"Tentativa {attempt + 1}/{max_retries} falhou: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
                    continue
                else:
                    logger.error(f"Todas as tentativas falharam para operação de banco de dados")
                    raise
            except Exception as e:
                logger.error(f"Erro não relacionado à conexão: {str(e)}")
                raise
    
    async def create_notification(self, user_id: str, notification_type: str, title: str, message: str, document_id: int = None):
        """Cria notificação via API principal"""
        if not self.main_api_token:
            logger.warning("MAIN_API_TOKEN não configurado, pulando criação de notificação")
            return
        
        try:
            notification_data = {
                "userId": user_id,
                "type": notification_type,
                "title": title,
                "message": message
            }
            
            if document_id:
                notification_data["documentId"] = document_id
            
            headers = {
                "Authorization": f"Bearer {self.main_api_token}",
                "Content-Type": "application/json"
            }
            
            # Loga o JSON que será enviado para a API (sem expor o token)
            try:
                logger.info(
                    f"Enviando notificação para {self.main_api_url}/api/notifications com payload: "
                    f"{json.dumps(notification_data, ensure_ascii=False)}"
                )
                logger.debug("Cabeçalhos: {'Authorization': 'Bearer ****', 'Content-Type': 'application/json'}")
            except Exception as log_err:
                logger.warning(f"Falha ao serializar payload de notificação para log: {str(log_err)}")
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.main_api_url}/api/notifications",
                    json=notification_data,
                    headers=headers,
                    timeout=10.0
                )
                
                if response.status_code == 201:
                    logger.info(f"Notificação criada com sucesso para usuário {user_id}: {title}")
                else:
                    logger.error(f"Erro ao criar notificação: {response.status_code} - {response.text}")
                    
        except Exception as e:
            logger.error(f"Erro ao criar notificação: {str(e)}")
    
    def get_document_user_id(self, document_id):
        """Obtém o ID do usuário que fez upload do documento"""
        def _get_document_user_id_operation():
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT uploaded_by_user_id FROM public.documents WHERE id = %s", (document_id,))
            result = cursor.fetchone()
            cursor.close()
            return result
        
        try:
            result = self.execute_with_retry(_get_document_user_id_operation)
            if result:
                return result[0]
            return None
        except Exception as e:
            logger.error(f"Erro ao obter user_id do documento {document_id}: {str(e)}")
            return None
    
    def update_document_status(self, document_id, status, text_extracted=None, summary=None):
        """Atualiza o status do documento no banco de dados."""
        def _update_document_status_operation():
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
        
        try:
            self.execute_with_retry(_update_document_status_operation)
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
        user_id = None
        
        # Validação básica dos dados do documento
        if not document_id:
            logger.error("DocumentId não encontrado nos dados do documento")
            return False
        
        # Obtém o ID do usuário para notificações
        user_id = self.get_document_user_id(document_id)
        
        if not storage_path:
            logger.error(f"StoragePath não encontrado para o documento {document_id}")
            try:
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                # Notifica sobre falha
                if user_id:
                    asyncio.run(self.create_notification(
                        user_id,
                        "DocumentProcessing",
                        "Falha no processamento",
                        f"Erro ao processar documento: caminho de armazenamento não encontrado",
                        document_id
                    ))
            except Exception:
                pass
            return False
        
        try:
            # Notifica início do processamento
            if user_id:
                asyncio.run(self.create_notification(
                    user_id,
                    "DocumentProcessing",
                    "Processamento iniciado",
                    f"Iniciando extração de texto do documento",
                    document_id
                ))
            
            # Atualiza o status para ExtractingText
            self.update_document_status(document_id, DocumentStatus.EXTRACTING_TEXT)
            
            # Baixa o documento
            try:
                temp_file_path = self.download_document(storage_path, file_type)
            except Exception as download_error:
                logger.error(f"Erro ao baixar documento {document_id}: {str(download_error)}")
                logger.error(traceback.format_exc())
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                # Notifica sobre falha no download
                if user_id:
                    asyncio.run(self.create_notification(
                        user_id,
                        "DocumentProcessing",
                        "Falha no processamento",
                        f"Erro ao baixar documento para processamento",
                        document_id
                    ))
                return False
            
            # Verifica se o arquivo foi baixado corretamente
            if not temp_file_path or not os.path.exists(temp_file_path):
                logger.error(f"Arquivo temporário não encontrado para o documento {document_id}")
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                # Notifica sobre falha
                if user_id:
                    asyncio.run(self.create_notification(
                        user_id,
                        "DocumentProcessing",
                        "Falha no processamento",
                        f"Arquivo temporário não encontrado",
                        document_id
                    ))
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
                    # Notifica sobre arquivo muito grande
                    if user_id:
                        asyncio.run(self.create_notification(
                            user_id,
                            "DocumentProcessing",
                            "Falha no processamento",
                            f"Documento muito grande ({file_size // (1024*1024)}MB), excede o limite de 100MB",
                            document_id
                        ))
                    return False
            except Exception as size_error:
                logger.error(f"Erro ao verificar tamanho do arquivo do documento {document_id}: {str(size_error)}")
            
            # Extrai o texto
            text, summary = self.extract_text(temp_file_path)
            
            # Verifica se o texto contém mensagem de erro
            if text.startswith("ERRO NA EXTRAÇÃO DE TEXTO"):
                logger.error(f"Falha na extração de texto do documento {document_id}: {text}")
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                # Notifica sobre falha na extração
                if user_id:
                    asyncio.run(self.create_notification(
                        user_id,
                        "DocumentProcessing",
                        "Falha na extração de texto",
                        f"Não foi possível extrair texto do documento",
                        document_id
                    ))
                return False
            
            # Divide o texto em chunks e gera embeddings
            try:
                self.create_document_chunks(document_id, text)
            except Exception as chunk_error:
                logger.error(f"Erro ao criar chunks do documento {document_id}: {str(chunk_error)}")
                logger.error(traceback.format_exc())
                # Continua mesmo se houver erro nos chunks
            
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
                        # Notifica sobre falha na atualização
                        if user_id:
                            asyncio.run(self.create_notification(
                                user_id,
                                "DocumentProcessing",
                                "Falha no processamento",
                                f"Erro ao salvar texto extraído do documento",
                                document_id
                            ))
                        return False
            
            # Notifica sucesso no processamento
            if user_id:
                asyncio.run(self.create_notification(
                    user_id,
                    "DocumentProcessing",
                    "Processamento concluído",
                    f"Texto extraído com sucesso ({len(text)} caracteres)",
                    document_id
                ))
            
            logger.info(f"Documento {document_id} processado com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao processar documento {document_id}: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Atualiza o status para falha
            try:
                self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                # Notifica sobre erro geral
                if user_id:
                    asyncio.run(self.create_notification(
                        user_id,
                        "DocumentProcessing",
                        "Falha no processamento",
                        f"Erro inesperado durante o processamento do documento",
                        document_id
                    ))
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
    
    def split_text_into_chunks(self, text, chunk_size=1000, chunk_overlap=200):
        """Divide o texto em chunks usando estratégia recursiva baseada no LangChain."""
        if not text or len(text.strip()) == 0:
            return []
        
        # Separadores hierárquicos (do mais específico para o mais geral)
        separators = [
            "\n\n",  # Parágrafos
            "\n",    # Quebras de linha
            ". ",    # Sentenças
            "! ",    # Exclamações
            "? ",    # Perguntas
            "; ",    # Ponto e vírgula
            ", ",    # Vírgulas
            " ",     # Espaços
            ""       # Caracteres individuais
        ]
        
        chunks = []
        current_chunks = [text]
        
        for separator in separators:
            new_chunks = []
            
            for chunk in current_chunks:
                if len(chunk) <= chunk_size:
                    new_chunks.append(chunk)
                else:
                    # Divide usando o separador atual
                    if separator == "":
                        # Último recurso: divide por caracteres
                        split_chunks = [chunk[i:i+chunk_size] for i in range(0, len(chunk), chunk_size)]
                    else:
                        split_chunks = chunk.split(separator)
                    
                    # Reconstrói os chunks respeitando o tamanho máximo
                    current_chunk = ""
                    for split_chunk in split_chunks:
                        test_chunk = current_chunk + (separator if current_chunk else "") + split_chunk
                        
                        if len(test_chunk) <= chunk_size:
                            current_chunk = test_chunk
                        else:
                            if current_chunk:
                                new_chunks.append(current_chunk)
                            current_chunk = split_chunk
                    
                    if current_chunk:
                        new_chunks.append(current_chunk)
            
            current_chunks = new_chunks
            
            # Se todos os chunks estão dentro do tamanho, para
            if all(len(chunk) <= chunk_size for chunk in current_chunks):
                break
        
        # Aplica overlap se especificado
        if chunk_overlap > 0 and len(current_chunks) > 1:
            overlapped_chunks = []
            for i, chunk in enumerate(current_chunks):
                if i == 0:
                    overlapped_chunks.append(chunk)
                else:
                    # Adiciona overlap do chunk anterior
                    prev_chunk = current_chunks[i-1]
                    overlap_text = prev_chunk[-chunk_overlap:] if len(prev_chunk) > chunk_overlap else prev_chunk
                    overlapped_chunk = overlap_text + " " + chunk
                    overlapped_chunks.append(overlapped_chunk)
            current_chunks = overlapped_chunks
        
        # Remove chunks vazios e muito pequenos
        final_chunks = [chunk.strip() for chunk in current_chunks if chunk.strip() and len(chunk.strip()) > 10]
        
        return final_chunks
    
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
            return np.array(response.data[0].embedding).tolist()
        except Exception as e:
            logger.error(f"Erro ao gerar embedding OpenAI: {str(e)}")
            return None
    
    def generate_embedding_free(self, text):
        """Gera embedding gratuito usando sentence-transformers + padding (768 + 768 = 1536)."""
        try:
            # Gera embedding de 768 dimensões
            embedding_768 = self.embedding_model.encode(text, convert_to_tensor=False)
            
            # Adiciona padding de zeros para chegar a 1536 dimensões
            padding = np.zeros(768)
            embedding_1536 = np.concatenate([embedding_768, padding])
            
            return embedding_1536.tolist()
        except Exception as e:
            logger.error(f"Erro ao gerar embedding gratuito: {str(e)}")
            return np.zeros(1536).tolist()

    def generate_embedding(self, text, user_id=None):
        """Gera embedding de 1536 dimensões - OpenAI se disponível, senão gratuito com padding."""
        if not self.embedding_model:
            logger.error("Modelo de embeddings não está disponível")
            return None

        try:
            if not text or text.strip() == "":
                return np.zeros(1536).tolist()
            
            # Limita o tamanho do texto para evitar problemas de memória
            max_text_length = 8192  # Limite típico para modelos de embedding
            if len(text) > max_text_length:
                text = text[:max_text_length]
            
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
            return np.zeros(1536).tolist()
    
    def create_document_chunks(self, document_id, text):
        """Cria chunks do documento e salva no banco de dados com embeddings."""
        if not text or len(text.strip()) == 0:
            logger.warning(f"Texto vazio para documento {document_id}, pulando criação de chunks")
            return
        
        # Primeiro, obtém o user_id do documento
        user_id = None
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT uploaded_by_user_id FROM public.documents WHERE id = %s", (document_id,))
            result = cursor.fetchone()
            if result:
                user_id = result[0]
            cursor.close()
        except Exception as e:
            logger.error(f"Erro ao obter user_id do documento {document_id}: {str(e)}")

        try:
            # Divide o texto em chunks
            chunks = self.split_text_into_chunks(text, chunk_size=1000, chunk_overlap=200)
            
            if not chunks:
                logger.warning(f"Nenhum chunk gerado para documento {document_id}")
                return
            
            logger.info(f"Gerados {len(chunks)} chunks para documento {document_id}")
            
            # Remove chunks existentes do documento (se houver)
            cursor = self.db_conn.cursor()
            cursor.execute("DELETE FROM public.document_chunks WHERE document_id = %s", (document_id,))
            
            # Insere os novos chunks
            for chunk_index, chunk_content in enumerate(chunks):
                try:
                    # Gera embedding para o chunk usando a estratégia híbrida
                    embedding = self.generate_embedding(chunk_content, user_id)
                    
                    if embedding is None:
                        logger.warning(f"Não foi possível gerar embedding para chunk {chunk_index} do documento {document_id}")
                        # Insere sem embedding
                        cursor.execute(
                            """
                            INSERT INTO public.document_chunks (document_id, chunk_index, content, created_at)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (document_id, chunk_index, chunk_content, datetime.datetime.now())
                        )
                    else:
                        # Insere com embedding
                        cursor.execute(
                            """
                            INSERT INTO public.document_chunks (document_id, chunk_index, content, embedding, created_at)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (document_id, chunk_index, chunk_content, embedding, datetime.datetime.now())
                        )
                    
                    logger.debug(f"Chunk {chunk_index} inserido para documento {document_id} (usuário: {user_id})")
                    
                except Exception as chunk_error:
                    logger.error(f"Erro ao inserir chunk {chunk_index} do documento {document_id}: {str(chunk_error)}")
                    # Continua com os próximos chunks mesmo se um falhar
                    continue
            
            # Confirma as alterações
            self.db_conn.commit()
            cursor.close()
            
            logger.info(f"Chunks salvos com sucesso para documento {document_id}")
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Erro ao criar chunks para documento {document_id}: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def check_stuck_documents(self):
        """Verifica documentos que estão presos no status ExtractingText por muito tempo."""
        def _check_stuck_documents_operation():
            cursor = self.db_conn.cursor()
            
            # Calcula o tempo limite para considerar um documento como preso
            time_limit = datetime.datetime.now() - datetime.timedelta(minutes=self.max_extracting_time_minutes)
            
            # Busca documentos que estão no status ExtractingText por mais tempo que o limite
            query = """
            SELECT id, storage_path, file_type, COALESCE(original_file_name, processed_file_name) AS file_name
            FROM public.documents 
            WHERE status = %s AND updated_at < %s
            LIMIT 10
            """
            
            cursor.execute(query, (DocumentStatus.EXTRACTING_TEXT, time_limit))
            stuck_documents = cursor.fetchall()
            cursor.close()
            
            return stuck_documents
        
        try:
            stuck_documents = self.execute_with_retry(_check_stuck_documents_operation)
            
            if stuck_documents:
                logger.warning(f"Encontrados {len(stuck_documents)} documentos presos no status ExtractingText")
                
                for doc in stuck_documents:
                    document_id, storage_path, file_type, file_name = doc
                    logger.warning(f"Marcando documento {document_id} como falha (preso no status ExtractingText)")
                    
                    try:
                        self.update_document_status(document_id, DocumentStatus.TEXT_EXTRACTION_FAILED)
                        logger.info(f"Status do documento {document_id} atualizado para falha")
                        
                        # Criar notificação sobre documento preso
                        user_id = self.get_document_user_id(document_id)
                        if user_id:
                            asyncio.run(self.create_notification(
                                user_id=user_id,
                                title="Processamento Interrompido",
                                message=f"O processamento do documento '{file_name or 'documento'}' foi interrompido devido a timeout. Tente fazer o upload novamente.",
                                type="error"
                            ))
                        
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
                                
                                # Criar notificação sobre erro no processamento
                                user_id = self.get_document_user_id(document_id)
                                if user_id:
                                    asyncio.run(self.create_notification(
                                        user_id=user_id,
                                        title="Erro no Processamento",
                                        message=f"Ocorreu um erro durante o processamento do documento. Tente fazer o upload novamente.",
                                        type="error"
                                    ))
                                
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
                # Usa o método de reconexão robusto para o banco de dados
                try:
                    self._reconnect_db()
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