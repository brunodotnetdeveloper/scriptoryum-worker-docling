#!/usr/bin/env python3
"""
Script de teste para o sistema de reprocessamento.
Este script permite testar e verificar o funcionamento do reprocessador.
"""

import os
import sys
import psycopg2
import datetime
from dotenv import load_dotenv
from reprocessor import DocumentReprocessor, DocumentStatus

# Carrega variáveis de ambiente
load_dotenv()

def test_database_connection():
    """Testa a conexão com o banco de dados."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"✅ Conexão com banco de dados OK: {version}")
        return True
    except Exception as e:
        print(f"❌ Erro na conexão com banco de dados: {str(e)}")
        return False

def check_required_columns():
    """Verifica se as colunas necessárias existem na tabela documents."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        cursor = conn.cursor()
        
        # Verifica se as colunas existem
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'documents' 
              AND table_schema = 'public'
              AND column_name IN ('retry_count', 'processing_notes')
        """)
        
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        required_columns = ['retry_count', 'processing_notes']
        missing_columns = [col for col in required_columns if col not in existing_columns]
        
        if missing_columns:
            print(f"❌ Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
            print("   Execute: psql -d seu_banco -f add_reprocessing_columns.sql")
            cursor.close()
            conn.close()
            return False
        else:
            print("✅ Todas as colunas necessárias estão presentes")
            cursor.close()
            conn.close()
            return True
            
    except Exception as e:
        print(f"❌ Erro ao verificar colunas: {str(e)}")
        return False

def get_document_statistics():
    """Obtém estatísticas dos documentos."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        cursor = conn.cursor()
        
        # Total de documentos
        cursor.execute("SELECT COUNT(*) FROM public.documents")
        total_docs = cursor.fetchone()[0]
        
        # Documentos por status
        cursor.execute("""
            SELECT status, COUNT(*) 
            FROM public.documents 
            GROUP BY status 
            ORDER BY COUNT(*) DESC
        """)
        status_counts = cursor.fetchall()
        
        # Documentos com falha por tentativas
        cursor.execute("""
            SELECT COALESCE(retry_count, 0) as retry_count, COUNT(*) 
            FROM public.documents 
            WHERE status = 'TextExtractionFailed'
            GROUP BY retry_count
            ORDER BY retry_count
        """)
        failed_by_retry = cursor.fetchall()
        
        # Documentos reprocessáveis
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.documents 
            WHERE status = 'TextExtractionFailed' 
              AND COALESCE(retry_count, 0) < 3
        """)
        reprocessable = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"\n📊 Estatísticas dos Documentos:")
        print(f"   Total de documentos: {total_docs}")
        print(f"   Documentos reprocessáveis: {reprocessable}")
        
        print(f"\n📈 Documentos por Status:")
        for status, count in status_counts:
            print(f"   {status}: {count}")
        
        if failed_by_retry:
            print(f"\n🔄 Falhas na Extração por Tentativas:")
            for retry_count, count in failed_by_retry:
                print(f"   {retry_count} tentativas: {count} documentos")
        
        return {
            'total': total_docs,
            'reprocessable': reprocessable,
            'status_counts': dict(status_counts),
            'failed_by_retry': dict(failed_by_retry)
        }
        
    except Exception as e:
        print(f"❌ Erro ao obter estatísticas: {str(e)}")
        return None

def create_test_failed_document():
    """Cria um documento de teste com falha para testar o reprocessamento."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        cursor = conn.cursor()
        
        # Insere um documento de teste
        cursor.execute("""
            INSERT INTO public.documents 
            (processed_file_name, storage_provider, storage_path, file_type, status, retry_count, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            'test_document.pdf',
            'cloudflare_r2',
            'test/path/document.pdf',
            'PDF',
            DocumentStatus.TEXT_EXTRACTION_FAILED,
            0,
            datetime.datetime.now() - datetime.timedelta(minutes=15)  # 15 minutos atrás
        ))
        
        doc_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Documento de teste criado com ID: {doc_id}")
        return doc_id
        
    except Exception as e:
        print(f"❌ Erro ao criar documento de teste: {str(e)}")
        return None

def test_reprocessor_instance():
    """Testa a criação de uma instância do reprocessador."""
    try:
        reprocessor = DocumentReprocessor(
            check_interval_minutes=1,  # 1 minuto para teste
            max_retry_attempts=2       # 2 tentativas para teste
        )
        print("✅ Instância do reprocessador criada com sucesso")
        return reprocessor
    except Exception as e:
        print(f"❌ Erro ao criar instância do reprocessador: {str(e)}")
        return None

def test_get_failed_documents(reprocessor):
    """Testa a busca por documentos com falha."""
    try:
        failed_docs = reprocessor.get_failed_documents(limit=5)
        print(f"✅ Busca por documentos com falha: {len(failed_docs)} encontrados")
        
        if failed_docs:
            print("   Documentos encontrados:")
            for doc_id, storage_path, file_type, retry_count, updated_at in failed_docs:
                print(f"   - ID: {doc_id}, Tentativas: {retry_count}, Atualizado: {updated_at}")
        
        return failed_docs
    except Exception as e:
        print(f"❌ Erro ao buscar documentos com falha: {str(e)}")
        return []

def main():
    """Função principal de teste."""
    print("🧪 Teste do Sistema de Reprocessamento\n")
    
    # Testa conexão com banco
    if not test_database_connection():
        sys.exit(1)
    
    # Verifica colunas necessárias
    if not check_required_columns():
        sys.exit(1)
    
    # Obtém estatísticas
    stats = get_document_statistics()
    if not stats:
        sys.exit(1)
    
    # Testa instância do reprocessador
    reprocessor = test_reprocessor_instance()
    if not reprocessor:
        sys.exit(1)
    
    # Testa busca por documentos com falha
    failed_docs = test_get_failed_documents(reprocessor)
    
    # Se não há documentos com falha, oferece criar um de teste
    if not failed_docs:
        print("\n❓ Nenhum documento com falha encontrado.")
        response = input("   Deseja criar um documento de teste? (s/n): ")
        if response.lower() in ['s', 'sim', 'y', 'yes']:
            test_doc_id = create_test_failed_document()
            if test_doc_id:
                print("\n🔄 Testando busca novamente...")
                failed_docs = test_get_failed_documents(reprocessor)
    
    # Oferece executar um ciclo de teste
    if failed_docs:
        print("\n❓ Documentos com falha encontrados.")
        response = input("   Deseja executar um ciclo de reprocessamento de teste? (s/n): ")
        if response.lower() in ['s', 'sim', 'y', 'yes']:
            print("\n🔄 Executando ciclo de teste...")
            try:
                stats = reprocessor.run_reprocessing_cycle()
                print(f"\n📊 Resultado do ciclo de teste:")
                print(f"   Documentos encontrados: {stats['total_found']}")
                print(f"   Reprocessados com sucesso: {stats['reprocessed_successfully']}")
                print(f"   Falhas no reprocessamento: {stats['reprocessing_failed']}")
                print(f"   Marcados como permanentemente falhados: {stats['permanently_failed']}")
                
                if stats.get('error'):
                    print(f"   ❌ Erro: {stats['error']}")
                else:
                    print("   ✅ Ciclo executado sem erros")
                    
            except Exception as e:
                print(f"❌ Erro durante ciclo de teste: {str(e)}")
    
    print("\n✅ Teste concluído!")
    print("\n💡 Para executar o reprocessador:")
    print("   - Modo contínuo: python run_reprocessor.py")
    print("   - Execução única: python run_reprocessor.py --run-once")
    print("   - Com sistema completo: python run_all.py")

if __name__ == "__main__":
    main()