#!/usr/bin/env python3
"""
Script para executar apenas o reprocessador de documentos.
Este script pode ser usado para executar o reprocessador de forma independente.
"""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv
from document_reprocessor import DocumentReprocessor

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('reprocessor_runner')

def main():
    """Função principal para executar o reprocessador."""
    parser = argparse.ArgumentParser(
        description='Reprocessador de documentos com falha na extração de texto'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=30,
        help='Intervalo em minutos entre verificações (padrão: 30)'
    )
    
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Número máximo de tentativas por documento (padrão: 3)'
    )
    
    parser.add_argument(
        '--run-once',
        action='store_true',
        help='Executa apenas um ciclo de reprocessamento e sai'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Ativa logs detalhados (DEBUG)'
    )
    
    args = parser.parse_args()
    
    # Configura nível de log se verbose
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Modo verbose ativado")
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Verifica se as variáveis de ambiente necessárias estão definidas
    required_env_vars = [
        'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_PORT',
        'REDIS_CONNECTION_STRING', 'REDIS_QUEUE'
    ]
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Variáveis de ambiente obrigatórias não definidas: {', '.join(missing_vars)}")
        logger.error("Verifique o arquivo .env")
        sys.exit(1)
    
    try:
        logger.info(f"Iniciando reprocessador com configurações:")
        logger.info(f"  - Intervalo: {args.interval} minutos")
        logger.info(f"  - Máximo de tentativas: {args.max_retries}")
        logger.info(f"  - Execução única: {'Sim' if args.run_once else 'Não'}")
        
        # Cria instância do reprocessador
        reprocessor = DocumentReprocessor(
            check_interval_minutes=args.interval,
            max_retry_attempts=args.max_retries
        )
        
        if args.run_once:
            # Executa apenas um ciclo
            logger.info("Executando ciclo único de reprocessamento...")
            stats = reprocessor.run_reprocessing_cycle()
            
            # Exibe estatísticas
            if stats.get('error'):
                logger.error(f"Erro no ciclo de reprocessamento: {stats['error']}")
                sys.exit(1)
            else:
                logger.info("Ciclo de reprocessamento concluído com sucesso!")
                logger.info(f"Estatísticas:")
                logger.info(f"  - Documentos encontrados: {stats['total_found']}")
                logger.info(f"  - Reprocessados com sucesso: {stats['reprocessed_successfully']}")
                logger.info(f"  - Falhas no reprocessamento: {stats['reprocessing_failed']}")
                logger.info(f"  - Marcados como permanentemente falhados: {stats['permanently_failed']}")
        else:
            # Executa em loop contínuo
            logger.info("Iniciando reprocessador em modo contínuo...")
            logger.info("Pressione Ctrl+C para parar")
            reprocessor.run()
            
    except KeyboardInterrupt:
        logger.info("Reprocessador interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal no reprocessador: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()