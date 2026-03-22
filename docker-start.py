#!/usr/bin/env python3
"""
Script de inicialização para Docker
Permite executar diferentes configurações baseado em variáveis de ambiente
"""

import os
import sys
import subprocess
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Verificar variável de ambiente para modo de execução
    run_mode = os.getenv('RUN_MODE', 'full').lower()
    
    logger.info(f"Iniciando em modo: {run_mode}")
    
    if run_mode == 'worker-only':
        # Executar apenas o worker
        logger.info("Executando apenas o worker")
        subprocess.run([sys.executable, 'document_worker.py'])
        
    elif run_mode == 'reprocessor-only':
        # Executar apenas o reprocessador
        logger.info("Executando apenas o reprocessador")
        subprocess.run([sys.executable, 'run_reprocessor.py'])
        
    elif run_mode == 'monitor-only':
        # Executar apenas o monitor
        logger.info("Executando apenas o monitor")
        subprocess.run([sys.executable, 'monitor.py'])
        
    else:
        # Modo padrão: executar todos os serviços
        logger.info("Executando todos os serviços (worker, embedding generator, reprocessor)")
        subprocess.run([sys.executable, 'run_all.py'])

if __name__ == '__main__':
    main()