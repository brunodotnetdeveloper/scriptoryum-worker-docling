import os
import sys
import time
import logging
import subprocess
import signal
import atexit
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('scriptoryum_runner')

# Carrega variáveis de ambiente
load_dotenv()

# Lista de processos em execução
processes = []

def start_process(script_name, process_name):
    """Inicia um processo Python."""
    try:
        logger.info(f"Iniciando {process_name}...")
        process = subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        processes.append((process, process_name))
        logger.info(f"{process_name} iniciado com PID {process.pid}")
        return process
    except Exception as e:
        logger.error(f"Erro ao iniciar {process_name}: {str(e)}")
        return None

def stop_all_processes():
    """Para todos os processos em execução."""
    logger.info("Parando todos os processos...")
    
    for process, name in processes:
        try:
            logger.info(f"Enviando sinal de término para {name} (PID {process.pid})")
            if os.name == 'nt':  # Windows
                process.terminate()
            else:  # Unix/Linux
                os.kill(process.pid, signal.SIGTERM)
            
            # Aguarda até 5 segundos para o processo terminar
            for _ in range(5):
                if process.poll() is not None:
                    break
                time.sleep(1)
            
            # Se o processo ainda estiver em execução, força o encerramento
            if process.poll() is None:
                logger.warning(f"{name} não respondeu ao sinal de término, forçando encerramento")
                process.kill()
            
            logger.info(f"{name} encerrado")
        except Exception as e:
            logger.error(f"Erro ao encerrar {name}: {str(e)}")

def monitor_processes():
    """Monitora os processos em execução e reinicia se necessário."""
    while True:
        for i, (process, name) in enumerate(processes):
            # Verifica se o processo ainda está em execução
            if process.poll() is not None:
                exit_code = process.poll()
                logger.warning(f"{name} encerrou com código {exit_code}, reiniciando...")
                
                # Coleta saídas do processo encerrado
                stdout, stderr = process.communicate()
                if stdout:
                    logger.info(f"Últimas saídas de {name}:\n{stdout}")
                if stderr:
                    logger.error(f"Últimos erros de {name}:\n{stderr}")
                
                # Reinicia o processo
                script_name = name.lower().replace(' ', '_') + '.py'
                new_process = start_process(script_name, name)
                
                # Atualiza a lista de processos
                if new_process:
                    processes[i] = (new_process, name)
        
        # Aguarda antes da próxima verificação
        time.sleep(5)

def main():
    """Função principal para iniciar todos os componentes."""
    logger.info("Iniciando sistema Scriptoryum...")
    
    # Registra função para parar todos os processos ao encerrar
    atexit.register(stop_all_processes)
    
    # Inicia o worker principal
    start_process('worker.py', 'Document Worker')
    
    # Aguarda um pouco para garantir que o worker principal esteja em execução
    time.sleep(2)
    
    # Inicia o gerador de embeddings
    start_process('embedding_generator.py', 'Embedding Generator')
    
    # Inicia o monitor de processos
    try:
        monitor_processes()
    except KeyboardInterrupt:
        logger.info("Interrupção de teclado detectada, encerrando...")
    finally:
        stop_all_processes()

if __name__ == "__main__":
    main()