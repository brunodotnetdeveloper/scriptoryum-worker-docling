#!/bin/bash

# Script para iniciar o worker com reinicialização automática

echo "Iniciando Scriptoryum Document Processing Worker..."

# Número máximo de tentativas de reinicialização
MAX_RETRIES=5
retry_count=0
wait_time=10

while true; do
    echo "Iniciando worker (tentativa $((retry_count+1)))"
    
    # Executa o worker
    python worker.py
    
    # Verifica o código de saída
    exit_code=$?
    
    # Se o código de saída for 0 (saída normal) ou 130 (SIGINT), sai do loop
    if [ $exit_code -eq 0 ] || [ $exit_code -eq 130 ]; then
        echo "Worker encerrado normalmente com código $exit_code"
        break
    fi
    
    # Incrementa o contador de tentativas
    retry_count=$((retry_count+1))
    
    # Verifica se atingiu o número máximo de tentativas
    if [ $retry_count -ge $MAX_RETRIES ]; then
        echo "Número máximo de tentativas ($MAX_RETRIES) atingido. Encerrando."
        exit 1
    fi
    
    # Aguarda antes de reiniciar
    echo "Worker falhou com código $exit_code. Reiniciando em $wait_time segundos..."
    sleep $wait_time
    
    # Aumenta o tempo de espera para a próxima tentativa (backoff exponencial)
    wait_time=$((wait_time * 2))
done

echo "Worker encerrado."