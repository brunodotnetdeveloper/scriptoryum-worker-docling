#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de teste para a funcionalidade de chunking de documentos.
Este script demonstra como o texto é dividido em chunks e como os embeddings são gerados.
"""

import os
import sys
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# Carrega variáveis de ambiente
load_dotenv()

def split_text_into_chunks(text, chunk_size=1000, chunk_overlap=200):
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

def test_chunking():
    """Testa a funcionalidade de chunking com um texto de exemplo."""
    
    # Texto de exemplo para teste
    sample_text = """
    A inteligência artificial (IA) é uma área da ciência da computação que se concentra no desenvolvimento de sistemas capazes de realizar tarefas que normalmente requerem inteligência humana.
    
    Essas tarefas incluem aprendizado, raciocínio, percepção, compreensão de linguagem natural e resolução de problemas. A IA tem suas raízes na filosofia, matemática, psicologia, linguística, neurociência e engenharia.
    
    Existem diferentes tipos de IA:
    
    1. IA Fraca (Narrow AI): Sistemas projetados para realizar tarefas específicas, como reconhecimento de voz ou jogos de xadrez.
    
    2. IA Forte (General AI): Sistemas que possuem capacidades cognitivas gerais comparáveis às dos humanos.
    
    3. Superinteligência: IA que supera a inteligência humana em todos os aspectos.
    
    As aplicações da IA são vastas e incluem:
    - Assistentes virtuais como Siri e Alexa
    - Sistemas de recomendação em plataformas como Netflix e Amazon
    - Carros autônomos
    - Diagnóstico médico assistido por computador
    - Tradução automática
    - Reconhecimento facial
    - Análise de sentimentos em redes sociais
    
    Os desafios éticos da IA incluem questões sobre privacidade, viés algorítmico, desemprego tecnológico e a necessidade de transparência nos sistemas de tomada de decisão.
    
    O futuro da IA promete avanços significativos em áreas como medicina personalizada, educação adaptativa, sustentabilidade ambiental e exploração espacial.
    """
    
    print("=== TESTE DE CHUNKING DE TEXTO ===")
    print(f"Texto original: {len(sample_text)} caracteres")
    print("\n" + "="*50 + "\n")
    
    # Testa diferentes configurações de chunking
    configurations = [
        {"chunk_size": 500, "chunk_overlap": 100, "name": "Chunks pequenos (500 chars, overlap 100)"},
        {"chunk_size": 1000, "chunk_overlap": 200, "name": "Chunks médios (1000 chars, overlap 200)"},
        {"chunk_size": 1500, "chunk_overlap": 300, "name": "Chunks grandes (1500 chars, overlap 300)"}
    ]
    
    for config in configurations:
        print(f"\n{config['name']}:")
        print("-" * 40)
        
        chunks = split_text_into_chunks(
            sample_text, 
            chunk_size=config['chunk_size'], 
            chunk_overlap=config['chunk_overlap']
        )
        
        print(f"Número de chunks gerados: {len(chunks)}")
        
        for i, chunk in enumerate(chunks):
            print(f"\nChunk {i+1} ({len(chunk)} chars):")
            print(f"'{chunk[:100]}{'...' if len(chunk) > 100 else ''}'")
    
    return chunks

def test_embeddings():
    """Testa a geração de embeddings."""
    print("\n\n=== TESTE DE GERAÇÃO DE EMBEDDINGS ===")
    
    try:
        # Carrega o modelo de embeddings
        print("Carregando modelo de embeddings...")
        model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')
        print("Modelo carregado com sucesso!")
        
        # Texto de teste
        test_text = "A inteligência artificial está revolucionando o mundo da tecnologia."
        
        print(f"\nTexto de teste: {test_text}")
        
        # Gera embedding
        embedding = model.encode(test_text, convert_to_tensor=False)
        embedding_list = embedding.tolist()
        
        print(f"Embedding gerado com {len(embedding_list)} dimensões")
        print(f"Primeiros 10 valores: {embedding_list[:10]}")
        print(f"Últimos 10 valores: {embedding_list[-10:]}")
        
        # Verifica se tem 768 dimensões
        if len(embedding_list) == 768:
            print("✅ Embedding tem o número correto de dimensões (768)")
        else:
            print(f"❌ Embedding tem {len(embedding_list)} dimensões, esperado 768")
            
    except Exception as e:
        print(f"❌ Erro ao testar embeddings: {str(e)}")
        print("Certifique-se de que o sentence-transformers está instalado:")
        print("pip install sentence-transformers")

def main():
    """Função principal do teste."""
    print("Iniciando testes de chunking e embeddings...\n")
    
    # Testa chunking
    chunks = test_chunking()
    
    # Testa embeddings
    test_embeddings()
    
    print("\n\n=== RESUMO DOS TESTES ===")
    print(f"✅ Chunking: {len(chunks)} chunks gerados com sucesso")
    print("✅ Embeddings: Teste concluído (verifique mensagens acima)")
    print("\n🚀 O sistema está pronto para processar documentos com chunking e embeddings!")

if __name__ == "__main__":
    main()