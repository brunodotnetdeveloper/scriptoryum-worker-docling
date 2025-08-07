# Guia de Migração: Docling para Pytesseract

## Mudanças Realizadas

### 1. Substituição da Biblioteca Principal
- **Antes**: Docling para extração de texto
- **Depois**: Pytesseract + PyMuPDF para OCR e processamento de documentos

### 2. Novas Dependências
As seguintes bibliotecas foram adicionadas ao `requirements.txt`:
- `pytesseract` - OCR (Optical Character Recognition)
- `Pillow` - Processamento de imagens
- `PyMuPDF` - Processamento de PDFs
- `opencv-python` - Processamento avançado de imagens
- `numpy` - Arrays numéricos

### 3. Configuração do Tesseract
Uma nova variável de ambiente foi adicionada:
- `TESSERACT_PATH` - Caminho para o executável do Tesseract (opcional)

### 4. Funcionalidades Implementadas

#### Extração de Texto de Imagens
- Suporte para PNG, JPG, JPEG, TIFF, BMP, GIF
- OCR em português e inglês (`lang='por+eng'`)
- Conversão automática para RGB quando necessário

#### Extração de Texto de PDFs
- Extração direta de texto quando disponível
- OCR automático para páginas com pouco ou nenhum texto
- Processamento página por página com identificação clara

#### Arquivos de Texto
- Suporte para arquivos de texto simples
- Tentativa de múltiplas codificações (UTF-8, Latin-1)

## Instalação do Tesseract

### Windows
1. Baixe o instalador do Tesseract: https://github.com/UB-Mannheim/tesseract/wiki
2. Instale o Tesseract
3. Configure a variável `TESSERACT_PATH` no arquivo `.env`:
   ```
   TESSERACT_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe
   ```

### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install tesseract-ocr tesseract-ocr-por
```

### macOS
```bash
brew install tesseract tesseract-lang
```

## Como Usar

1. Instale as novas dependências:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure o Tesseract (se necessário) no arquivo `.env`

3. Execute o worker normalmente:
   ```bash
   python worker.py
   ```

## Vantagens da Nova Implementação

1. **Melhor OCR**: Tesseract é uma das melhores engines de OCR disponíveis
2. **Suporte Multilíngue**: Configurado para português e inglês
3. **Processamento Híbrido**: Combina extração direta de texto com OCR quando necessário
4. **Menor Dependência**: Menos bibliotecas complexas
5. **Maior Controle**: Mais controle sobre o processo de extração

## Limitações

1. **Requer Tesseract**: Necessário instalar o Tesseract OCR no sistema
2. **Qualidade da Imagem**: A qualidade do OCR depende da qualidade da imagem
3. **Processamento Mais Lento**: OCR pode ser mais lento que extração direta de texto