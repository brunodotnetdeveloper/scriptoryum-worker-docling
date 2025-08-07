# Configuração do Tesseract no Docker

## ✅ Configuração Atual

O Dockerfile foi atualizado para incluir automaticamente o Tesseract OCR e todas as suas dependências necessárias.

### Pacotes Instalados no Container:

#### 🔍 **Tesseract OCR**
- `tesseract-ocr` - Engine principal do Tesseract
- `tesseract-ocr-por` - Suporte para português
- `tesseract-ocr-eng` - Suporte para inglês
- `libtesseract-dev` - Bibliotecas de desenvolvimento

#### 🖼️ **Dependências para Processamento de Imagens**
- `libgl1-mesa-glx` - OpenGL para OpenCV
- `libglib2.0-0` - Biblioteca GLib
- `libsm6` - Session Management
- `libxext6` - X11 Extension
- `libxrender-dev` - X Render Extension
- `libgomp1` - GNU OpenMP

#### 🛠️ **Dependências de Compilação**
- `gcc` - Compilador C
- `python3-dev` - Headers do Python
- `libpq-dev` - PostgreSQL development headers

## 🚀 Como Usar

### 1. **Build do Container**
```bash
# Build simples
docker build -t scriptoryum-worker .

# Build com logs detalhados
docker build -t scriptoryum-worker . --progress=plain
```

### 2. **Executar com Docker Compose**
```bash
# Executar apenas o worker
docker-compose up worker

# Executar todos os serviços
docker-compose -f docker-compose.full.yml up
```

### 3. **Verificar Instalação do Tesseract**
```bash
# Entrar no container
docker exec -it <container_name> bash

# Verificar versão do Tesseract
tesseract --version

# Listar idiomas disponíveis
tesseract --list-langs
```

## 🔧 Configuração Automática

### **Variáveis de Ambiente**
No container Docker, **NÃO é necessário** configurar a variável `TESSERACT_PATH` pois:
- O Tesseract é instalado no PATH padrão do sistema
- O pytesseract encontra automaticamente o executável

### **Arquivo .env para Docker**
```env
# Outras configurações...
DB_HOST=postgres
REDIS_CONNECTION_STRING=redis://redis:6379

# TESSERACT_PATH não é necessário no Docker
# TESSERACT_PATH=  # Deixe vazio ou remova esta linha
```

## 🐛 Troubleshooting

### **Erro: "tesseract is not installed"**
```bash
# Rebuild o container
docker-compose build --no-cache worker
```

### **Erro: "Failed to load language"**
```bash
# Verificar idiomas instalados
docker exec -it <container> tesseract --list-langs

# Deve mostrar:
# List of available languages (3):
# eng
# osd
# por
```

### **Erro de OpenCV/Imagem**
As dependências `libgl1-mesa-glx`, `libglib2.0-0`, etc. foram adicionadas para resolver problemas comuns do OpenCV em containers.

## 📊 Recursos do Container

### **Limites de Memória (docker-compose.full.yml)**
- `document-worker`: 1GB
- `embedding-generator`: 2GB
- `system-monitor`: 256MB
- `process-manager`: 512MB

### **Otimizações**
- Cache de layers do Docker otimizado
- Limpeza automática do apt cache
- Verificação automática da instalação do Tesseract

## 🔄 Rebuild Necessário

Após as mudanças no Dockerfile, é necessário fazer rebuild:

```bash
# Parar containers existentes
docker-compose down

# Rebuild sem cache
docker-compose build --no-cache

# Iniciar novamente
docker-compose up
```

## ✨ Vantagens da Configuração Docker

1. **Ambiente Consistente**: Mesma versão do Tesseract em todos os ambientes
2. **Dependências Isoladas**: Não interfere com o sistema host
3. **Fácil Deploy**: Basta fazer build e executar
4. **Suporte Multilíngue**: Português e inglês pré-configurados
5. **Otimizado**: Todas as dependências necessárias incluídas