# Data Lake com Python + DuckDB + Parquet


## Características

- Armazenamento eficiente com formato Parquet
- Processamento rápido com DuckDB
- Interface simples e moderna
- Baixo custo de implementação
- Suporte a diferentes formatos de entrada (pandas e polars)
- Particionamento de dados
- Consultas SQL nativas

## Fluxo de Dados

### 1. Ingestão de Dados
O sistema aceita dados em dois formatos principais:
- DataFrames do pandas
- DataFrames do polars

Exemplo de ingestão:
```python
# Criando dados de exemplo
data = pd.DataFrame({
    'id': range(1, 6),
    'nome': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'idade': [25, 30, 35, 40, 45],
    'data_criacao': pd.date_range(start='2023-01-01', periods=5)
})

# Ingestão com particionamento
file_path = dl.ingest_data(data, 'usuarios', partition_by='data_criacao')
```

### 2. Armazenamento
Os dados são armazenados seguindo este processo:
1. Conversão automática para pandas (se necessário)
2. Geração de timestamp para versionamento
3. Salvamento em formato Parquet
4. Opcionalmente, particionamento por coluna específica

Estrutura de armazenamento:
```
data/
├── usuarios_20240101_120000.parquet
├── usuarios_20240102_150000.parquet
└── ...
```

### 3. Consulta e Análise
Os dados podem ser consultados usando SQL através do DuckDB:
```python
# Exemplo de consulta
query = """
SELECT nome, idade 
FROM usuarios 
WHERE idade > 30
"""
result = dl.query_data(query)
```

## Requisitos

- Python 3.8+
- Dependências listadas em `requirements.txt`

## Instalação

1. Clone este repositório
2. Crie um ambiente virtual:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
.\venv\Scripts\activate  # Windows
```
3. Instale as dependências:
```bash
pip install -r requirements.txt
```

## Uso

Execute o script principal:
```bash
python data_lake.py
```

## Estrutura do Projeto

- `data_lake.py`: Script principal com a implementação do Data Lake
- `data/`: Diretório para armazenar os dados
- `requirements.txt`: Dependências do projeto
- `README.md`: Documentação do projeto

## Funcionalidades

- Ingestão de dados
- Armazenamento em formato Parquet
- Consultas SQL com DuckDB
- Análise de dados
- Particionamento de dados
- Versionamento automático
- Logging de operações

## Exemplo Completo

```python
from data_lake import DataLake
import pandas as pd

# Inicialização
dl = DataLake()

# Dados de exemplo
data = pd.DataFrame({
    'id': range(1, 6),
    'nome': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'idade': [25, 30, 35, 40, 45],
    'data_criacao': pd.date_range(start='2023-01-01', periods=5)
})

# 1. Ingestão
file_path = dl.ingest_data(data, 'usuarios', partition_by='data_criacao')

# 2. Registro no DuckDB
dl.register_parquet_file(file_path, 'usuarios')

# 3. Consulta
result = dl.query_data("""
    SELECT nome, idade 
    FROM usuarios 
    WHERE idade > 30
""") 