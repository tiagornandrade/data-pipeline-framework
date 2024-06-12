# Data Pipeline Framework

O Data Pipeline Framework é um framework Python para facilitar a criação, execução e gerenciamento de pipelines de dados.

## Visão Geral

Este framework fornece uma estrutura flexível e extensível para construir pipelines de dados que podem ser usados em uma variedade de cenários, desde a ingestão de dados até a transformação e carregamento em diferentes sistemas de armazenamento.

Principais recursos:

- Abstração de estágios de pipeline para fácil composição e reutilização.
- Suporte para diferentes tipos de estágios, incluindo fontes de dados, transformações e destinos.
- Extensibilidade para adicionar novos tipos de estágios e funcionalidades.
- Tratamento de erros e recuperação integrados.
- Fácil integração com outras bibliotecas e ferramentas de processamento de dados.

## Instalação

Para instalar o framework, basta usar pip:

```
pip install data-pipeline-framework
```

## Uso

Aqui está um exemplo simples de como criar e executar um pipeline de dados usando este framework:

```python
from src.pipeline import Pipeline
from src.operators import StageOperator, DimensionOperator
from src.db_manager import DBManager

# Configuração do DBManager
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'my_database',
    'user': 'username',
    'password': 'password'
}

def create_db_manager() -> DBManager:
    """Creates an instance of the DBManager."""
    return DBManager(**DB_CONFIG)

def create_user_pipeline(db_manager: DBManager) -> Pipeline:
    """Creates the user pipeline."""
    stage_op = StageOperator(table_name="stg_user", db_manager=db_manager)
    dim_op = DimensionOperator(table_name="dim_user", db_manager=db_manager)
    return Pipeline(name="UserPipeline", operators=[stage_op, dim_op])

# Criando o DBManager
db_manager = create_db_manager()

# Criando e executando o pipeline
user_pipeline = create_user_pipeline(db_manager)
user_pipeline.execute()
```

Consulte a documentação completa para mais informações sobre como usar o framework.

## Contribuindo

Se você encontrar problemas ou tiver sugestões de melhorias, sinta-se à vontade para abrir uma issue ou enviar um pull request neste repositório.

## Licença

Este projeto está licenciado sob a Licença MIT. Consulte o arquivo LICENSE para obter mais detalhes.
