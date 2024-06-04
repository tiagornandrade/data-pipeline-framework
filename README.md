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
from core.utils.create_pipeline import Pipeline
from core.operators import SourceStage, TransformStage, SinkStage

# Definindo estágios do pipeline
source = SourceStage(...)
transform = TransformStage(...)
sink = SinkStage(...)

# Criando o pipeline
pipeline = Pipeline(source, transform, sink)

# Executando o pipeline
pipeline.run()
```

Consulte a documentação completa para mais informações sobre como usar o framework.

## Contribuindo

Se você encontrar problemas ou tiver sugestões de melhorias, sinta-se à vontade para abrir uma issue ou enviar um pull request neste repositório.

## Licença

Este projeto está licenciado sob a Licença MIT. Consulte o arquivo LICENSE para obter mais detalhes.
