# python-sphinx-example

Este projeto é um exmplo de como utilizar o [sphinx](https://www.sphinx-doc.org/en/master/) para gerar documentação de forma automática em projetos python.

----

- Para mais detalhes [veja este tutorial](https://medium.com/@david.morosinineto/documenta%C3%A7%C3%A3o-em-python-com-sphinx-73c5c627b811) no medium.
- O diretório `docs_correct/` contém a configuração correta para gerar a documentação deste repositório (É necessário criar o ambiente virtual e instalar as depêndencias apenas)

## Configurando o ambiente

- Clonar este repositório (Ou preparar o próprio projeto)
  
    ```bash
    $ git clone https://github.com/davidmorosini/python-sphinx-example.git
    ```

- Acessar o diretório raiz do projeto

    ```bash
    $ cd python-sphinx-example
    ```

- Criar um ambiente virtual python e ativar (Neste exemplo vou utilizar o virtualenv padrão do python, mas poderíamos utilizar o [conda](https://www.anaconda.com/) como base)
  
    ```bash
    $ virtualenv venv
    $ source venv/bin/activate
    ```

- Instalar todas as bibliotecas necessárias para executar o projeto (Incluindo a dependência do `sphinx`)
  
    ```bash
    (venv) $ pip install -r requirements.txt
    ```

- Vamos criar um diretório para concentrar todos os itens da documentação
  
    ```bash
    (venv) $ make
    ```
    - Neste exemplo será criado um diretório chamado `docs`
    - Conclua o formulário de inicialização do `sphinx-quickstart`

- Vá até o diretório criado `docs`

    ```bash
    (venv) $ cd docs
    ```

- Realize as seguintes alterações no arquivo `conf.py`
  
    ```python
    # Descomente estas linhas e altere o diretório abaixo para o atual
    import os
    import sys
    sys.path.insert(0, os.path.abspath('../../'))


    # Inclua os itens abaixo na lista `extensions`
    extensions = [
        'sphinx.ext.autodoc',
        'sphinx.ext.ifconfig',
        'sphinx.ext.viewcode',
        'sphinx.ext.githubpages',
    ]

    # Logo após a lista `extensions` insira a linha abaixo
    master_doc = 'index'
    ```

- Substitua o conteúdo do arquivo `index.rst`
  
    ```
    .. example documentation master file, created by
    sphinx-quickstart on Sun Aug  2 18:21:27 2020.
    You can adapt this file completely to your liking, but it should at least
    contain the root `toctree` directive.

    Bem vindo a Documentacao (voce pode customizar isso)
    ====================================================

    .. toctree::
        :maxdepth: 2
        :caption: Contents:

        modules/example_sql_dag
        modules/airflow_utils

    Indices and tables
    ==================

    * :ref:`genindex`
    * :ref:`modindex`
    * :ref:`search`
    ```

- Crie o diretório `modules` e crie os arquivos abaixo dentro deste diretório
  
    - `airflow_utils.rst`
        ```
        PostgresDict Operator
        =====================

        .. toctree::
            :maxdepth: 4
            :caption: Contents:

        Init
        ====

        .. automodule:: airflow_utils.postgres_dict
            :members:
        ```

    - `example_sql_dag.rst`
        ```
        SQL Exec Module
        ===============

        .. toctree::
            :maxdepth: 4
            :caption: Contents:

        Init
        ====

        .. automodule:: airflow_dags.example_sql_dag.__init__
            :members:

        Utils
        =====

        .. automodule:: airflow_dags.example_sql_dag.utils.__init__
            :members:
        ```

- Volte até o diretório `docs/` e execute o comando

    ```bash
    (venv) $ make html
    ```

### Finish!

A documentação em formato html estará no diretório `docs/build/html`
