Projeto destinado a extração das tabelas de um database Ecommerce que está hospedado na cloud Azure.
- No total foram extraídos 8 tabelas, sendo elas: "customers", "employees", "offices", "orderdetails","orders","payments","product_lines" e "products".
-  Foram gerados respectivos arquivos csv's, contendo os dados das tabelas em questão.
-  Os arquivos foram depositados em uma landing_zone na Cloud Storage (GCP), sendo a primeira camada no datalake.
-  Foi projetaado para ser executado localmente.

A estrutura do projeto foi desenvolvida utilizando o paradigma de Orientação à Objetos.
-  Possui 4 diretórios, cada um com sua respectiva responsabilidades.
-  "conection", contém a classe responsável pela conexão com database.
-  "etl", contém a classe, responsável pela execução da query, construção dos dataframes e upload dos arquivos para a GCP.
-  "execution", contém o método "execution", onde foi instanciado o objeto da classe, leitura do yaml e chamada dos respectivos métodos.
-  "sql", contém a consulta sql genérica utilizada para a extração dos dados.
-  "config.yaml", contém a lista das tabelas, como também as colunas das respectivas tabelas. 

Tools used:
1. Python
2. PostgreSQl (In Azure)
3. Cloud Storage
