_Observações iniciais: esse teste deverá ser feito em 1 hora, no máximo; a interpretação das questões faz parte da avaliação, portanto não há esclarecimentos de dúvidas ao longo do teste; todos os arquivos usados na avaliação (arquivo .PY e arquivos .CSV) devem ser salvos numa pasta com o seu nome;_


# **Caso 1**

Desenvolva um script **Spark** com **Python (PySpark)** que faça a leitura do arquivo **data.csv**, disponibilizado juntamente com este documento. Em seguida:
- Crie um dataframe com tais dados;
- Adicione também uma coluna com a Ordem de cada linha;
- Re-ordene o dataframe, de forma ascendente por Município, Data de Atualização e Transação.

Após a criação do dataframe disponibilizar o resultado em um novo arquivo csv (com nome **data_ordem.csv**). Esse novo arquivo deve conter as seguintes colunas:

TRANSACAO, MUNICIPIO, ESTADO, DATA_ATUALIZACAO e ORDEM_ORIGINAL


# **Caso 2**

Desenvolva um script **Spark** com **Python (PySpark)** que faça a leitura dos arquivos **data.csv, usuário.csv e data_usuario.csv**, disponibilizados juntamente com este documento, relacione-os para obter a quantidade de transações por usuário.

Construa um dataframe e ordene de forma ascende por Contagem de usuário e Nome do Usuário e disponibilize o resultado em um novo arquivo csv que deve conter o seguinte formato:

COUNT, USUARIO


## **Descritivos técnicos sobre arquivos de Saída e Entrada:**


**Arquivos de Saída**

**Arquivo: data_ordem.csv**

**Formato:** csv

**Delimitador:** |

**Enconding:** UTF-8

_TRANSACAO 		    STRING_

_MUNICIPIO 			STRING_

_ESTADO 			STRING_

_DATA_ATUALIZACAO 	STRING_

_ORDEM_ORIGINAL		INT_


**Arquivo: cont_usuario.csv**

**Formato:** csv

**Delimitador:** |

**Enconding:** UTF-8

_COUNT	 		INT_

_USUARIO		STRING_



**Arquivos de entradas:**

**Arquivo: data.csv**

**Formato:** csv

**Delimitador:** |

**Enconding:** UTF-8

_TRANSACAO 		STRING_

_MUNICIPIO 		STRING_

_ESTADO 			STRING_

_DATA_ATUALIZACAO STRING_


**Arquivo: usuario.csv**

**Formato:** csv

**Delimitador:** |

**Enconding:** ANSI

_ID 			STRING_

_NOME 			STRING_

**Arquivo: data_usuario.csv**

**Formato:** csv

**Delimitador:** ;

**Enconding:** ANSI

_TRANSACAO			STRING_

_USUARIO 			STRING_