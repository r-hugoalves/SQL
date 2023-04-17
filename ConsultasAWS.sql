/* CRIANDO A TABELA DE CLIENTES */

CREATE EXTERNAL TABLE clientes(
  id BIGINT, 
  idade BIGINT, 
  sexo STRING, 
  dependentes BIGINT, 
  escolaridade STRING, 
  tipo_cartao STRING, 
  limite_credito DOUBLE, 
  valor_transacoes_12m DOUBLE, 
  qtd_transacoes_12m BIGINT) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://XXXXX/'

/* TRABALHANDO COM A TABELA clientes*/

SELECT * FROM clientes;
SELECT id, idade, limite_credito FROM clientes WHERE sexo = 'M' ORDER BY idade DESC;
SELECT sexo, AVG(idade) AS "media_idade_por_sexo" FROM clientes GROUP BY sexo;
SELECT id, valor_transacoes_12m FROM clientes WHERE escolaridade = 'mestrado' and sexo = 'F';
SELECT sexo, AVG(idade) AS "media_idade_por_sexo" FROM clientes GROUP BY sexo;

/* TRABALHANDO COM PARTIÇÕES*/

CREATE EXTERNAL TABLE clientes_part(
  id BIGINT, 
  idade BIGINT, 
  dependentes BIGINT, 
  escolaridade STRING, 
  tipo_cartao STRING, 
  limite_credito DOUBLE, 
  valor_transacoes_12m DOUBLE, 
  qtd_transacoes_12m BIGINT) 
  PARTITIONED BY (sexo string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://partitioned/';

MSCK REPAIR TABLE clientes_part;

/* MANIPULANDO AS PARTIÇÕES */

SELECT * FROM clientes_part WHERE sexo = 'F';
SELECT id, idade, limite_credito FROM clientes_part WHERE sexo = 'M' ORDER BY limite_credito DESC;

/* ADICIONANDO COLUNAS */

ALTER TABLE clientes ADD COLUMNS (estado STRING)

/* CRIANDO UMA NOVA TABELA */

CREATE TABLE transacoes (
  id_cliente INT,   
  id_transacao INT PRIMARY KEY,
  data_compra DATE UNIQUE,
  valor FLOAT NOT NULL, 
  id_loja varchar(25),
  FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
  CHECK (valor > 0) 
);

/* INSERINDO DADOS DIRETAMENTE NA TABELA */

INSERT INTO transacoes VALUES (1,768805383,2021-06-10,50.74,'magalu');
INSERT INTO transacoes VALUES (2,768805399,2021-06-13,30.90,'giraffas');
INSERT INTO transacoes VALUES (3,818770008,2021-06-05,110.00,'postoshell');
INSERT INTO transacoes VALUES (1,76856563,2021-07-10,2000.90,'magalu');
INSERT INTO transacoes VALUES (1,767573759,2021-06-20,15.70,'subway');
INSERT INTO transacoes VALUES (3,818575758,2021-06-25,2.99,'seveneleven');
INSERT INTO transacoes VALUES (4,764545534,2021-07-11,50.74,'extra');
INSERT INTO transacoes VALUES (5,76766789,2021-08-02,10.00,'subway');
INSERT INTO transacoes VALUES (3,8154567758,2021-08-15,1100.00,'shopee');

/* CRIANDO UMA NOVA TABELA */

CREATE EXTERNAL TABLE transacoes(
  id_cliente BIGINT, 
  id_transacao BIGINT,
  valor FLOAT,
  id_loja STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://bucket-transacoes/'
/* */
SELECT DISTINCT  id_loja AS nome_loja FROM transacoes;
SELECT id_cliente, valor FROM transacoes ORDER BY valor DESC LIMIT 2;

/* SELEÇÃO DE DADOS */

SELECT *
FROM transacoes
WHERE valor > 30 AND id_loja = 'magalu';

SELECT *
FROM transacoes
WHERE valor > 30 OR id_loja = 'magalu';

SELECT *
FROM transacoes
WHERE id_loja IN ('magalu','subway') AND valor > 10;

SELECT *
FROM transacoes
WHERE valor BETWEEN 60.0 AND 1000.0;

SELECT * 
FROM transacoes 
WHERE id_loja LIKE 'mag%'

/* SELEÇÃO CONDICIONAL */

SELECT * 
FROM transacoes 
WHERE id_loja LIKE '%sh%'

SELECT id_cliente, id_loja, valor,
CASE
    WHEN valor > 1000 THEN 'Compra com alto valor'
    WHEN valor < 1000 THEN 'Compra com baixo valor'
END 
AS classeValor, 
CASE
    WHEN id_loja IN ('giraffas','subway')  THEN 'alimentacao'
    WHEN id_loja IN ('magalu','extra') THEN 'variedade'
    WHEN id_loja IN ('postoshell','seveneleven') THEN '24horas'
    ELSE 'outros'
END 
AS tipo_compra
FROM transacoes;

/* CRIANDO UMA NOVA TABELA */

CREATE EXTERNAL TABLE IF NOT EXISTS default.heartattack (
  `age` int,
  `sex` int,
  `cp` int,
  `trtbps` int,
  `chol` int,
  `fbs` int,
  `restecg` int,
  `thalachh` int,
  `exng` int,
  `oldpeak` double,
  `slp` int,
  `caa` int,
  `thall` int,
  `output` int 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://XXXXXX/'
TBLPROPERTIES ('has_encrypted_data'='false');

/* TRABALHANDO COM FILTROS */

SELECT * FROM heartattack limit 10;

SELECT COUNT(age) AS QUANTIDADE_LINHAS
FROM heartattack;

SELECT COUNT(age) AS QUANTIDADE, 
CASE
WHEN output =1 THEN ' more chance of heart attack'
ELSE 'less chance of heart attack'
END AS output
FROM heartattack
GROUP BY output;

SELECT MAX(age), MIN(age), AVG(age), output  
FROM heartattack
GROUP BY output

SELECT MAX(age), MIN(age), AVG(age), output ,sex
FROM heartattack
GROUP BY output, sex;

SELECT COUNT(output), output, sex 
FROM heartattack
GROUP BY output, sex
having COUNT(output) > 25

/* TRABALHANDO COM MAIS DE UMA TABELA */

SELECT id_cliente FROM transacoes
UNION
SELECT id_cliente  FROM cliente;

SELECT transacoes.id_cliente, cliente.nome
FROM transacoes
INNER JOIN cliente
ON transacoes.id_cliente = cliente.id_cliente;

SELECT *
FROM cliente
CROSS JOIN transacoes;

SELECT *
FROM transacoes
LEFT JOIN cliente 
ON cliente.id_cliente = transacoes.id_cliente;

SELECT *
FROM transacoes
RIGHT JOIN cliente 
ON cliente.id_cliente = transacoes.id_cliente;