-- Normalização das tabelas
-- 1° passo: Aplicação da 1FN(Primeira Forma Normal)

--Fazendo consultas para analisar o banco e verificar se há grupos repetitivos
select distinct * from tb_locacao

select distinct idLocacao, qtdDiaria, vlrDiaria, dataLocacao, horaLocacao, dataEntrega, horaEntrega from tb_locacao

select distinct idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente from tb_locacao

select distinct idCarro, kmCarro, classiCarro, marcaCarro, modeloCarro, anoCarro from tb_locacao
--Percebo que há repetição nessa consulta mesmo com o uso do Distinct
-- sendo causada pela coluna kmCarro, é necessário que separe em uma nova tabela durante remoção de dependências 

select DISTINCT idcombustivel, tipoCombustivel from tb_locacao

select distinct idVendedor, nomeVendedor, sexoVendedor, estadoVendedor from tb_locacao



-- 2° passo: Aplicação da 2FN(Segunda Forma Normal)
--Remover dependências parciais

--Criando novas tabelas para separar  as entidades
-- Tabela Cliente
DROP TABLE IF EXISTS TbCliente;
CREATE TABLE TbCliente (
	idCliente int PRIMARY KEY,
	nomeCliente varchar(100),
	cidadeCliente varchar(40),
	estadoCliente varchar(40),
	paisCliente varchar(40)	
)

--Tabela Carro
DROP Table if Exists TbCarro;
CREATE TABLE TbCarro (
	idCarro int PRIMARY KEY,
	classiCarro varchar(50),
	marcaCarro varchar(80),
	modeloCarro varchar(80),
	anoCarro int
)

--Tabela KM Carro
DROP Table if exists TbKmCarro;
CREATE TABLE TbKmCarro(
	idCarro int,
	kmCarro int,
	Foreign Key (idCarro) References TbCarro(idCarro)
)


--Tabela Combustivel
DROP TABLE IF EXISTS TbCombustivel;
CREATE TABLE TbCombustivel (
	idCombustivel int PRIMARY KEY,
	tipoCombustivel varchar(20)
)

--Tabela Vendedor
DROP TABLE IF EXISTS TbVendedor;
CREATE TABLE TbVendedor (
	idVendedor int PRIMARY KEY,
	nomeVendedor varchar(15),
	sexoVendedor smallint,
	estadoVendedor varchar(40)
)

--Inserção dos dados nas tabelas

--Tabela Cliente
INSERT INTO TbCliente (idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente)
SELECT DISTINCT idCliente, nomeCliente, cidadeCliente, estadoCliente, paisCliente
from tb_locacao 


--Conferindo se os dados estão corretos
SELECT * from TbCliente


--Tabela Carro
INSERT INTO TbCarro (idCarro, classiCarro, marcaCarro, modeloCarro, anoCarro)
SELECT Distinct idCarro, classiCarro, marcaCarro, modeloCarro, anoCarro
from tb_locacao 

--Conferindo se os dados estão corretos
SELECT * FROM TbCarro


--Tabela Km Carro
INSERT INTO TbKmCarro (idCarro, kmCarro)
SELECT idCarro, kmCarro
from tb_locacao

--Conferindo se os dados estão corretos
SELECT * FROM TbKmCarro

-- Select com as duas Tabelas(Carro e KmCarro) 
SELECT * from TbCarro as c
left join TbKmCarro as k
on c.idCarro = k.idCarro


--Tabela Combustivel
INSERT INTO TbCombustivel (idCombustivel, tipoCombustivel)
SELECT distinct idcombustivel, tipoCombustivel 
from tb_locacao

--Conferindo se os dados estão corretos
SELECT * FROM TbCombustivel


--Tabela Vendedor
INSERT INTO TbVendedor (idVendedor, nomeVendedor, sexoVendedor, estadoVendedor)
SELECT DISTINCT idVendedor, nomeVendedor, sexoVendedor, estadoVendedor 
from tb_locacao

--Conferindo se os dados estão corretos
SELECT * FROM TbVendedor



-- 3° passo: Aplicando a 3FN(Terceira Forma Normal)
-- Eliminando dependências transitivas

-- Como ja foram separadas as dependências parciais (2FN), 
-- criarei uma nova tabela Locação para eliminar as colunas desnecessárias

DROP TABLE IF EXISTS TbLocacao;
CREATE TABLE TbLocacao (
	idLocacao int PRIMARY KEY,
	idCliente int,
	idCarro int,
	idCombustivel int,
	idVendedor int,
	dataLocacao DATETIME,
	horaLocacao TIME,
	qtdDiaria int,
	vlrDiaria decimal(18,2),
	dataEntrega DATE,
	horaEntrega TIME,
	FOREIGN KEY (idCliente) REFERENCES TbCliente(idCliente),
	FOREIGN KEY (idCarro) REFERENCES TbCarro(idCarro),
	FOREIGN KEY (idCombustivel) REFERENCES TbCombustivel(idCombustivel),
	FOREIGN KEY (idVendedor) REFERENCES TbVendedor(idVendedor)
)

--Inserindo os dados na nova tabela de Locação
INSERT INTO TbLocacao (idLocacao, idCliente, idCarro, idCombustivel, idVendedor, dataLocacao, horaLocacao,
qtdDiaria, vlrDiaria, dataEntrega, horaEntrega)
SELECT DISTINCT idLocacao, idCliente, idCarro, idcombustivel, idVendedor, dataLocacao, horaLocacao,
qtdDiaria, vlrDiaria, dataEntrega, horaEntrega 
FROM tb_locacao

--Conferindo se os dados estão corretos
SELECT * FROM TbLocacao

--Se necessário exclua a tabela original
DROP TABLE IF EXISTS tb_locacao 

--Aqui a normalização está completa

