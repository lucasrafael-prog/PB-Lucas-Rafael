--Modelagem Dimensional

--Criando Views para representar o Fato e as Dimensões


----------------------------------------
--Dimensão Carro
DROP VIEW IF EXISTS Dim_Carro;
CREATE VIEW Dim_Carro AS
SELECT DISTINCT
	idCarro,
	classiCarro,
	marcaCarro,
	modeloCarro,
	anoCarro
FROM TbCarro

--Conferindo se a Dimensão esta Correta
SELECT * FROM Dim_Carro

----------------------------------------
--Dimensão KmCarro
DROP VIEW IF EXISTS Dim_KmCarro;
CREATE VIEW Dim_KmCarro AS
SELECT DISTINCT
	idCarro,
	kmCarro
FROM TbKmCarro

--Conferindo se a Dimensão esta Correta
SELECT * FROM Dim_KmCarro

----------------------------------------
--Dimensão Cliente
DROP VIEW IF EXISTS Dim_Cliente;
CREATE VIEW Dim_Cliente AS
SELECT DISTINCT
	idCliente,
	nomeCliente,
	cidadeCliente,
	estadoCliente,
	paisCliente
FROM TbCliente

--Conferindo se a Dimensão esta Correta
SELECT * FROM Dim_Cliente

----------------------------------------
--Dimensão Combustivel
DROP VIEW IF EXISTS Dim_Combustivel;
CREATE VIEW Dim_Combustivel AS
SELECT DISTINCT
	idCombustivel,
	tipoCombustivel
FROM TbCombustivel

--Conferindo se a Dimensão esta Correta
SELECT * FROM Dim_Combustivel


----------------------------------------
--Dimensão Vendedor
DROP VIEW IF EXISTS Dim_Vendedor;
CREATE VIEW Dim_Vendedor AS
SELECT DISTINCT
	idVendedor,
	nomeVendedor,
	case when sexoVendedor = 1 then 'F'
	when sexoVendedor = 0 then 'M'
	end as sexodoVendedor,
	estadoVendedor
FROM TbVendedor

--Conferindo se a Dimensão esta Correta
SELECT * FROM Dim_Vendedor

----------------------------------------
--Dimensão Data
DROP VIEW IF EXISTS Dim_Data;
CREATE VIEW Dim_Data AS
SELECT DISTINCT
	dataLocacao as DataLocacao,
	strftime('%d', dataLocacao) AS DiaLocacao,
    strftime('%m', dataLocacao) AS MesLocacao,
    strftime('%Y', dataLocacao) AS AnoLocacao,
    dataEntrega as DataEntrega,
    strftime('%d', dataEntrega) AS DiaEntrega,
    strftime('%m', dataEntrega) AS MesEntrega,
    strftime('%Y', dataEntrega) AS AnoEntrega
FROM TbLocacao
--Conferindo se a Dimensão esta Correta
SELECT * FROM Dim_Data

--Houve um problema no formato da data entao foi necessario dividir a string de data
-- utilizando update com substr

--Conferindo o formato das datas no BD
SELECT dataLocacao, typeof(dataLocacao), dataEntrega, typeof(dataEntrega)
FROM TbLocacao

-- Atualização da dataLocacao
UPDATE TbLocacao
SET dataLocacao = 
    substr(dataLocacao, 1, 4) || '-' || 
    substr(dataLocacao, 5, 2) || '-' || 
    substr(dataLocacao, 7, 2)
WHERE typeof(dataLocacao) = 'integer';

-- Atualização da dataEntrega
UPDATE TbLocacao
SET dataEntrega = 
    substr(dataEntrega, 1, 4) || '-' || 
    substr(dataEntrega, 5, 2) || '-' || 
    substr(dataEntrega, 7, 2)
WHERE typeof(dataEntrega) = 'integer';

--Novo teste da dimensão
SELECT * FROM Dim_Data

----------------------------------------
--Fato Locação
DROP VIEW IF EXISTS FatoLocacao;
CREATE VIEW FatoLocacao AS
SELECT DISTINCT
	idLocacao,
	qtdDiaria,
	vlrDiaria,
	dataLocacao,
	horaLocacao,
	dataEntrega,
	horaEntrega,
	idCliente,
	idCarro,
	idCombustivel,
	idVendedor
FROM TbLocacao

--Conferindo se o Fato esta Correto
SELECT * FROM FatoLocacao


--Aqui a modelagem dimensional está completa