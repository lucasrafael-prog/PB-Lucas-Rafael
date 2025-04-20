--Exportar o resultado da query que obtém as 5 editoras com maior quantidade de livros na biblioteca para um arquivo CSV. 
--Utilizar o caractere (pipe) como separador. 
--Lembre-se que o conteúdo do seu arquivo deverá respeitar a sequência de colunas e seus respectivos nomes de cabeçalho que listamos abaixo:
--CodEditora NometEditora QuantidadeLivros
select edi.codeditora as CodEditora, edi.nome as NomeEditora, count(*) as QuantidadeLivros 
FROM livro as liv
left join editora as edi
on liv.editora = edi.codeditora
join endereco as end
on end.codendereco = edi.endereco
group by edi.nome, end.estado, end.cidade
order by QuantidadeLivros desc
limit 5

