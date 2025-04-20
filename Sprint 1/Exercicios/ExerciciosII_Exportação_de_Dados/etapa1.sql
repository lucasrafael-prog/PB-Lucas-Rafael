--Exportar o resultado da query que obtém os 10 livros mais caros para um arquivo CSV. 
--Utilizar o caractere";" (ponto e virgula) como separador. 
--Lembre-se que o conteúdo do seu arquivo deverá respeitar a sequência de colunas e seus respectivos nomes de cabeçalho que listamos abaixo:
--CodLivro Titulo CodAutor NomeAutor Valor CodEditora Nome Editora
select l.cod as CodLivro, l.titulo as Titulo, a.codautor as CodAutor, a.nome as NomeAutor, l.valor as Valor,
e.codeditora as CodEditora, e.nome as NomeEditora
from livro as l
left join autor as a
on a.codautor = l.autor
left join editora as e
on e.codeditora = l.editora
order by valor desc
limit 10

