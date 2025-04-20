select a.nome, a.codautor, a.nascimento, count(l.cod) as quantidade
from autor as a
left join livro as l
on a.codautor = l.autor
group by a.nome, a.codautor
order by a.nome

