select a.codautor, a.nome, count(l.cod) as quantidade_publicacoes
from autor as a
left join livro as l
on a.codautor = l.autor
group by a.nome
order by quantidade_publicacoes desc
limit 1
