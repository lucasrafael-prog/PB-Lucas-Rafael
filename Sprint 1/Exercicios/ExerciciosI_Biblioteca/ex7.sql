select a.nome from autor as a
left join livro as l
on a.codautor = l.autor
where l.titulo is null
group by a.nome
order by a.nome
