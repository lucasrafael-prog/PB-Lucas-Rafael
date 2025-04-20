select distinct a.nome from autor as a
LEFT join livro as l
on a.codautor = l.autor
LEFT join editora as ed
on l.editora = ed.codeditora
LEFT join endereco as en
on ed.endereco = en.codendereco
where en.estado not in ('RIO GRANDE DO SUL','PARANÁ', 'SANTA CATARINA')
order by a.nome

