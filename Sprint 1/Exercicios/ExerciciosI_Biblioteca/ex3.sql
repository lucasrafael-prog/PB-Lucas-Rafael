select count(*) as quantidade , edi.nome, end.estado, end.cidade
FROM livro as liv
left join editora as edi
on liv.editora = edi.codeditora
join endereco as end
on end.codendereco = edi.endereco
group by edi.nome, end.estado, end.cidade
order by quantidade desc
limit 5

