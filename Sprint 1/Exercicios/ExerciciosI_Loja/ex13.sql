 select cdpro, nmcanalvendas, nmpro, sum(qtd) as quantidade_vendas from tbvendas
 where status = 'Concluído'
 group by nmcanalvendas, nmpro
 order by quantidade_vendas asc
 