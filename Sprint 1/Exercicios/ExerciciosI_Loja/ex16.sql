select  estado, nmpro, round(avg(qtd), 4) as quantidade_media from tbvendas
where status = 'Concluído'
group by estado, nmpro
order by estado, nmpro