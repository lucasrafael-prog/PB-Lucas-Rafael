select estado, round(avg(vrunt * qtd), 2) as gastomedio from tbvendas
where status = 'Concluído'
group by estado
order by gastomedio desc