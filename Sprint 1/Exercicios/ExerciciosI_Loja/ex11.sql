select cdcli, nmcli, SUM(vrunt * qtd) as gasto FROM tbvendas
where status = 'Concluído'
GROUP by nmcli
order by gasto desc 
limit 1
