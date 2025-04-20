select vdd.nmvdd as vendedor, sum(ven.qtd * ven.vrunt) as valor_total_vendas,
round(sum((ven.qtd * ven.vrunt) *  vdd.perccomissao / 100.0), 2) as comissao
from tbvendas as ven
left join tbvendedor as vdd
on vdd.cdvdd = ven.cdvdd
where ven.status = 'Concluído' 
group by vendedor 
order by comissao desc
