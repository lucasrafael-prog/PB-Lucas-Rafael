select dep.cddep, dep.nmdep, dep.dtnasc, sum(ven.vrunt * ven.qtd) as valor_total_vendas from tbdependente as dep
left join tbvendedor as vdd
on dep.cdvdd = vdd.cdvdd 
left join tbvendas as ven
on vdd.cdvdd = ven.cdvdd
where status = 'Concluído' and ven.qtd > 0 
group by dep.nmdep, vdd.nmvdd
order by valor_total_vendas
limit 1
