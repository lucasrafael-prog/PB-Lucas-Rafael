select vdd.cdvdd, vdd.nmvdd, count(ven.cdven) from tbvendedor as vdd 
left join tbvendas as ven
on vdd.cdvdd = ven.cdvdd
where ven.status = ('Concluído') 
group by vdd.nmvdd
order by count(ven.cdven) desc
limit 1

