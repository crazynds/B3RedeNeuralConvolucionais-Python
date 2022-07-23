
select a2.id,a2.stock_name,a2.trading_date,a2.bdi_code,a2.market_type,a1.* from acao_historico a1
join acao_historico a2 on a1.stock_name = a2.stock_name and a1.trading_date = a2.trading_date and a1.id!=a2.id and a1.id<a2.id
where a1.id = 489572
limit 100;

select * from acao_historico where stock_name = 'NUBR33'
order by price_min;

select stock_name,count(*) from acao_historico group by stock_name having count(*)<60;

select count(*) from acao_historico where stock_name in (
select stock_name from acao_historico group by stock_name having count(*)<48
);


select bdi_code, count(*) from acao_historico group by bdi_code;
delete from acao_historico where bdi_code in (5,7,8,10,12,13,14,22,74,75,78,82,96);

select stock_name.trading_date,prevStockName,prevDateNew,IF(prevStockName=stock_name,datediff(a1.trading_date,a1.prevHist),0)
from (
	select *,@prevStockName as prevStockName, @prevDateNew as prevHist,
    @prevDateNew:=trading_date,
    @prevStockName:=stock_name
    from acao_historico
    order by stock_name, trading_date
) a1
limit 1000;

truncate acao_historico;
drop table acao_historico;
create table acao_historico(
    id bigint primary key AUTO_INCREMENT,
    stock_name varchar(10) not null,
    trading_date date not null,
    bdi_code int,
    market_type int,
    
    price_open DECIMAL(9,2) not null,
    price_max DECIMAL(9,2) not null,
    price_min DECIMAL(9,2) not null,
    price_avg DECIMAL(9,2) not null,
    price_last_deal DECIMAL(9,2) not null,
    price_best_buy_offer DECIMAL(9,2) not null,
    price_best_sell_offer DECIMAL(9,2) not null,
    
    quantity_papers_negotiated bigint not null,
    number_trades bigint not null,
    unique(stock_name, trading_date)
);


