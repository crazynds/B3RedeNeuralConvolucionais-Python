drop table acao_historico_inputs;
truncate acao_historico_inputs;

create table acao_historico_inputs(
    id bigint primary key AUTO_INCREMENT,
    stock_name varchar(10) not null,
    trading_date date not null,
    
    day1_price_open DECIMAL(9,2) not null,
    day1_price_min DECIMAL(9,2) not null,
    day1_price_max DECIMAL(9,2) not null,
    day1_price_avg DECIMAL(9,2) not null,
    day1_price_close DECIMAL(9,2) not null,
    
    day2_price_open DECIMAL(9,2) not null,
    day2_price_min DECIMAL(9,2) not null,
    day2_price_max DECIMAL(9,2) not null,
    day2_price_avg DECIMAL(9,2) not null,
    day2_price_close DECIMAL(9,2) not null,
    
    day3_price_open DECIMAL(9,2) not null,
    day3_price_min DECIMAL(9,2) not null,
    day3_price_max DECIMAL(9,2) not null,
    day3_price_avg DECIMAL(9,2) not null,
    day3_price_close DECIMAL(9,2) not null,
    
    day4_price_open DECIMAL(9,2) not null,
    day4_price_min DECIMAL(9,2) not null,
    day4_price_max DECIMAL(9,2) not null,
    day4_price_avg DECIMAL(9,2) not null,
    day4_price_close DECIMAL(9,2) not null,
    
    day5_price_open DECIMAL(9,2) not null,
    day5_price_min DECIMAL(9,2) not null,
    day5_price_max DECIMAL(9,2) not null,
    day5_price_avg DECIMAL(9,2) not null,
    day5_price_close DECIMAL(9,2) not null,
    
    day6_price_open DECIMAL(9,2) not null,
    day6_price_min DECIMAL(9,2) not null,
    day6_price_max DECIMAL(9,2) not null,
    day6_price_avg DECIMAL(9,2) not null,
    day6_price_close DECIMAL(9,2) not null,
    
    day7_price_open DECIMAL(9,2) not null,
    day7_price_min DECIMAL(9,2) not null,
    day7_price_max DECIMAL(9,2) not null,
    day7_price_avg DECIMAL(9,2) not null,
    day7_price_close DECIMAL(9,2) not null,
    
    day8_price_open DECIMAL(9,2) not null,
    day8_price_min DECIMAL(9,2) not null,
    day8_price_max DECIMAL(9,2) not null,
    day8_price_avg DECIMAL(9,2) not null,
    day8_price_close DECIMAL(9,2) not null,
    
    day9_price_open DECIMAL(9,2) not null,
    day9_price_min DECIMAL(9,2) not null,
    day9_price_max DECIMAL(9,2) not null,
    day9_price_avg DECIMAL(9,2) not null,
    day9_price_close DECIMAL(9,2) not null,
    
    global_price_open DECIMAL(9,2) not null,
    global_price_min DECIMAL(9,2) not null,
    global_price_max DECIMAL(9,2) not null,
    global_price_avg DECIMAL(9,2) not null,
    global_price_close DECIMAL(9,2) not null,
    
    period_variation_avg DECIMAL(7,2) not null,
    period_variation_open DECIMAL(7,2) not null,
    period_variation_close DECIMAL(7,2) not null,
    
	unique(stock_name, trading_date)
);

select * from acao_historico_inputs;

call prepare_acao_historico_train('OIBR3');

drop procedure prepare_acao_historico_train;

DELIMITER $$
CREATE PROCEDURE prepare_acao_historico_train (IN stock_name_parameter VARCHAR(10))
BEGIN
	DECLARE finished INTEGER DEFAULT 0;
	
    DECLARE id_val BIGINT DEFAULT 0;
	DECLARE trading_date_val date;
    
	DECLARE counter_itens INTEGER default 0;
    DECLARE global_price_sum DECIMAL(9,2) DEFAULT 0;
    

    DECLARE period_variation_avg DECIMAL(7,2);
    DECLARE period_variation_open DECIMAL(7,2);
    DECLARE period_variation_close DECIMAL(7,2);
    
    DECLARE day_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day_price_min DECIMAL(9,2);
    DECLARE day_price_max DECIMAL(9,2);
    DECLARE day_price_avg DECIMAL(9,2);
    DECLARE day_price_close DECIMAL(9,2);
    
    DECLARE day0_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day0_price_min DECIMAL(9,2);
    DECLARE day0_price_max DECIMAL(9,2);
    DECLARE day0_price_avg DECIMAL(9,2);
    DECLARE day0_price_close DECIMAL(9,2);
    
    DECLARE day1_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day1_price_min DECIMAL(9,2);
    DECLARE day1_price_max DECIMAL(9,2);
    DECLARE day1_price_avg DECIMAL(9,2);
    DECLARE day1_price_close DECIMAL(9,2);
    
    DECLARE day2_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day2_price_min DECIMAL(9,2);
    DECLARE day2_price_max DECIMAL(9,2);
    DECLARE day2_price_avg DECIMAL(9,2);
    DECLARE day2_price_close DECIMAL(9,2);
    
    DECLARE day3_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day3_price_min DECIMAL(9,2);
    DECLARE day3_price_max DECIMAL(9,2);
    DECLARE day3_price_avg DECIMAL(9,2);
    DECLARE day3_price_close DECIMAL(9,2);
    
    DECLARE day4_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day4_price_min DECIMAL(9,2);
    DECLARE day4_price_max DECIMAL(9,2);
    DECLARE day4_price_avg DECIMAL(9,2);
    DECLARE day4_price_close DECIMAL(9,2);
    
    DECLARE day5_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day5_price_min DECIMAL(9,2);
    DECLARE day5_price_max DECIMAL(9,2);
    DECLARE day5_price_avg DECIMAL(9,2);
    DECLARE day5_price_close DECIMAL(9,2);
    
    DECLARE day6_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day6_price_min DECIMAL(9,2);
    DECLARE day6_price_max DECIMAL(9,2);
    DECLARE day6_price_avg DECIMAL(9,2);
    DECLARE day6_price_close DECIMAL(9,2);
    
    DECLARE day7_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day7_price_min DECIMAL(9,2);
    DECLARE day7_price_max DECIMAL(9,2);
    DECLARE day7_price_avg DECIMAL(9,2);
    DECLARE day7_price_close DECIMAL(9,2);
    
    DECLARE day8_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day8_price_min DECIMAL(9,2);
    DECLARE day8_price_max DECIMAL(9,2);
    DECLARE day8_price_avg DECIMAL(9,2);
    DECLARE day8_price_close DECIMAL(9,2);
    
    DECLARE day9_price_open DECIMAL(9,2) DEFAULT -1;
    DECLARE day9_price_min DECIMAL(9,2);
    DECLARE day9_price_max DECIMAL(9,2);
    DECLARE day9_price_avg DECIMAL(9,2);
    DECLARE day9_price_close DECIMAL(9,2);
    
    DECLARE global_price_open DECIMAL(9,2) DEFAULT 0;
    DECLARE global_price_min DECIMAL(9,2) DEFAULT 0;
    DECLARE global_price_max DECIMAL(9,2) DEFAULT 0;
    DECLARE global_price_avg DECIMAL(9,2) DEFAULT 0;
    DECLARE global_price_close DECIMAL(9,2) DEFAULT 0;
    
    
	DECLARE curs CURSOR FOR 
		select id,trading_date,price_open,price_avg,price_last_deal,price_best_buy_offer,price_best_sell_offer
        from acao_historico where stock_name=stock_name_parameter order by trading_date;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;
    
    OPEN curs;
    SET finished = 0;
	FETCH curs INTO id_val,trading_date_val,day_price_open,day_price_avg,day_price_close,day_price_min,day_price_max;
	SET global_price_min = day_price_min;
	SET global_price_max = day_price_max;
    
    WHILE finished=0 DO
		IF(day9_price_open!=-1)THEN
        
			SET period_variation_avg = (day_price_avg/day1_price_avg -1)*100;
			SET period_variation_open = (day_price_open/day1_price_open -1)*100;
			SET period_variation_close = (day_price_close/day1_price_close -1)*100;
        
			INSERT INTO acao_historico_inputs 
			VALUES (default,stock_name_parameter,trading_date_val,
			day1_price_open,day1_price_min,day1_price_max,day1_price_avg,day1_price_close,
			day2_price_open,day2_price_min,day2_price_max,day2_price_avg,day2_price_close,
			day3_price_open,day3_price_min,day3_price_max,day3_price_avg,day3_price_close,
			day4_price_open,day4_price_min,day4_price_max,day4_price_avg,day4_price_close,
			day5_price_open,day5_price_min,day5_price_max,day5_price_avg,day5_price_close,
			day6_price_open,day6_price_min,day6_price_max,day6_price_avg,day6_price_close,
			day7_price_open,day7_price_min,day7_price_max,day7_price_avg,day7_price_close,
			day8_price_open,day8_price_min,day8_price_max,day8_price_avg,day8_price_close,
			day9_price_open,day9_price_min,day9_price_max,day9_price_avg,day9_price_close,
            0,global_price_min,global_price_max,global_price_avg,0,
            period_variation_avg,period_variation_open,period_variation_close
			);
		END IF;

	
        
		SET day9_price_open = day8_price_open;
		SET day9_price_min = day8_price_min;
		SET day9_price_max = day8_price_max;
		SET day9_price_avg = day8_price_avg;
		SET day9_price_close = day8_price_close;

		SET day8_price_open = day7_price_open;
		SET day8_price_min = day7_price_min;
		SET day8_price_max = day7_price_max;
		SET day8_price_avg = day7_price_avg;
		SET day8_price_close = day7_price_close;

		SET day7_price_open = day6_price_open;
		SET day7_price_min = day6_price_min;
		SET day7_price_max = day6_price_max;
		SET day7_price_avg = day6_price_avg;
		SET day7_price_close = day6_price_close;

		SET day6_price_open = day5_price_open;
		SET day6_price_min = day5_price_min;
		SET day6_price_max = day5_price_max;
		SET day6_price_avg = day5_price_avg;
		SET day6_price_close = day5_price_close;

		SET day5_price_open = day4_price_open;
		SET day5_price_min = day4_price_min;
		SET day5_price_max = day4_price_max;
		SET day5_price_avg = day4_price_avg;
		SET day5_price_close = day4_price_close;

		SET day4_price_open = day3_price_open;
		SET day4_price_min = day3_price_min;
		SET day4_price_max = day3_price_max;
		SET day4_price_avg = day3_price_avg;
		SET day4_price_close = day3_price_close;

		SET day3_price_open = day2_price_open;
		SET day3_price_min = day2_price_min;
		SET day3_price_max = day2_price_max;
		SET day3_price_avg = day2_price_avg;
		SET day3_price_close = day2_price_close;
		
		SET day2_price_open = day1_price_open;
		SET day2_price_min = day1_price_min;
		SET day2_price_max = day1_price_max;
		SET day2_price_avg = day1_price_avg;
		SET day2_price_close = day1_price_close;
        
		SET day1_price_open = day0_price_open;
		SET day1_price_min = day0_price_min;
		SET day1_price_max = day0_price_max;
		SET day1_price_avg = day0_price_avg;
		SET day1_price_close = day0_price_close;
        
		SET day0_price_open = day_price_open;
		SET day0_price_min = day_price_min;
		SET day0_price_max = day_price_max;
		SET day0_price_avg = day_price_avg;
		SET day0_price_close = day_price_close;
        

		-- para cada iten ele passa por esse processo
		SET counter_itens = counter_itens+1;
		SET global_price_sum = global_price_sum+day_price_avg;
		IF (day_price_min<global_price_min) THEN
			SET global_price_min = day_price_min;
		END IF;
		IF (day_price_max>global_price_min) THEN
			SET global_price_max = day_price_max;
		END IF;
		SET global_price_avg = global_price_sum/counter_itens;
		FETCH curs INTO id_val,trading_date_val,day_price_open,day_price_avg,day_price_close,day_price_min,day_price_max;
    END WHILE;
    CLOSE curs;

    
END$$
DELIMITER ;
