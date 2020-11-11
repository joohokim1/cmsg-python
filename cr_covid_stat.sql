create table covid_stat (
        base_date varchar(100),
        location varchar(20),
        total_confirmed_cnt int,
        daily_confirmed_cnt int,
        local_cnt int,
        inflow_cnt int,
        isolation_cnt int,
        release_cnt int,
        dead_cnt int,
        ratio_100k double,
        primary key (base_date, location)
);
