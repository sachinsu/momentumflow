create database if not exists momentumflow;

create user if not exists  momentumflow password 'momentumflow';

grant all privileges on database momentumflow to momentumflow;

create table if not exists cnx500companies (
    company varchar(200),
    industry varchar(100),
    symbol varchar(20) primary key,
    ltp money, 
    yearlyhigh money,
    updatedat date default current_date 
);

create table if not exists momentumstocks (
    company varchar(200),
    symbol varchar(20),
    ltp money, 
    buyorsell varchar(5),
    updatedat date default current_date,

    primary key (symbol, updatedat)
);

