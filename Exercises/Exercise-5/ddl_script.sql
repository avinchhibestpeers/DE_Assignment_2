DROP table if exists accounts CASCADE;
create table if not exists accounts(
    customer_id int primary key,
    first_name varchar(200),
    last_name varchar(200),
    address_1 varchar(500),
    address_2 varchar(500),
    city varchar(200),
    state varchar(200),
    zip_code int,
    join_date date
);


drop index if exists add_join_date CASCADE;
create index if not exists add_join_date
on accounts (join_date, city, zip_code);


DROP table if EXISTS products CASCADE;
create table if not exists products(
    product_id int primary key,
    product_code int,
    product_description text
);

DROP table if exists transactions;
create table if not exists transactions(
    transaction_id varchar(30) primary key,
    transaction_date date,
    product_id int references products(product_id),
    product_code int,
    product_description text,
    quantity int,
    account_id int references accounts(customer_id)
);

drop index if exists transaction_index;
create index if not exists transaction_index
on transactions (transaction_date, product_code);
