/*[create_table]*/
create table if not exists test.sqlc
(
    id integer
);

/*[create_table2]*/
create table test.sqlc1
(
    id integer
);

/*[insert_data]*/
insert into test.sqlc1(id)
values (10);

/*[query_region]*/
select *
from test.region
limit 3;