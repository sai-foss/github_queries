


ATTACH '/home/sai/Study/thesis/database/tpch_SF_1.duckdb' AS ddb;

ATTACH '/home/sai/Study/thesis/database/tpch_SF_10.duckdb' AS ddb;


ATTACH '/home/sai/Study/thesis/database/tpch_SF_100.duckdb' AS ddb;


USE ddb;  



show all tables



-------- wide joins
EXPLAIN ANALYZE
select
	o_year,
	sum(case
		when nation = 'BRAZIL' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY ANODIZED STEEL'
	) as all_nations
group by
	o_year
order by
	o_year;





-- distinct counts
EXPLAIN ANALYZE
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#45'
	and p_type not like 'MEDIUM POLISHED%'
	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;












--- TOP-N query 
EXPLAIN ANALYZE
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 300
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100;








-- sessionization ---> our own creation for this dataset as TPC-H does not have one by default

EXPLAIN ANALYZE
WITH customer_day_session AS (
    SELECT
        o.o_custkey,
        o.o_orderdate AS session_date,
        COUNT(DISTINCT o.o_orderkey) AS orders_in_session,
        COUNT(*) AS pages_visited_proxy,
        COUNT(DISTINCT l.l_partkey) AS products_seen,
        MAX(CASE WHEN o.o_orderstatus = 'F' THEN 1 ELSE 0 END) AS has_fulfilled_order
    FROM orders o
    JOIN lineitem l
      ON l.l_orderkey = o.o_orderkey
    GROUP BY
        o.o_custkey,
        o.o_orderdate
)
SELECT
    AVG(pages_visited_proxy) AS avg_pages_visited_proxy,
    AVG(products_seen)       AS avg_distinct_products_proxy
FROM customer_day_session
WHERE has_fulfilled_order = 0;

