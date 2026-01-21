use lakeflow
GO

TRUNCATE TABLE dbo.orders;
TRUNCATE TABLE dbo.customers;

INSERT INTO customers(email, age, region, first_name, last_name)
VALUES ('john.doe@yahoo.com', 23, 'USA', 'John', 'Doe');
INSERT INTO customers(email, age, region, first_name, last_name)
VALUES ('jane.doe@gmail.com', 45, 'Europe', 'Jane', 'Doe');
INSERT INTO customers(email, age, region, first_name, last_name)
VALUES ('che.yang@info.com', 34, 'Asia', 'Che', 'Yang');
INSERT INTO customers(email, age, region, first_name, last_name)
VALUES ('denis.schmitt@gmx.com', 23, 'USA', 'Denis', 'Schmitt');

INSERT INTO lakeflow.dbo.orders (customer_id, amount, status, order_ts, discount)
VALUES (1, 99.90, N'refunded', N'2026-01-12 13:44:26.2840800', null);
INSERT INTO lakeflow.dbo.orders (customer_id, amount, status, order_ts, discount)
VALUES (2, 123.45, N'paid', N'2026-01-12 15:32:18.4474388', null);
INSERT INTO lakeflow.dbo.orders (customer_id, amount, status, order_ts, discount)
VALUES (3, 99.90, N'paid', N'2026-01-13 15:52:07.9819517', null);
INSERT INTO lakeflow.dbo.orders (customer_id, amount, status, order_ts, discount)
VALUES (1, 88.90, N'paid', N'2026-01-14 09:40:51.6032524', 0.23);
INSERT INTO lakeflow.dbo.orders (customer_id, amount, status, order_ts, discount)
VALUES (4, 188.90, N'ordered', N'2026-01-14 09:40:51.6032524', 0.13);


UPDATE dbo.orders
SET status = 'refunded'
WHERE order_id = 1;

/*DELETE
FROM dbo.orders
WHERE order_id = 1;
GO*/
