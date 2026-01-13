use lakeflow
GO

INSERT INTO dbo.orders (customer_id, amount, status)
VALUES (1, 99.90, 'paid');

UPDATE dbo.orders
SET status = 'refunded'
WHERE order_id = 1;

/*DELETE
FROM dbo.orders
WHERE order_id = 1;
GO*/
