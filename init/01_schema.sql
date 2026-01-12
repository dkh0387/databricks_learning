-- Postgres
/*CREATE TABLE orders
(
    order_id    SERIAL PRIMARY KEY,
    customer_id INT            NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT           NOT NULL,
    order_ts    TIMESTAMP DEFAULT now()
);*/

-- SQL Server
CREATE DATABASE lakeflow;
GO

USE lakeflow;
GO

CREATE TABLE orders
(
    order_id    INT IDENTITY (1,1) PRIMARY KEY,
    customer_id INT            NOT NULL,
    amount      DECIMAL(10, 2) NOT NULL,
    status      VARCHAR(50)    NOT NULL,
    order_ts    DATETIME2 DEFAULT SYSDATETIME()
);
GO
