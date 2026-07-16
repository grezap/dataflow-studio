-- Representative order-flow seed for OltpDb. Idempotent: if the marker customer already exists the
-- whole batch returns without inserting. Foreign keys are resolved by natural-key subqueries, so no
-- IDENTITY values are hard-coded. Temporal (Customers/Products) + audit columns are managed by the
-- schema; the seed supplies only created_by/modified_by.

SET NOCOUNT ON;

IF EXISTS (SELECT 1 FROM dbo.Customers WHERE CustomerCode = 'SEED-C001')
BEGIN
    PRINT 'OltpDb already seeded (SEED-C001 present) — nothing to do.';
    RETURN;
END;

-- Product categories (2 top-level + 2 children).
INSERT dbo.ProductCategories (ParentId, Name, Slug, DisplayOrder, created_by, modified_by) VALUES
    (NULL, 'Electronics', 'electronics', 1, 'seed', 'seed'),
    (NULL, 'Home & Kitchen', 'home', 2, 'seed', 'seed');
INSERT dbo.ProductCategories (ParentId, Name, Slug, DisplayOrder, created_by, modified_by) VALUES
    ((SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'electronics'), 'Audio', 'audio', 1, 'seed', 'seed'),
    ((SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'home'), 'Kitchen', 'kitchen', 1, 'seed', 'seed');

-- Products (6 across the categories).
INSERT dbo.Products (Sku, CategoryId, DisplayName, Description, ListPriceUsd, Weight_g, Status, created_by, modified_by) VALUES
    ('SKU-AUD-001', (SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'audio'),       'Wireless Headphones', 'Over-ear ANC headphones', 199.9900, 250, 1, 'seed', 'seed'),
    ('SKU-AUD-002', (SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'audio'),       'Bluetooth Speaker',   'Portable speaker',        89.5000, 600, 1, 'seed', 'seed'),
    ('SKU-ELE-001', (SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'electronics'), 'USB-C Charger',       '65W GaN charger',         29.9900, 100, 1, 'seed', 'seed'),
    ('SKU-KIT-001', (SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'kitchen'),     'Chef Knife',          '8-inch steel knife',      59.0000, 300, 1, 'seed', 'seed'),
    ('SKU-KIT-002', (SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'kitchen'),     'Cutting Board',       'Bamboo board',            24.9900, 800, 1, 'seed', 'seed'),
    ('SKU-HOM-001', (SELECT CategoryId FROM dbo.ProductCategories WHERE Slug = 'home'),        'Table Lamp',          'LED desk lamp',           39.9900, 1200, 1, 'seed', 'seed');

-- Warehouses (3).
INSERT dbo.Warehouses (Code, Name, Region, CountryIso2, TimezoneIana, created_by, modified_by) VALUES
    ('WH-EAST', 'East DC', 'US-East',     'US', 'America/New_York',    'seed', 'seed'),
    ('WH-WEST', 'West DC', 'US-West',     'US', 'America/Los_Angeles', 'seed', 'seed'),
    ('WH-EU',   'EU DC',   'EU-Central',  'DE', 'Europe/Berlin',       'seed', 'seed');

-- Customers (4).
INSERT dbo.Customers (CustomerCode, DisplayName, Email, PhoneE164, PreferredLocale, Status, LifetimeValueUsd, created_by, modified_by) VALUES
    ('SEED-C001', 'Ada Lovelace',      'ada@example.com',       '+15551000001', 'en-US', 1, 318.18, 'seed', 'seed'),
    ('SEED-C002', 'Alan Turing',       'alan@example.com',      '+15551000002', 'en-GB', 1, 119.49, 'seed', 'seed'),
    ('SEED-C003', 'Grace Hopper',      'grace@example.com',     NULL,           'en-US', 1,  64.99, 'seed', 'seed'),
    ('SEED-C004', 'Katherine Johnson', 'katherine@example.com', NULL,           'en-US', 1,  44.99, 'seed', 'seed');

-- One default address per customer (used as billing + shipping).
INSERT dbo.CustomerAddresses (CustomerId, AddressType, Line1, City, Region, PostalCode, CountryIso2, IsDefault, created_by, modified_by)
SELECT CustomerId, 1, '1 Analytical Way', 'London',     NULL,    'EC1A 1AA', 'GB', 1, 'seed', 'seed' FROM dbo.Customers WHERE CustomerCode = 'SEED-C001'
UNION ALL SELECT CustomerId, 1, '2 Enigma Road',    'Manchester', NULL,    'M1 1AA',   'GB', 1, 'seed', 'seed' FROM dbo.Customers WHERE CustomerCode = 'SEED-C002'
UNION ALL SELECT CustomerId, 1, '3 Compiler Ave',   'New York',   'NY',    '10001',    'US', 1, 'seed', 'seed' FROM dbo.Customers WHERE CustomerCode = 'SEED-C003'
UNION ALL SELECT CustomerId, 1, '4 Orbit Street',   'Hampton',    'VA',    '23666',    'US', 1, 'seed', 'seed' FROM dbo.Customers WHERE CustomerCode = 'SEED-C004';

-- Inventory: every product in every warehouse.
INSERT dbo.ProductInventory (ProductId, WarehouseId, OnHand, Reserved, ReorderPoint, SafetyStock, created_by, modified_by)
SELECT p.ProductId, w.WarehouseId, 100, 5, 20, 10, 'seed', 'seed'
FROM dbo.Products p CROSS JOIN dbo.Warehouses w
WHERE p.Sku LIKE 'SKU-%' AND w.Code IN ('WH-EAST', 'WH-WEST', 'WH-EU');

-- Orders (4), each billing = shipping = the customer's default address.
INSERT dbo.Orders (OrderNumber, CustomerId, BillingAddressId, ShippingAddressId, PlacedAtUtc, Status, SubtotalUsd, TaxUsd, ShippingUsd, TotalUsd, Currency, created_by, modified_by)
SELECT 'SEED-ORD-0001', c.CustomerId, a.AddressId, a.AddressId, '2026-07-01T10:00:00', 4, 289.98, 23.20, 5.00, 318.18, 'USD', 'seed', 'seed'
    FROM dbo.Customers c JOIN dbo.CustomerAddresses a ON a.CustomerId = c.CustomerId WHERE c.CustomerCode = 'SEED-C001'
UNION ALL SELECT 'SEED-ORD-0002', c.CustomerId, a.AddressId, a.AddressId, '2026-07-02T11:30:00', 3, 113.99, 5.50, 0.00, 119.49, 'USD', 'seed', 'seed'
    FROM dbo.Customers c JOIN dbo.CustomerAddresses a ON a.CustomerId = c.CustomerId WHERE c.CustomerCode = 'SEED-C002'
UNION ALL SELECT 'SEED-ORD-0003', c.CustomerId, a.AddressId, a.AddressId, '2026-07-03T09:15:00', 2, 59.00, 5.99, 0.00, 64.99, 'USD', 'seed', 'seed'
    FROM dbo.Customers c JOIN dbo.CustomerAddresses a ON a.CustomerId = c.CustomerId WHERE c.CustomerCode = 'SEED-C003'
UNION ALL SELECT 'SEED-ORD-0004', c.CustomerId, a.AddressId, a.AddressId, '2026-07-04T16:45:00', 1, 39.99, 5.00, 0.00, 44.99, 'USD', 'seed', 'seed'
    FROM dbo.Customers c JOIN dbo.CustomerAddresses a ON a.CustomerId = c.CustomerId WHERE c.CustomerCode = 'SEED-C004';

-- Order lines (natural-key resolution for order/product/warehouse).
INSERT dbo.OrderLines (OrderId, ProductId, WarehouseId, Quantity, UnitPriceUsd, DiscountUsd, created_by, modified_by)
SELECT o.OrderId, p.ProductId, w.WarehouseId, 1, 199.9900, 0, 'seed', 'seed'
    FROM dbo.Orders o, dbo.Products p, dbo.Warehouses w WHERE o.OrderNumber = 'SEED-ORD-0001' AND p.Sku = 'SKU-AUD-001' AND w.Code = 'WH-EAST'
UNION ALL SELECT o.OrderId, p.ProductId, w.WarehouseId, 1, 89.5000, 0, 'seed', 'seed'
    FROM dbo.Orders o, dbo.Products p, dbo.Warehouses w WHERE o.OrderNumber = 'SEED-ORD-0001' AND p.Sku = 'SKU-AUD-002' AND w.Code = 'WH-EAST'
UNION ALL SELECT o.OrderId, p.ProductId, w.WarehouseId, 1, 89.5000, 0, 'seed', 'seed'
    FROM dbo.Orders o, dbo.Products p, dbo.Warehouses w WHERE o.OrderNumber = 'SEED-ORD-0002' AND p.Sku = 'SKU-AUD-002' AND w.Code = 'WH-WEST'
UNION ALL SELECT o.OrderId, p.ProductId, w.WarehouseId, 1, 24.9900, 0, 'seed', 'seed'
    FROM dbo.Orders o, dbo.Products p, dbo.Warehouses w WHERE o.OrderNumber = 'SEED-ORD-0002' AND p.Sku = 'SKU-KIT-002' AND w.Code = 'WH-WEST'
UNION ALL SELECT o.OrderId, p.ProductId, w.WarehouseId, 1, 59.0000, 0, 'seed', 'seed'
    FROM dbo.Orders o, dbo.Products p, dbo.Warehouses w WHERE o.OrderNumber = 'SEED-ORD-0003' AND p.Sku = 'SKU-KIT-001' AND w.Code = 'WH-EU'
UNION ALL SELECT o.OrderId, p.ProductId, w.WarehouseId, 1, 39.9900, 0, 'seed', 'seed'
    FROM dbo.Orders o, dbo.Products p, dbo.Warehouses w WHERE o.OrderNumber = 'SEED-ORD-0004' AND p.Sku = 'SKU-HOM-001' AND w.Code = 'WH-EAST';

-- Payment transactions (one settled capture per order).
INSERT dbo.Transactions (OrderId, Provider, ProviderRef, Kind, AmountUsd, OccurredAtUtc, Status, created_by, modified_by)
SELECT o.OrderId, 'stripe', 'ch_seed_0001', 2, 318.18, '2026-07-01T10:01:00', 2, 'seed', 'seed' FROM dbo.Orders o WHERE o.OrderNumber = 'SEED-ORD-0001'
UNION ALL SELECT o.OrderId, 'stripe', 'ch_seed_0002', 2, 119.49, '2026-07-02T11:31:00', 2, 'seed', 'seed' FROM dbo.Orders o WHERE o.OrderNumber = 'SEED-ORD-0002'
UNION ALL SELECT o.OrderId, 'paypal', 'pp_seed_0003', 2, 64.99, '2026-07-03T09:16:00', 2, 'seed', 'seed' FROM dbo.Orders o WHERE o.OrderNumber = 'SEED-ORD-0003'
UNION ALL SELECT o.OrderId, 'stripe', 'ch_seed_0004', 1, 44.99, '2026-07-04T16:46:00', 1, 'seed', 'seed' FROM dbo.Orders o WHERE o.OrderNumber = 'SEED-ORD-0004';

-- Shipments (for the shipped/delivered orders).
INSERT dbo.Shipments (OrderId, Carrier, TrackingNumber, ShippedAtUtc, DeliveredAtUtc, Status, created_by, modified_by)
SELECT o.OrderId, 'ups',   'SEED-TRK-0001', '2026-07-02T09:00:00', '2026-07-04T14:00:00', 4, 'seed', 'seed' FROM dbo.Orders o WHERE o.OrderNumber = 'SEED-ORD-0001'
UNION ALL SELECT o.OrderId, 'fedex', 'SEED-TRK-0002', '2026-07-03T09:00:00', NULL,                  3, 'seed', 'seed' FROM dbo.Orders o WHERE o.OrderNumber = 'SEED-ORD-0002';

PRINT 'OltpDb seeded: 4 categories, 6 products, 3 warehouses, 4 customers/addresses, inventory, 4 orders, 6 lines, 4 transactions, 2 shipments.';
