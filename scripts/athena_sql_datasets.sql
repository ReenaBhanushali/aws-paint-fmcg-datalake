create table demand_supply_summary as 
SELECT 
    so.product_id,
    so.product_name,
    SUM(so.quantity) AS total_demand,
    inv.available_qty,
    COALESCE(pp.planned_qty,0) AS planned_qty,
    (inv.available_qty + COALESCE(pp.planned_qty,0) - SUM(so.quantity)) AS balance
FROM paint_fmcg_db.sales_orders so
LEFT JOIN paint_fmcg_db.inventory inv 
    ON so.product_id = inv.product_id
LEFT JOIN paint_fmcg_db.production_plan pp
    ON so.product_id = pp.product_id
GROUP BY so.product_id, so.product_name, inv.available_qty, pp.planned_qty;

create table sales_stock_summary as 
SELECT 
    s.product_id,
    SUM(s.quantity) AS total_ordered,
    COALESCE(i.available_qty, 0) AS total_inventory,
    COALESCE(i.available_qty, 0) - SUM(s.quantity) AS balance_stock
FROM sales_orders s
LEFT JOIN inventory i
    ON s.product_id = i.product_id
GROUP BY s.product_id, i.available_qty
ORDER BY balance_stock ASC;
