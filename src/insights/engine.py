def top_zones(con, country: str, top_n: int = 5):
    sql = """
    SELECT zone, SUM(orders) AS total_orders
    FROM ops.metrics
    WHERE country = ?
    GROUP BY zone
    ORDER BY total_orders DESC
    LIMIT ?
    """
    return con.execute(sql, [country, top_n]).fetchall()
