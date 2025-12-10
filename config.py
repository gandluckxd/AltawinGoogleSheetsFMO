import os
from dotenv import load_dotenv

load_dotenv()

# Настройки подключения к базе данных Altawin
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '10.8.0.3'),
    'port': int(os.getenv('DB_PORT', '3050')),
    'database': os.getenv('DB_DATABASE', 'D:/altAwinDB/ppk.gdb'),
    'user': os.getenv('DB_USER', 'sysdba'),
    'password': os.getenv('DB_PASSWORD', 'masterkey'),
    'charset': os.getenv('DB_CHARSET', 'WIN1251')
}

# Настройки Google Sheets
GOOGLE_SHEETS_CONFIG = {
    'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json'),
    'spreadsheet_name': os.getenv('GOOGLE_SPREADSHEET_NAME', 'Копия ТАБЛИЦА УЧЕТА ЗАКАЗОВ для тестирования'),
    'worksheet_name': os.getenv('GOOGLE_WORKSHEET_NAME', 'Общий'),
    'worksheet_name_by_order': os.getenv('GOOGLE_WORKSHEET_NAME_BY_ORDER', 'Расшифр по заказам')
}

# Настройки для основной таблицы (новая таблица)
GOOGLE_SHEETS_MAIN_CONFIG = {
    'credentials_file': os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json'),
    'spreadsheet_id': os.getenv('GOOGLE_MAIN_SPREADSHEET_ID', '1pbz9K6uarZy-3oax9OGfyA6MxtDCNoI9noavlr1YhOc'),
    'worksheet_name_orders': os.getenv('GOOGLE_MAIN_WORKSHEET_ORDERS', 'Заказы')
}

# SQL-запросы
SQL_QUERIES = {
    'izd_pvh': """
        SELECT
            o.proddate,
            SUM(oi.qty) AS qty_izd_pvh
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN models m ON m.orderitemsid = oi.orderitemsid
        JOIN r_systems rs ON rs.rsystemid = m.sysprofid
        WHERE o.proddate BETWEEN ? AND ?
            AND rs.systemtype = 0
            AND rs.rsystemid <> 8
        GROUP BY o.proddate
    """,
    'razdv': """
        SELECT
            o.proddate,
            SUM(oi.qty) AS qty_razdv
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN models m ON m.orderitemsid = oi.orderitemsid
        JOIN r_systems rs ON rs.rsystemid = m.sysprofid
        WHERE o.proddate BETWEEN ? AND ?
            AND ((rs.systemtype = 1) OR (rs.rsystemid = 8))
        GROUP BY o.proddate
    """,
    'mosnet': """
        SELECT
            o.proddate,
            SUM(oi.qty * itd.qty) AS qty_mosnet
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN itemsdetail itd ON itd.orderitemsid = oi.orderitemsid
        WHERE o.proddate BETWEEN ? AND ?
            AND itd.grgoodsid = 46110
        GROUP BY o.proddate
    """,
    'glass_packs': """
        select
            o.proddate,
            sum(oi.qty) as qty_glass_packs
        from orders o
        join orderitems oi on oi.orderid = o.orderid
        join models m on m.orderitemsid = oi.orderitemsid
        join modelparts mp on mp.modelid = m.modelid
        join modelfillings mf on mf.modelpartid = mp.modelpartid
        join gpackettypes gp on gp.gptypeid = mf.gptypeid
        join r_systems rs on rs.rsystemid = gp.rsystemid
        where o.proddate between ? and ?
        and rs.rsystemid in (3, 21)
        group by o.proddate
    """,
    'sandwiches': """
        select
            o.proddate,
            sum(oi.qty) as qty_sandwiches
        from orders o
        join orderitems oi on oi.orderid = o.orderid
        join models m on m.orderitemsid = oi.orderitemsid
        join modelparts mp on mp.modelid = m.modelid
        join modelfillings mf on mf.modelpartid = mp.modelpartid
        join gpackettypes gp on gp.gptypeid = mf.gptypeid
        join r_systems rs on rs.rsystemid = gp.rsystemid
        where o.proddate between ? and ?
        and rs.rsystemid in (22)
        group by o.proddate
    """,
    'windowsills': """
        select
            o.proddate,
            sum(i.qty * oi.qty) as qty_windowsills
        from orderitems oi
        join orders o on o.orderid = oi.orderid
        join itemsdetail i on i.orderitemsid = oi.orderitemsid
        join goods g on i.goodsid = g.goodsid
        join groupgoods gg on i.grgoodsid = gg.grgoodsid
        where o.proddate between ? and ?
        and gg.ggtypeid = 42
        group by
            o.proddate
    """,
    'iron': """
        select
            o.proddate,
            sum(oi.qty * its.qty) as qty_iron
        from orderitems oi
        join orders o on o.orderid = oi.orderid
        join itemssets its on its.orderitemsid = oi.orderitemsid
        join groupgoods gg on gg.grgoodsid = its.setid
        where o.proddate between ? and ?
        and gg.isggset = 1
        and ((gg.marking like '%Водоотлив%') or (gg.marking like '%Железо%') or (gg.marking like '%Козырек%') or (gg.marking like '%Нащельник%'))
        group by o.proddate
    """
}

# SQL-запросы с группировкой по заказам
SQL_QUERIES_BY_ORDER = {
    'izd_pvh': """
        SELECT
            o.proddate,
            o.orderno,
            SUM(oi.qty) AS qty_izd_pvh
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN models m ON m.orderitemsid = oi.orderitemsid
        JOIN r_systems rs ON rs.rsystemid = m.sysprofid
        WHERE o.datemodified BETWEEN ? AND ?
            AND o.proddate IS NOT NULL
            AND rs.systemtype = 0
            AND rs.rsystemid <> 8
            AND rs.rsystemid <> 27
        GROUP BY o.proddate, o.orderno
    """,
    'razdv': """
        SELECT
            o.proddate,
            o.orderno,
            SUM(oi.qty) AS qty_razdv
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN models m ON m.orderitemsid = oi.orderitemsid
        JOIN r_systems rs ON rs.rsystemid = m.sysprofid
        WHERE o.datemodified BETWEEN ? AND ?
            AND o.proddate IS NOT NULL
            AND ((rs.systemtype = 1) OR (rs.rsystemid = 8))
        GROUP BY o.proddate, o.orderno
    """,
    'mosnet': """
        SELECT
            o.proddate,
            o.orderno,
            SUM(oi.qty * itd.qty) AS qty_mosnet
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN itemsdetail itd ON itd.orderitemsid = oi.orderitemsid
        WHERE o.datemodified BETWEEN ? AND ?
            AND o.proddate IS NOT NULL
            AND itd.grgoodsid = 46110
        GROUP BY o.proddate, o.orderno
    """,
    'glass_packs': """
        select
            o.proddate,
            o.orderno,
            sum(oi.qty) as qty_glass_packs
        from orders o
        join orderitems oi on oi.orderid = o.orderid
        join models m on m.orderitemsid = oi.orderitemsid
        join modelparts mp on mp.modelid = m.modelid
        join modelfillings mf on mf.modelpartid = mp.modelpartid
        join gpackettypes gp on gp.gptypeid = mf.gptypeid
        join r_systems rs on rs.rsystemid = gp.rsystemid
        where o.datemodified between ? and ?
        and o.proddate is not null
        and rs.rsystemid in (3, 21)
        group by o.proddate, o.orderno
    """,
    'sandwiches': """
        SELECT
            o.proddate,
            o.orderno,
            SUM(oi.qty * itd.qty) AS qty_sandwiches
        FROM orders o
        JOIN orderitems oi ON oi.orderid = o.orderid
        JOIN itemsdetail itd ON itd.orderitemsid = oi.orderitemsid
        join groupgoods gg on gg.grgoodsid = itd.grgoodsid
        join groupgoodstypes ggt on ggt.ggtypeid = gg.ggtypeid
        WHERE o.datemodified BETWEEN ? AND ?
            AND o.proddate IS NOT NULL
            AND ggt.code in ('Sand', 'SandDop')
        GROUP BY o.proddate, o.orderno
    """,
    'windowsills': """
        select
            o.proddate,
            o.orderno,
            sum(i.qty * oi.qty) as qty_windowsills
        from orderitems oi
        join orders o on o.orderid = oi.orderid
        join itemsdetail i on i.orderitemsid = oi.orderitemsid
        join goods g on i.goodsid = g.goodsid
        join groupgoods gg on i.grgoodsid = gg.grgoodsid
        where o.datemodified between ? and ?
        and o.proddate is not null
        and gg.ggtypeid = 42
        group by
            o.proddate, o.orderno
    """,
    'iron': """
        select
            o.proddate,
            o.orderno,
            sum(oi.qty * its.qty) as qty_iron
        from orderitems oi
        join orders o on o.orderid = oi.orderid
        join itemssets its on its.orderitemsid = oi.orderitemsid
        join groupgoods gg on gg.grgoodsid = its.setid
        where o.datemodified between ? and ?
        and o.proddate is not null
        and gg.isggset = 1
        and ((gg.marking like '%Водоотлив%') or (gg.marking like '%Железо%') or (gg.marking like '%Козырек%') or (gg.marking like '%Нащельник%'))
        group by o.proddate, o.orderno
    """,
    'totalprice': """
        select
            o.proddate,
            o.orderno,
            o.totalprice
        from orders o
        where o.datemodified between ? and ?
        and o.proddate is not null
        group by o.proddate, o.orderno, o.totalprice
    """,
    'orderid': """
        select
            o.proddate,
            o.orderno,
            o.orderid
        from orders o
        where o.datemodified between ? and ?
        and o.proddate is not null
        group by o.proddate, o.orderno, o.orderid
    """,
    'readiness': """
        select
            o.proddate,
            o.orderno,
            TRIM(case
                when COUNT(DISTINCT el.ctelementsid) = 0 then 'Готов'
                when COUNT(DISTINCT el.ctelementsid) = SUM(CASE WHEN wd.isapproved = 1 THEN 1 ELSE 0 END) then 'Готов'
                else 'Не готов'
            end) as readiness
        from orders o
        join orderitems oi on oi.orderid = o.orderid
        join models m on m.orderitemsid = oi.orderitemsid
        left join ct_elements el on el.modelid = m.modelid and el.cttypeelemsid = 2
        left join ct_whdetail wd on wd.ctelementsid = el.ctelementsid
        where o.datemodified between ? and ?
        and o.proddate is not null
        group by o.proddate, o.orderno
    """
}

# SQL-запрос для проверки готовности заказа
SQL_QUERY_CHECK_ORDER_READINESS = """
    select wd.isapproved
    from orders o
    join orderitems oi on oi.orderid = o.orderid
    join models m on m.orderitemsid = oi.orderitemsid
    left join ct_elements el on el.modelid = m.modelid
    left join ct_whdetail wd on wd.ctelementsid = el.ctelementsid
    where o.orderid = ?
    and el.cttypeelemsid = 2
"""
