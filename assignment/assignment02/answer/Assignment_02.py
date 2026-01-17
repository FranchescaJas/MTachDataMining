from os import close
"""
@Author: Jayashree D
@Date: 2024-06-10
@Title: Assignment 02 - SQL Queries on 'allelectronics' Database
"""
print("Jayashree")

import  mysql.connector
from  mysql.connector import  Error
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "allelectronics",
    "port": 3306
}

def check_db_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("Connection to database was successful.")
            return True
        print("not connected to database.")
        return False

    except Error as e:
        print(f"Error while connecting to database: {e}")
        return False
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()



def check_required_tables():
    required_tables = {"customer", "item", "employee","branch","purchases","items_sold","works_at"}
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        existing_tables = {table[0] for table in cursor.fetchall()}
        missing_tables = required_tables - existing_tables
        if not missing_tables:
            print("All required tables are present.")
            return True
        else:
            print(f"Missing tables: {', '.join(missing_tables)}")
            return False
    except Error as e:
        print(f"Error while checking tables: {e}")
        return False

    finally:
        try:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
        except:
            pass





def print_rows(title,cols,rows):
    print(f"\n{title}")
    print("-" * len(title))
    print("\t".join(cols))
    for row in rows:
        print("\t".join(str(item) for item in row))








def get_item_X_from_database(conn):
    """
    Fetches the most frequently sold item from database and returns its item_id.
    """
    sql = """
          SELECT item_id
          FROM items_sold
          GROUP BY item_id
          ORDER BY SUM(qty) DESC
              LIMIT 1;
          """

    cursor = conn.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    cursor.close()

    if row:
        return row[0]
    else:
        return None



def task1(conn):

    item_x = get_item_X_from_database(conn)

    if not item_x:
        print("No item found in database.")
        return

    sql = """
          SELECT it.name AS along_item, COUNT(*) AS frequency
          FROM items_sold i1
                   JOIN items_sold i2 ON i1.trans_ID = i2.trans_ID
                   JOIN item it ON it.item_ID = i2.item_ID
          WHERE i1.item_ID = %s
            AND i2.item_ID <> %s
          GROUP BY it.name
          ORDER BY frequency DESC
              LIMIT 1; 
          """

    cursor = conn.cursor()
    cursor.execute(sql, (item_x, item_x))
    result = cursor.fetchone()
    cursor.close()

    if result:
        print(f"Most frequent item bought with item {item_x}:")
        print(f"Item Name : {result[0]}")
        print(f"Frequency : {result[1]}")
    else:
        print(f"No item found that is bought along with item {item_x}")





def task2(conn):
    sql = """
          SELECT branch_ID, item_ID, total_qty
          FROM (
                   SELECT w.branch_ID,
                          isd.item_ID,
                          SUM(isd.qty) AS total_qty,
                          ROW_NUMBER() OVER (
               PARTITION BY w.branch_ID
               ORDER BY SUM(isd.qty) DESC
             ) AS rn
                   FROM works_at w
                            JOIN purchases p ON p.empl_ID = w.empl_ID
                            JOIN items_sold isd ON isd.trans_ID = p.trans_ID
                   GROUP BY w.branch_ID, isd.item_ID
               ) t
          WHERE rn = 1
          ORDER BY branch_ID;
          """

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()

    print("\nTask-2) Top selling product (by qty) for each branch")
    print("branch_ID\titem_ID\ttotal_qty")
    for r in rows:
        print(f"{r[0]}\t\t{r[1]}\t\t{r[2]}")



def task3(conn):
    sql = """
          WITH top_product AS (
              SELECT branch_ID, item_ID
              FROM (
                       SELECT w.branch_ID,
                              isd.item_ID,
                              SUM(isd.qty) AS total_qty,
                              ROW_NUMBER() OVER (
                 PARTITION BY w.branch_ID
                 ORDER BY SUM(isd.qty) DESC
               ) AS rn
                       FROM works_at w
                                JOIN purchases p ON p.empl_ID = w.empl_ID
                                JOIN items_sold isd ON isd.trans_ID = p.trans_ID
                       GROUP BY w.branch_ID, isd.item_ID
                   ) x
              WHERE rn = 1
          ),
               cofreq AS (
                   SELECT w.branch_ID,
                          tp.item_ID AS product_M,
                          i2.item_ID AS along_item,
                          COUNT(*) AS freq
                   FROM works_at w
                            JOIN purchases p ON p.empl_ID = w.empl_ID
                            JOIN items_sold i1 ON i1.trans_ID = p.trans_ID
                            JOIN top_product tp
                                 ON tp.branch_ID = w.branch_ID
                                     AND tp.item_ID = i1.item_ID
                            JOIN items_sold i2 ON i2.trans_ID = i1.trans_ID
                   WHERE i2.item_ID <> i1.item_ID
                   GROUP BY w.branch_ID, tp.item_ID, i2.item_ID
               )
          SELECT branch_ID, product_M, along_item, freq
          FROM (
                   SELECT branch_ID, product_M, along_item, freq,
                          ROW_NUMBER() OVER (PARTITION BY branch_ID ORDER BY freq DESC) AS rn
                   FROM cofreq
               ) z
          WHERE rn = 1
          ORDER BY branch_ID; 
          """

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()

    print("\nTask-3) Most frequent item bought along with product M (top-selling) for each branch")
    print("branch_ID\tproduct_M\talong_item\tfreq")
    for r in rows:
        print(f"{r[0]}\t\t{r[1]}\t\t{r[2]}\t\t{r[3]}")


def task4(conn):
    sql = """
          SELECT e.empl_ID, e.name, SUM(isd.qty) AS total_items
          FROM purchases p
                   JOIN items_sold isd ON isd.trans_ID = p.trans_ID
                   JOIN employee e ON e.empl_ID = p.empl_ID
          GROUP BY e.empl_ID, e.name
          ORDER BY total_items DESC
              LIMIT 1; 
          """

    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()

    print("\nTask-4) Employee associated with maximum number of items")
    if row:
        print(f"Employee ID   : {row[0]}")
        print(f"Employee Name : {row[1]}")
        print(f"Total Items   : {row[2]}")
    else:
        print("No data found")



def main():
    try:

        if not check_db_connection():
            return
        if not check_required_tables():
            return

        conn = mysql.connector.connect(**DB_CONFIG)
        task1(conn)
        task2(conn)
        task3(conn)
        task4(conn)
        conn.close()

    except Error as e:
        print(f"Error: {e}")
        print("Verified DB connection parameters")
        print("Verified required  table/column ")






if __name__ == "__main__":
    main()