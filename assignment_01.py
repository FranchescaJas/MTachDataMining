import mysql.connector



def load_db_properties(file_path="db.properties"):
    props = {}
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            props[key.strip()] = value.strip()
    return props


#check that database is connected or not
def connect_db():
    props = load_db_properties()

    return mysql.connector.connect(
        host=props["db.host"],
        user=props["db.user"],
        password=props["db.password"],
        database=props["db.name"],
        port=int(props["db.port"])
    )


#run query
def run_query(conn, title, query):
    print("..")
    print(title)
    print("..")

    cursor = conn.cursor()
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    print(" | ".join(columns))
    print("-" * 90)

    for row in cursor.fetchall():
        print(" | ".join(str(col) for col in row))

    cursor.close()


#main
def main():
    try:
        conn = connect_db()
        print("databse connection successful")

        # 1.Show the types of all items that were made in India
        run_query(
            conn,
            "1) Show the types of all items that were made in India ",
            "SELECT DISTINCT type FROM item WHERE place_made = 'India';"
        )

        # 2. List all the items costing less than $1000.
        run_query(
            conn,
            "2) List all the items costing less than $1000.",
            "SELECT item_ID, name, price FROM item WHERE price < 1000 ORDER BY price;"
        )

        # 3.  Show the sales of December month grouped by customer.
        run_query(
            conn,
            "3) Show the sales of December month grouped by customer.",
            """
            SELECT c.cust_ID, c.name, SUM(p.amount) AS total_dec_sales
            FROM purchases p
                     JOIN customer c ON c.cust_ID = p.cust_ID
            WHERE MONTH(p.date) = 12
            GROUP BY c.cust_ID, c.name
            ORDER BY total_dec_sales DESC;
            """
        )

        # 4. Which sales person had the highest amount of sales?
        run_query(
            conn,
            "4) Which sales person had the highest amount of sales?",
            """
            SELECT e.empl_ID, e.name, SUM(p.amount) AS total_sales
            FROM purchases p
                     JOIN employee e ON e.empl_ID = p.empl_ID
            GROUP BY e.empl_ID, e.name
            ORDER BY total_sales DESC
                LIMIT 1;
            """
        )

        # 5.Sort the employees in decreasing order of their salaries.
        run_query(
            conn,
            "5) Sort the employees in decreasing order of their salaries.",
            "SELECT empl_ID, name, salary FROM employee ORDER BY salary DESC;"
        )

        # 6. List the name of the employees working in a particular branch.
        branch_id = "B2"
        run_query(
            conn,
            f"6) Employees working in branch {branch_id}",
            f"""
            SELECT e.empl_ID, e.name, b.name AS branch_name
            FROM works_at w
            JOIN employee e ON e.empl_ID = w.empl_ID
            JOIN branch b ON b.branch_ID = w.branch_ID
            WHERE b.branch_ID = '{branch_id}';
            """
        )

        # 7.List all the customers who have income greater than $1000
        run_query(
            conn,
            "7) List all the customers who have income greater than $1000",
            "SELECT cust_ID, name, income FROM customer WHERE income > 1000 ORDER BY income DESC;"
        )

        # 8. Which employee is associated with more number of transactions.
        run_query(
            conn,
            "8) Which employee is associated with more number of transactions.",
            """
            SELECT e.empl_ID, e.name, COUNT(*) AS transaction_count
            FROM purchases p
                     JOIN employee e ON e.empl_ID = p.empl_ID
            GROUP BY e.empl_ID, e.name
            ORDER BY transaction_count DESC
                LIMIT 1;
            """
        )

        conn.close()
        print("")

    except Exception as e:
        print("Getting Exceptionn while processing this data ", e)


if __name__ == "__main__":
    main()
