from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String,DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# PostgreSQL connection details
pg_host = "localhost"
pg_db = "csv_database"
pg_user = "postgres"
pg_password = "obontong"

# MySQL connection details for RDS
mysql_host = "database-2.c7c0as86cf6q.ap-south-1.rds.amazonaws.com"
mysql_db = "salesdb"
mysql_user = "admin"
mysql_password = "obontong"

# PostgreSQL and MySQL connection URLs using SQLAlchemy
pg_connection_url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}/{pg_db}"
mysql_connection_url = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}"

# Initialize MetaData
metadata = MetaData()

# Define table schema with extend_existing=True
def define_table():
    """
    Defines the table schema for MySQL with extend_existing=True to avoid redefinition errors.
    """
    return Table('saless', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('country', String(50)),  # Adjust data type as needed
                 Column('item_type', String(50)),
                 Column('sales_channel', String(50)),
                 Column('order_priority', String(50)),
                 Column('order_date', String(50)),
                 Column('region', String(50)),
                 Column('ship_date', String(50)),
                 Column('units_sold', Integer),
                 Column('unit_price', Integer),
                 Column('unit_cost', Integer),
                 Column('total_revenue', Integer),
                 Column('total_cost', Integer),
                 Column('total_profit', Integer),
                 
                 extend_existing=True)  # Prevent redefinition errors

# Connect to PostgreSQL using SQLAlchemy
def connect_postgresql():
    try:
        engine = create_engine(pg_connection_url)
        connection = engine.connect()
        print("✅ PostgreSQL connection established.")
        return engine, connection
    except SQLAlchemyError as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        return None, None

# Connect to MySQL (RDS) using SQLAlchemy
def connect_mysql():
    try:
        engine = create_engine(mysql_connection_url)
        connection = engine.connect()
        print("✅ MySQL connection established.")
        return engine, connection
    except SQLAlchemyError as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None, None

# Extract data from PostgreSQL
def extract_data_from_postgresql(pg_connection, query):
    try:
        result = pg_connection.execute(query)
        data = result.fetchall()
        print(f"📥 Extracted {len(data)} rows from PostgreSQL.")
        return data
    except SQLAlchemyError as e:
        print(f"❌ Error extracting data from PostgreSQL: {e}")
        return []

# Create the table in MySQL using SQLAlchemy
def create_table_in_mysql(mysql_engine):
    try:
        table = define_table()
        metadata.create_all(mysql_engine)  # Creates table if it doesn't exist
        print(f"✅ Table '{table.name}' created in MySQL.")
    except SQLAlchemyError as e:
        print(f"❌ Error creating table in MySQL: {e}")

# Insert data into MySQL (RDS) using SQLAlchemy ORM
def insert_data_to_mysql(mysql_engine, data):
    try:
        if not data:
            print("⚠️ No data to insert.")
            return

        table = define_table()  # Ensure the correct table is used

        # Use bulk insert for better performance
        with mysql_engine.begin() as connection:
            insert_list = [
                {"number": row[0], "country": row[1], "item_type": row[2],"sales_channel": row[3],"order_priority": row[4],"order_date": row[5],"region": row[6],"country": row[7],
                 "ship_date": row[7],"units_sold": row[8],"unit_price": row[9],"unit_cost": row[10],"total_revenue": row[11],"total_cost": row[12],"total_profit": row[13]
                 }
                for row in data
            ]
            connection.execute(table.insert(), insert_list)

        print(f"📤 Inserted {len(data)} rows into MySQL.")
    except SQLAlchemyError as e:
        print(f"❌ Error inserting data into MySQL: {e}")

# Main function to orchestrate the process
def main():
    # Query to fetch data from PostgreSQL (Modify as needed)
    pg_query = """
        SELECT *
        FROM sales_schema.sales_table;
    """  

    # Connect to PostgreSQL
    pg_engine, pg_conn = connect_postgresql()
    if not pg_conn:
        return

    # Connect to MySQL
    mysql_engine, mysql_conn = connect_mysql()
    if not mysql_conn:
        pg_conn.close()
        return

    # Create the table in MySQL
    create_table_in_mysql(mysql_engine)

    # Extract data from PostgreSQL
    data = extract_data_from_postgresql(pg_conn, pg_query)
    if data:
        # Insert data into MySQL
        insert_data_to_mysql(mysql_engine, data)

    # Close the connections
    pg_conn.close()
    mysql_conn.close()
    print("✅ Process completed successfully.")

# Run the main function
if __name__ == "__main__":
    main()
