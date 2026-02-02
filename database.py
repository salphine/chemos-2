import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import json

class Database:
    def __init__(self):
        self.connection = None
        self.connect()
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="password",
                database="sales_system",
                port=3306
            )
            print("Database connected successfully")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            # Create in-memory data for demo
            self.connection = None
            
    def get_connection(self):
        if not self.connection or not self.connection.is_connected():
            self.connect()
        return self.connection
    
    def execute_query(self, query, params=None):
        conn = self.get_connection()
        if conn is None:
            return None
            
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(query, params or ())
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            conn.commit()
            return True
        except Exception as e:
            print(f"Query error: {e}")
            return None
        finally:
            cursor.close()
    
    def create_tables(self):
        """Create necessary tables"""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                role ENUM('admin', 'manager', 'clerk') DEFAULT 'clerk',
                email VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                category VARCHAR(50),
                price DECIMAL(10, 2) NOT NULL,
                stock_quantity INT DEFAULT 0,
                min_stock_level INT DEFAULT 10,
                max_stock_level INT DEFAULT 100,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                transaction_id VARCHAR(20) UNIQUE,
                product_id INT,
                quantity INT,
                unit_price DECIMAL(10, 2),
                total_price DECIMAL(10, 2),
                tax_amount DECIMAL(10, 2),
                payment_method VARCHAR(20),
                customer_info TEXT,
                user_id INT,
                sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS inventory_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT,
                action VARCHAR(20),
                quantity_change INT,
                new_quantity INT,
                user_id INT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS settings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                business_name VARCHAR(100),
                tax_rate DECIMAL(5, 2) DEFAULT 16.0,
                currency VARCHAR(10) DEFAULT 'KES',
                low_stock_alert BOOLEAN DEFAULT TRUE,
                receipt_template TEXT,
                email_notifications BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        ]
        
        for query in queries:
            self.execute_query(query)
        
        # Insert default admin user
        self.execute_query("""
            INSERT IGNORE INTO users (username, password, role, email) 
            VALUES ('admin', 'admin123', 'admin', 'admin@system.com')
        """)
        
        # Insert default settings
        self.execute_query("""
            INSERT IGNORE INTO settings (business_name) 
            VALUES ('Lukenya Getaway Resort')
        """)
    
    def get_sample_data(self):
        """Return sample data for demo purposes"""
        products = [
            {'id': 1, 'name': 'Bottled Water', 'category': 'Beverages', 'price': 50.0, 'stock_quantity': 150, 'min_stock_level': 20},
            {'id': 2, 'name': 'Soda', 'category': 'Beverages', 'price': 80.0, 'stock_quantity': 75, 'min_stock_level': 30},
            {'id': 3, 'name': 'Coffee', 'category': 'Beverages', 'price': 150.0, 'stock_quantity': 15, 'min_stock_level': 25},
            {'id': 4, 'name': 'Tea', 'category': 'Beverages', 'price': 120.0, 'stock_quantity': 8, 'min_stock_level': 15},
            {'id': 5, 'name': 'Sandwich', 'category': 'Food', 'price': 300.0, 'stock_quantity': 45, 'min_stock_level': 20},
            {'id': 6, 'name': 'Burger', 'category': 'Food', 'price': 450.0, 'stock_quantity': 25, 'min_stock_level': 15},
            {'id': 7, 'name': 'Pizza', 'category': 'Food', 'price': 800.0, 'stock_quantity': 12, 'min_stock_level': 10},
            {'id': 8, 'name': 'Chicken Wings', 'category': 'Food', 'price': 650.0, 'stock_quantity': 18, 'min_stock_level': 15},
            {'id': 9, 'name': 'Ice Cream', 'category': 'Dessert', 'price': 200.0, 'stock_quantity': 35, 'min_stock_level': 20},
            {'id': 10, 'name': 'Cake Slice', 'category': 'Dessert', 'price': 250.0, 'stock_quantity': 22, 'min_stock_level': 10},
        ]
        
        users = [
            {'id': 1, 'username': 'admin', 'role': 'admin', 'email': 'admin@system.com'},
            {'id': 2, 'username': 'manager1', 'role': 'manager', 'email': 'manager@system.com'},
            {'id': 3, 'username': 'clerk1', 'role': 'clerk', 'email': 'clerk@system.com'},
        ]
        
        return products, users