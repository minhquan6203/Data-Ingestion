import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys

def generate_customers(num_records=10000, chunk_size=1000):
    # Sample data for generating realistic customer information
    first_names = [
        'John', 'Maria', 'Robert', 'Sarah', 'James', 'Emma', 'Michael', 'Lisa', 'David', 'Sophie',
        'William', 'Olivia', 'Daniel', 'Sophia', 'Joseph', 'Isabella', 'Thomas', 'Mia', 'Charles', 'Charlotte',
        'Henry', 'Amelia', 'Sebastian', 'Harper', 'Alexander', 'Evelyn', 'Jack', 'Abigail', 'Owen', 'Emily',
        'Gabriel', 'Elizabeth', 'Julian', 'Sofia', 'Wyatt', 'Madison', 'Luke', 'Avery', 'Jayden', 'Ella',
        'Ethan', 'Grace', 'Benjamin', 'Chloe', 'Samuel', 'Victoria', 'Matthew', 'Layla', 'Aiden', 'Zoe',
        'Christopher', 'Penelope', 'Andrew', 'Riley', 'Joshua', 'Nora', 'Ryan', 'Lily', 'Nathan', 'Eleanor',
        'Christian', 'Hannah', 'Jonathan', 'Lillian', 'Levi', 'Natalie', 'Elijah', 'Addison', 'Aaron', 'Luna',
        'Caleb', 'Savannah', 'Isaac', 'Brooklyn', 'Eli', 'Maya', 'Isaiah', 'Camila', 'Hunter', 'Aria',
        'Jackson', 'Scarlett', 'Justin', 'Audrey', 'Leo', 'Bella', 'Lincoln', 'Claire', 'Tyler', 'Skylar',
        'Ezra', 'Lucy', 'Landon', 'Anna', 'Mason', 'Caroline', 'Xavier', 'Nova', 'Jeremiah', 'Genesis'
    ]
    
    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
        'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
        'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
        'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
        'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts',
        'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker', 'Cruz', 'Edwards', 'Collins', 'Reyes',
        'Stewart', 'Morris', 'Morales', 'Murphy', 'Cook', 'Rogers', 'Gutierrez', 'Ortiz', 'Morgan', 'Cooper',
        'Peterson', 'Bailey', 'Reed', 'Kelly', 'Howard', 'Ramos', 'Kim', 'Cox', 'Ward', 'Richardson',
        'Watson', 'Brooks', 'Chavez', 'Wood', 'James', 'Bennett', 'Gray', 'Mendoza', 'Ruiz', 'Hughes',
        'Price', 'Alvarez', 'Castillo', 'Sanders', 'Patel', 'Myers', 'Long', 'Ross', 'Foster', 'Jimenez'
    ]
    
    countries = [
        'USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan', 'China', 'India', 'Brazil',
        'Spain', 'Italy', 'Mexico', 'South Korea', 'Russia', 'Netherlands', 'Sweden', 'Singapore', 'New Zealand', 'Ireland',
        'Argentina', 'South Africa', 'Norway', 'Denmark', 'Poland', 'Switzerland', 'Austria', 'Belgium', 'Portugal', 'Greece',
        'Turkey', 'Israel', 'Egypt', 'Thailand', 'Malaysia', 'Philippines', 'Indonesia', 'Vietnam', 'Pakistan', 'Chile',
        'Colombia', 'Peru', 'Nigeria', 'Kenya', 'Morocco', 'Saudi Arabia', 'UAE', 'Finland', 'Hungary', 'Czech Republic',
        'Romania', 'Ukraine', 'Croatia', 'Serbia', 'Bulgaria', 'Slovakia', 'Estonia', 'Latvia', 'Lithuania', 'Slovenia'
    ]
    
    # Create output file and write header
    os.makedirs('data', exist_ok=True)
    with open('data/customers.csv', 'w') as f:
        f.write('customer_id,name,email,registration_date,country\n')
    
    total_records = 0
    
    # Generate data in chunks
    for chunk_start in range(0, num_records, chunk_size):
        chunk_end = min(chunk_start + chunk_size, num_records)
        chunk_size_actual = chunk_end - chunk_start
        
        # Generate customer IDs for this chunk
        customer_ids = [f'C{str(i+1).zfill(4)}' for i in range(chunk_start, chunk_end)]
        
        # Generate random data for this chunk
        data = {
            'customer_id': customer_ids,
            'name': [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(chunk_size_actual)],
            'email': [f"customer{i+1}@example.com" for i in range(chunk_start, chunk_end)],
            'registration_date': [(datetime(2023, 1, 1) + timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d') 
                                for _ in range(chunk_size_actual)],
            'country': [random.choice(countries) for _ in range(chunk_size_actual)]
        }
        
        # Create dataframe for this chunk and append to file
        chunk_df = pd.DataFrame(data)
        chunk_df.to_csv('data/customers.csv', index=False, mode='a', header=False)
        total_records += chunk_size_actual
    
    return total_records

def generate_products(num_records=10000, chunk_size=1000):
    # Sample data for generating realistic product information
    categories = [
        'Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty', 'Toys', 'Food', 'Health', 'Automotive',
        'Office Supplies', 'Furniture', 'Jewelry', 'Pet Supplies', 'Baby Products', 'Tools', 'Outdoors', 'Art Supplies', 'Music', 'Movies',
        'Appliances', 'Shoes', 'Accessories', 'Kitchen', 'Bath', 'Bedding', 'Lighting', 'Storage', 'Cleaning', 'Stationery',
        'Crafts', 'Gaming', 'Cameras', 'Audio', 'Computer Hardware', 'Software', 'Mobile Phones', 'Tablets', 'TV & Video', 'Wearable Tech',
        'Smart Home', 'Travel Gear', 'Luggage', 'Fitness Equipment', 'Supplements', 'Groceries', 'Beverages', 'Snacks', 'Baking', 'Specialty Foods'
    ]
    
    product_names = [
        'Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Smartwatch', 'Camera', 'Printer', 'Monitor', 'Keyboard', 'Mouse',
        'T-shirt', 'Jeans', 'Dress', 'Shoes', 'Jacket', 'Sweater', 'Pants', 'Skirt', 'Socks', 'Hat',
        'Novel', 'Textbook', 'Comic', 'Magazine', 'Journal', 'Cookbook', 'Art Book', 'Children Book', 'Biography', 'Poetry',
        'Sofa', 'Chair', 'Table', 'Desk', 'Bed', 'Dresser', 'Shelf', 'Cabinet', 'Lamp', 'Mirror',
        'Vacuum', 'Blender', 'Toaster', 'Microwave', 'Refrigerator', 'Dishwasher', 'Coffee Maker', 'Air Fryer', 'Mixer', 'Grill',
        'Necklace', 'Ring', 'Bracelet', 'Earrings', 'Watch', 'Sunglasses', 'Scarf', 'Handbag', 'Wallet', 'Backpack',
        'Toy Car', 'Action Figure', 'Doll', 'Board Game', 'Puzzle', 'Building Blocks', 'Stuffed Animal', 'Remote Control Car', 'Educational Toy', 'Art Set',
        'Dog Food', 'Cat Litter', 'Pet Toy', 'Fish Tank', 'Dog Bed', 'Pet Carrier', 'Bird Cage', 'Pet Bowl', 'Pet Treats', 'Pet Shampoo'
    ]
    
    brands = [
        'Apple', 'Samsung', 'Sony', 'Microsoft', 'Dell', 'HP', 'LG', 'Asus', 'Lenovo', 'Acer',
        'Nike', 'Adidas', 'Puma', 'Under Armour', 'Levi\'s', 'Gap', 'H&M', 'Zara', 'Ralph Lauren', 'Calvin Klein',
        'Amazon', 'Google', 'Logitech', 'JBL', 'Bose', 'Philips', 'Panasonic', 'Dyson', 'Bosch', 'KitchenAid',
        'IKEA', 'Ashley', 'Pottery Barn', 'Wayfair', 'Crate & Barrel', 'West Elm', 'La-Z-Boy', 'Herman Miller', 'Steelcase', 'Ethan Allen',
        'Tiffany & Co', 'Pandora', 'Swarovski', 'Cartier', 'Rolex', 'Fossil', 'Michael Kors', 'Coach', 'Kate Spade', 'Gucci',
        'LEGO', 'Mattel', 'Hasbro', 'Fisher-Price', 'Playmobil', 'Nintendo', 'Ravensburger', 'Melissa & Doug', 'Hot Wheels', 'Barbie',
        'Purina', 'Pedigree', 'Royal Canin', 'Hill\'s', 'Blue Buffalo', 'Friskies', 'Kong', 'Petmate', 'Whiskas', 'Iams'
    ]
    
    # Create output file and write header
    with open('data/products.csv', 'w') as f:
        f.write('product_id,name,category,price,in_stock\n')
    
    total_records = 0
    
    # Generate data in chunks
    for chunk_start in range(0, num_records, chunk_size):
        chunk_end = min(chunk_start + chunk_size, num_records)
        chunk_size_actual = chunk_end - chunk_start
        
        # Generate product IDs for this chunk
        product_ids = [f'P{str(i+1).zfill(4)}' for i in range(chunk_start, chunk_end)]
        
        # Generate random data for this chunk
        data = {
            'product_id': product_ids,
            'name': [f"{random.choice(brands)} {random.choice(product_names)} {random.randint(1, 999)}" for _ in range(chunk_size_actual)],
            'category': [random.choice(categories) for _ in range(chunk_size_actual)],
            'price': [round(random.uniform(10, 1000), 2) for _ in range(chunk_size_actual)],
            'in_stock': [random.choice([True, False]) for _ in range(chunk_size_actual)]
        }
        
        # Create dataframe for this chunk and append to file
        chunk_df = pd.DataFrame(data)
        chunk_df.to_csv('data/products.csv', index=False, mode='a', header=False)
        total_records += chunk_size_actual
    
    return total_records

def generate_orders(num_records=10000, chunk_size=1000):
    # Create output file and write header
    with open('data/orders.csv', 'w') as f:
        f.write('order_id,customer_id,order_date,total_amount,status\n')
    
    total_records = 0
    
    # Order statuses with weighted probabilities
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned', 'On Hold', 'Refunded']
    status_weights = [0.1, 0.15, 0.2, 0.4, 0.05, 0.05, 0.03, 0.02]  # More realistic distribution
    
    # Generate data in chunks
    for chunk_start in range(0, num_records, chunk_size):
        chunk_end = min(chunk_start + chunk_size, num_records)
        chunk_size_actual = chunk_end - chunk_start
        
        # Generate order IDs for this chunk
        order_ids = [f'O{str(i+1).zfill(4)}' for i in range(chunk_start, chunk_end)]
        
        # Random order dates within the last year, with more recent dates being more common
        current_date = datetime(2023, 3, 31)
        days_ago = [int(abs(random.expovariate(0.015))) for _ in range(chunk_size_actual)]
        days_ago = [min(d, 365) for d in days_ago]  # Cap at 365 days
        
        # Generate order times that follow a realistic pattern (more orders during business hours)
        hours = [random.choices(range(0, 24), weights=[
            0.01, 0.005, 0.002, 0.001, 0.002, 0.01,  # 0-5 AM (low)
            0.02, 0.05, 0.08, 0.09, 0.1, 0.1,        # 6-11 AM (rising)
            0.1, 0.08, 0.09, 0.1, 0.1, 0.09,         # 12-5 PM (peak)
            0.08, 0.07, 0.05, 0.04, 0.02, 0.01       # 6-11 PM (declining)
        ])[0] for _ in range(chunk_size_actual)]
        
        minutes = [random.randint(0, 59) for _ in range(chunk_size_actual)]
        seconds = [random.randint(0, 59) for _ in range(chunk_size_actual)]
        
        # Generate random data for this chunk
        data = {
            'order_id': order_ids,
            'customer_id': [f'C{str(random.randint(1, min(1000, num_records))).zfill(4)}' for _ in range(chunk_size_actual)],
            'order_date': [(current_date - timedelta(days=days_ago[i], 
                                                   hours=0, 
                                                   minutes=0, 
                                                   seconds=0)).replace(hour=hours[i], 
                                                                     minute=minutes[i], 
                                                                     second=seconds[i]).strftime('%Y-%m-%d %H:%M:%S') 
                          for i in range(chunk_size_actual)],
            'total_amount': [round(random.uniform(20, 1500) * (1 + random.random() * 0.2), 2) for _ in range(chunk_size_actual)],  # More variance
            'status': [random.choices(statuses, weights=status_weights)[0] for _ in range(chunk_size_actual)]
        }
        
        # Create dataframe for this chunk and append to file
        chunk_df = pd.DataFrame(data)
        chunk_df.to_csv('data/orders.csv', index=False, mode='a', header=False)
        total_records += chunk_size_actual
    
    return total_records

def main():
    # Check for command line arguments
    num_records = 10000  # Default
    if len(sys.argv) > 1:
        try:
            num_records = int(sys.argv[1])
            print(f"Will generate {num_records} records per dataset")
        except ValueError:
            print(f"Invalid number of records: {sys.argv[1]}, using default 10000")
            num_records = 10000
    
    # Calculate appropriate chunk size based on number of records
    chunk_size = min(1000, max(100, num_records // 10))  # Ensure chunks are between 100 and 1000
    
    print("Generating sample data...")
    print(f"Using chunk size of {chunk_size} for memory efficiency")
    
    customers_count = generate_customers(num_records, chunk_size)
    products_count = generate_products(num_records, chunk_size)
    orders_count = generate_orders(num_records, chunk_size)
    
    print("Sample data generated successfully!")
    print(f"Customers: {customers_count} records")
    print(f"Products: {products_count} records")
    print(f"Orders: {orders_count} records")

if __name__ == "__main__":
    main() 