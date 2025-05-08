import os
import json
import pandas as pd

# Load the OpenAPI spec
base_path = os.path.dirname(__file__)
with open(os.path.join(base_path, 'Northwind-V3.json'), 'r') as f:
    spec = json.load(f)

# Choose an endpoint (e.g., /Products)
endpoint = "/Products"

# Extract field names for the endpoint from OpenAPI "parameters" or inferred manually
fields = ["ProductID", "ProductName", "SupplierID", "CategoryID", "QuantityPerUnit", "UnitPrice", "UnitsInStock", "UnitsOnOrder", "ReorderLevel", "Discontinued"]

# Sample mock data (generate 3 rows)
data = [
    [1, "Chai", 1, 1, "10 boxes x 20 bags", 18.0, 39, 0, 10, False],
    [2, "Chang", 1, 1, "24 - 12 oz bottles", 19.0, 17, 40, 25, False],
    [3, "Aniseed Syrup", 1, 2, "12 - 550 ml bottles", 10.0, 13, 70, 25, False],
]

# Convert to DataFrame
df = pd.DataFrame(data, columns=fields)

# Output to CSV
df.to_csv(os.path.join(base_path, "Products.csv"), index=False)
print("✅ Products.csv generated successfully.")

# ----------------------------
# /Orders
orders_fields = ["OrderID", "CustomerID", "EmployeeID", "OrderDate", "RequiredDate", "ShippedDate", "ShipVia", "Freight", "ShipName"]
orders_data = [
    [10248, "VINET", 5, "1996-07-04", "1996-08-01", "1996-07-16", 3, 32.38, "Vins et alcools Chevalier"],
    [10249, "TOMSP", 6, "1996-07-05", "1996-08-16", "1996-07-10", 1, 11.61, "Toms Spezialitäten"],
    [10250, "HANAR", 4, "1996-07-08", "1996-08-05", "1996-07-12", 2, 65.83, "Hanari Carnes"]
]
df_orders = pd.DataFrame(orders_data, columns=orders_fields)
df_orders.to_csv(os.path.join(base_path, "Orders.csv"), index=False)
print("✅ Orders.csv generated successfully.")

# ----------------------------
# /Customers
customers_fields = ["CustomerID", "CompanyName", "ContactName", "ContactTitle", "Address", "City", "PostalCode", "Country", "Phone"]
customers_data = [
    ["VINET", "Vins et alcools Chevalier", "Paul Henriot", "Accounting Manager", "59 rue de l'Abbaye", "Reims", "51100", "France", "26.47.15.10"],
    ["TOMSP", "Toms Spezialitäten", "Karin Josephs", "Marketing Manager", "Luisenstr. 48", "Münster", "44087", "Germany", "0251-031259"],
    ["HANAR", "Hanari Carnes", "Mario Pontes", "Accounting Manager", "Rua do Paço, 67", "Rio de Janeiro", "05454-876", "Brazil", "(21) 555-0091"]
]
df_customers = pd.DataFrame(customers_data, columns=customers_fields)
df_customers.to_csv(os.path.join(base_path, "Customers.csv"), index=False)
print("✅ Customers.csv generated successfully.")

# ----------------------------
# /Employees
employees_fields = ["EmployeeID", "LastName", "FirstName", "Title", "BirthDate", "HireDate", "City", "Country"]
employees_data = [
    [1, "Davolio", "Nancy", "Sales Representative", "1948-12-08", "1992-05-01", "Seattle", "USA"],
    [2, "Fuller", "Andrew", "Vice President, Sales", "1952-02-19", "1992-08-14", "Tacoma", "USA"],
    [3, "Leverling", "Janet", "Sales Representative", "1963-08-30", "1992-04-01", "Kirkland", "USA"]
]
df_employees = pd.DataFrame(employees_data, columns=employees_fields)
df_employees.to_csv(os.path.join(base_path, "Employees.csv"), index=False)
print("✅ Employees.csv generated successfully.")

# ----------------------------
# /Order_Details
order_details_fields = ["OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"]
order_details_data = [
    [10248, 11, 14.0, 12, 0.0],
    [10248, 42, 9.8, 10, 0.0],
    [10248, 72, 34.8, 5, 0.0]
]
df_order_details = pd.DataFrame(order_details_data, columns=order_details_fields)
df_order_details.to_csv(os.path.join(base_path, "Order_Details.csv"), index=False)
print("✅ Order_Details.csv generated successfully.")