import pandas as pd
from sqlalchemy import create_engine

# Extract
df1 = pd.read_csv(r"C:\Users\HP\Desktop\Analysis Softwares\Phase 1 Project - Customer Orders ETL Pipeline\customers.csv")
df2 = pd.read_csv(r"C:\Users\HP\Desktop\Analysis Softwares\Phase 1 Project - Customer Orders ETL Pipeline\orders.csv")
df3 = pd.read_csv(r"C:\Users\HP\Desktop\Analysis Softwares\Phase 1 Project - Customer Orders ETL Pipeline\products.csv")

# Inspect
print(df1.columns)
print(df2.columns)
print(df3.columns)
df1.info()
df2.info()
df3.info()

# Transform
df1["full_name"] = df1["full_name"].str.strip().str.title()
df2["amount"] = df2["amount"].fillna(df2["amount"].mean())
df2 = df2.dropna(subset= ["order_date"])
df2["order_date"] = pd.to_datetime(df2["order_date"])
df2["order_month"] = df2["order_date"].dt.month
completed = df2[df2["status"] == "completed"]
print(completed)

# Merge
merged = pd.merge(completed, df1, on= "customer_id", how="left")
final_merged = pd.merge(merged, df3, on="product_id", how="left")

# Enrich
final_merged["order_size"] = final_merged["amount"].apply(lambda x: "large" if x>=1000 else "small")

# Aggregate
results = final_merged.groupby(["city", "category"]).agg(
    order_count = ("order_id", "count"),
    total_amount = ("amount", "sum")
).reset_index()
print(results)

# Load
engine = create_engine("postgresql://postgres:3619@localhost:5432/mydb")
results.to_sql(name="order_summary", con=engine, if_exists="replace", index=False)
print("Loaded Successfully!!!!")