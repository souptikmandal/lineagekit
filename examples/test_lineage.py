import pandas as pd
from lineagekit.dataset import dataset
from lineagekit.transform import transform


@dataset(name="orders_raw", io="read", fmt="csv")
def load_orders(p): return pd.read_csv(p)

@transform(
    name="clean_orders",
    produces="orders_cleaned",
    passthrough=["order_id", "qty", "price"],
    rename={"cust_id": "customer_id"},
    derives={"total": ["qty", "price"]}   # total = qty*price
)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    return (df.assign(total=df["qty"] * df["price"])
              .rename(columns={"cust_id": "customer_id"}))

orders = load_orders("orders.csv")
cleaned = clean(orders)