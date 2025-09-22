import os
import pandas as pd
from lineagekit.dataset import dataset
from lineagekit.transform import transform

VARIANT = os.getenv("LINEAGE_VARIANT", "A").upper()

@dataset(name="orders_raw", io="read", fmt="csv")
def load_orders():
    # Base data, deterministic across runs
    df = pd.DataFrame({
        "order_id": [1,2,3,4,5,6,7,8,9,10,11,12],
        "qty":      [2,1,5,3,2,1,10,4,2,7,1,3],
        "price":    [10.00,5.00,7.50,12.99,3.25,99.95,1.99,15.49,0.99,8.75,250.00,19.99],
        "cust_id":  [100,200,100,300,100,400,250,300,200,100,500,250],
    })
    if VARIANT == "B":
        # Trigger a TYPE_CHANGE on qty (int -> float)
        df["qty"] = df["qty"].astype(float)
        # Trigger a NULL_SPIKE on price (~25% NaNs)
        df.loc[[2,6,10], "price"] = [None, None, None]
    return df

@transform(
    name="clean_orders",
    produces="orders_cleaned",
    passthrough=["order_id", "qty", "price"],
    rename={"cust_id": "customer_id"},
    derives={"total": ["qty", "price"]}
)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={"cust_id": "customer_id"}).copy()
    out["total"] = out["qty"] * out["price"]
    if VARIANT == "B":
        # Optional schema add to demonstrate SCHEMA_ADD
        out["promo_applied"] = (out["order_id"] % 2 == 0)
    return out

@dataset(name="orders_sink", io="write", fmt="parquet")
def write_orders(df: pd.DataFrame, out_path: str):
    # No actual file write for this test
    pass

def main():
    orders = load_orders()
    cleaned = clean(orders)
    write_orders(cleaned, "data/clean/orders.parquet")

if __name__ == "__main__":
    main()
