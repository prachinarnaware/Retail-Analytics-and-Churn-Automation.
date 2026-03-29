import pandas as pd
import numpy as np
import urllib.parse
from sqlalchemy import create_engine
from scipy import stats


raw_pw = "workbench@24"
encoded_pw = urllib.parse.quote_plus(raw_pw)
engine = create_engine(f"mysql+pymysql://root:{encoded_pw}@127.0.0.1:3306/retail_project1")

def run_project():
    try:

        print("Connecting to Database...")
        df = pd.read_sql("SELECT * FROM fact_sales", engine)
        df['invoice_date'] = pd.to_datetime(df['invoice_date'])
        print("1. SQL Connection: OK ")
        snapshot = df['invoice_date'].max() + pd.Timedelta(days=1)

        rfm = df.groupby('customer_id').agg({
            'invoice_date': lambda x: (snapshot - x.max()).days,
            'invoice_no': 'nunique',
            'revenue': 'sum'
        }).rename(columns={
            'invoice_date': 'recency',
            'invoice_no': 'frequency',
            'revenue': 'monetary'
        })

       
        rfm['Segment'] = pd.qcut(
            rfm['monetary'],
            5,
            labels=['Low', 'Mid-Low', 'Medium', 'High', 'VIP']
        )

        print("2. RFM Calculation: OK ")

        
        groups = [group['monetary'].values for name, group in rfm.groupby('Segment')]

        f_stat, p_val = stats.f_oneway(*groups)

        print("\n--- STATISTICAL AUDIT ---")
        print("F-Statistic:", f_stat)
        print("P-Value:", p_val)

        if p_val < 0.05:
            print("Result: Valid Segmentation! ")
        else:
            print("Result: Weak Segmentation ")
           
            rfm.to_csv("rfm_segments.csv", index=True)
            print("RFM file saved as rfm_segments.csv ")

    except Exception as e:
        print(f"Error jhala aahe: {e}")

if __name__ == "__main__":
    run_project()
