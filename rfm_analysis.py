import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

raw_password = "workbench@24"
encoded_password = urllib.parse.quote_plus(raw_password)
engine = create_engine(f"mysql+pymysql://root:{encoded_password}@127.0.0.1:3306/retail_project1")

try:

    df = pd.read_sql("SELECT * FROM fact_sales", engine)
    print("CONNECTED TO MYSQL \n")
    
    print("RAW DATA:")
    print(df.head()) 
    
    
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    today = df['invoice_date'].max() + pd.Timedelta(days=1)

    rfm = df.groupby('customer_id').agg({
        'invoice_date': lambda x: (today - x.max()).days,
        'invoice_no': 'count',
        'revenue': 'sum'
    }).reset_index()

    rfm.columns = ['customer_id', 'recency', 'frequency', 'monetary']
    print("\nRFM TABLE:")
    print(rfm.head()) 

    rfm['r_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
    
    def get_segment(df):
        score = int(df['r_score']) + int(df['f_score'])
        if score >= 9: return 'Champions'
        elif score >= 7: return 'Loyal Customers'
        elif score >= 5: return 'Potential Loyalist'
        elif score >= 3: return 'New Customers'
        else: return 'Hibernating'

    rfm['Segment'] = rfm.apply(get_segment, axis=1)

 
    print("\nCUSTOMERS PER SEGMENT:")
    print(rfm['Segment'].value_counts())

    avg_spend = rfm.groupby('Segment')['monetary'].mean().sort_values(ascending=False)
    print("\nAVERAGE SPEND PER SEGMENT:")
    print(avg_spend)

    print("\nRFM FILE SAVED ")
    rfm.to_csv('rfm_output.csv', index=False)

except Exception as e:
    print(f"ERROR: {e}")