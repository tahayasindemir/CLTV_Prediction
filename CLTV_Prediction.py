from sqlalchemy import create_engine
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter
from lifetimes.plotting import plot_period_transactions
from sklearn.preprocessing import MinMaxScaler

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: '%.4f' % x)


def outlier_thresholds(dataframe, variable):
    quartile1 = dataframe[variable].quantile(0.01)
    quartile3 = dataframe[variable].quantile(0.99)
    interquantile_range = quartile3 - quartile1
    up_limit = quartile3 + 1.5 * interquantile_range
    low_limit = quartile1 - 1.5 * interquantile_range
    return low_limit, up_limit


def replace_with_thresholds(dataframe, variable):
    low_limit, up_limit = outlier_thresholds(dataframe, variable)
    dataframe.loc[(dataframe[variable] < low_limit), variable] = low_limit
    dataframe.loc[(dataframe[variable] > up_limit), variable] = up_limit


df__ = pd.read_excel(r"C:\Users\taha\Desktop\DSMLBC6\Ders Notları\HAFTA_03\online_retail_II.xlsx",
                     sheet_name="Year 2010-2011")
df = df__.copy()
df.shape

df.dropna(inplace=True)

df = df[~df["Invoice"].str.contains("C", na=False)]

df = df[df["Price"] > 0]

replace_with_thresholds(df, "Quantity")

replace_with_thresholds(df, "Price")

df["TotalPrice"] = df["Quantity"] * df["Price"]

today_date = dt.datetime(2011, 12, 11)

cltv_df = df.groupby('CustomerID').agg({'InvoiceDate': [lambda date: (date.max() - date.min()).days,
                                                        lambda date: (today_date - date.min()).days],
                                        'Invoice': lambda num: num.nunique(),
                                        'TotalPrice': lambda total_price: total_price.sum()})

cltv_df.columns = cltv_df.columns.droplevel(0)
cltv_df.columns = ['recency', 'T', 'frequency', 'monetary']

cltv_df["monetary"] = cltv_df["monetary"] / cltv_df["frequency"]
cltv_df = cltv_df[(cltv_df['frequency'] > 1)]
cltv_df["recency"] = cltv_df["recency"] / 7
cltv_df["T"] = cltv_df["T"] / 7

bgf = BetaGeoFitter(penalizer_coef=0.001)
bgf.fit(cltv_df['frequency'],
        cltv_df['recency'],
        cltv_df['T'])

ggf = GammaGammaFitter(penalizer_coef=0.01)
ggf.fit(cltv_df['frequency'], cltv_df['monetary'])
cltv_df["expected_average_profit"] = ggf.conditional_expected_average_profit(cltv_df['frequency'],
                                                                             cltv_df['monetary'])

cltv = ggf.customer_lifetime_value(bgf, cltv_df['frequency'], cltv_df['recency'], cltv_df['T'], cltv_df['monetary'],
                                   time=6, freq="W", discount_rate=0.01)

cltv = cltv.reset_index()
cltv_final = cltv_df.merge(cltv, on="CustomerID", how="left")
cltv_final.describe().T

cltv_1_month = ggf.customer_lifetime_value(bgf, cltv_df['frequency'], cltv_df['recency'], cltv_df['T'],
                                           cltv_df['monetary'], time=1, freq="W", discount_rate=0.01)

cltv_1_month = cltv_1_month.reset_index()
cltv_1_month_final = cltv_df.merge(cltv, on="CustomerID", how="left")
cltv_1_month_final.sort_values(by='clv', ascending=False).head(10).describe().T

cltv_12_month = ggf.customer_lifetime_value(bgf, cltv_df['frequency'], cltv_df['recency'], cltv_df['T'],
                                            cltv_df['monetary'], time=12, freq="W", discount_rate=0.01)

cltv_12_month = cltv_12_month.reset_index()
cltv_12_month_final = cltv_df.merge(cltv, on="CustomerID", how="left")
cltv_12_month_final.sort_values(by='clv', ascending=False).head(10).describe().T

cltv_final["segment"] = pd.qcut(cltv_final["clv"], 4, labels=["D", "C", "B", "A"])

cltv_final.groupby("segment").agg({"count", "mean", "sum"})


creds = {'user': 'user',
         'passwd': 'password',
         'host': 'host',
         'port': 'port',
         'db': 'db'
         }
connstr = 'connstr'

conn = create_engine(connstr.format(**creds))

cltv_final["CustomerID"] = cltv_final["CustomerID"].astype(int)

cltv_final.to_sql(name='user', con=conn, if_exists='replace', index=False)

pd.read_sql_query("show tables", conn)
