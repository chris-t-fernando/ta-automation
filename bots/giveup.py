import btalib
import pandas as pd

# Read a csv file into a pandas dataframe
df = pd.read_csv("bots/data.txt", parse_dates=True, index_col="Date")
sma = btalib.sma(df)
cci = btalib.cci(df)
print("banana")
