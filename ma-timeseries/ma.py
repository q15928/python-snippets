import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as pdr

style.use('ggplot')


def retrieve_data(start_dt, end_dt, symbol_list):
    """Collect the stock market data"""
    df = pd.DataFrame()
    for s in symbol_list:
        tmp_df = pdr.DataReader(s, 'yahoo', start_dt, end_dt)
        tmp_df['Symbol'] = s
        if df.empty:
            df = tmp_df
        else:
            df = df.append(tmp_df)

    df.to_csv('./data/stocks.csv')
    return df


def compute_ma(df, window):
    """Compute the moving average of Adj Close for each Symbol"""
    ma_name = 'ma{0:d}'.format(window)
    df[ma_name] = (df.groupby('Symbol', sort=False)['Adj Close']
                   .rolling(window).mean()
                   .reset_index(0, drop=True))
    return df


if __name__ == '__main__':
    start = dt.datetime(2012, 1, 1)
    end = dt.datetime(2019, 6, 30)
    symbol_list = ['GOOG', 'AAPL']
    df = retrieve_data(start, end, symbol_list)
    df = compute_ma(df, 20)
    df = compute_ma(df, 50)
    df[df['Symbol'] == 'GOOG'][['Adj Close', 'ma20', 'ma50']]\
        .plot(title='Google')
    plt.show()
