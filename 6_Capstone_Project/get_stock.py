import pandas as pd
from yahooquery import Ticker
import datetime


def get_stock_data(ticker_list, date, save_path = None):
    """Function to get stock data of a list of stocks for a date (day)

    Args:
        ticker_list: list with ticker of the cryptos which the data should be retrieved. 
            Ex: ['BTC-USD', 'ETH-USD'] - bitcoin and etherium in dollar;
        date ('YYYY-MM-DD'): for which data this data should be retrived
    """
    date_formated = datetime.datetime.fromisoformat(date) - datetime.timedelta(days = 1)
    stock_data = Ticker(ticker_list)
    df = stock_data.history(start = date_formated, end = date_formated).reset_index()
    df.to_csv(save_path)




if __name__ == '__main__':
    price = get_stock_data(['BTC-USD', 'ETH-USD'], '2020-11-01', 'teste.csv')
    print(price)