import os
import time
import pandas as pd
import yfinance as yf
import requests
from alpha_vantage.fundamentaldata import FundamentalData


OUTPUT_FOLDER = "data"
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)


START_DATE = "2000-01-01"
API_KEY = os.environ.get("ALPHA_VANTAGE_KEY")
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "ORCL", "INTC", "ADBE", "CSCO", "CRM"]


if not API_KEY:
    raise ValueError("La clé API Alpha Vantage est manquante dans les secrets.")

def get_prices_yfinance(symbol):
    try:
        ticker = yf.Ticker(symbol)
        hist = yf.download(symbol, start=START_DATE, interval="1d")
        
        if not hist.empty:
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            hist['Ticker'] = symbol
            
            hist.reset_index(inplace=True)

            if 'Date' in hist.columns:
                hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d')
            
            filename = f"{OUTPUT_FOLDER}/{symbol}_prices.csv"
            hist.to_csv(filename, index=False)
            print(f"Prix sauvegardés : {filename}")
        else:
            print(f"Aucune donnée de prix trouvée pour {symbol}")
            
    except Exception as e:
        print(f"Erreur yfinance pour {symbol}: {e}")

def get_fundamentals_alpha(symbol):
    fd = FundamentalData(key=API_KEY, output_format='pandas')
    
    try:
        # Income Statement 
        income_data, _ = fd.get_income_statement_quarterly(symbol)
        income_data['Ticker'] = symbol
        income_data.to_csv(f"{OUTPUT_FOLDER}/{symbol}_financials_income.csv", index=False)
        print(f"Income Statement OK")
        time.sleep(5) 

        # Balance Sheet 
        balance_data, _ = fd.get_balance_sheet_quarterly(symbol)
        balance_data['Ticker'] = symbol
        balance_data.to_csv(f"{OUTPUT_FOLDER}/{symbol}_financials_balance.csv", index=False)
        print(f"Balance Sheet OK")
        time.sleep(5)

        # Cash Flow
        cash_data, _ = fd.get_cash_flow_quarterly(symbol)
        cash_data['Ticker'] = symbol
        cash_data.to_csv(f"{OUTPUT_FOLDER}/{symbol}_financials_cashflow.csv", index=False)
        print(f"Cash Flow OK")
        
    except Exception as e:
        print(f"Erreur Alpha Vantage pour {symbol}: {e}")
        if "Thank you" in str(e):
            print("LIMITE API ATTEINTE.")


for t in TICKERS:
    get_prices_yfinance(t)
    time.sleep(2)
    get_fundamentals_alpha(t)
    
    print("-" * 30)
    time.sleep(15) 

print("\n Mise à jour terminée.")
