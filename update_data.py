import os
import time
import pandas as pd
import yfinance as yf
import requests
from alpha_vantage.fundamentaldata import FundamentalData

#CONFIGURATION
START_DATE = "2005-01-01"
API_KEY = os.environ.get("ALPHA_VANTAGE_KEY")
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "ORCL", "INTC", "ADBE", "CSCO", "CRM"]

if not API_KEY:
    raise ValueError("La clé API Alpha Vantage est manquante dans les secrets.")

def get_prices_yfinance(symbol):
    try:
        ticker = yf.Ticker(symbol, session=session)
        hist = yf.download(symbol, start=START_DATE, interval="1d")
        
        if not hist.empty:
            hist.reset_index(inplace=True)
            
            filename = f"{symbol}_prices.csv"
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
        income_data.to_csv(f"{symbol}_financials_income.csv", index=False)
        print(f"Income Statement OK")
        time.sleep(2) 

        # Balance Sheet 
        balance_data, _ = fd.get_balance_sheet_quarterly(symbol)
        balance_data.to_csv(f"{symbol}_financials_balance.csv", index=False)
        print(f"Balance Sheet OK")
        time.sleep(2)

        # Cash Flow
        cash_data, _ = fd.get_cash_flow_quarterly(symbol)
        cash_data.to_csv(f"{symbol}_financials_cashflow.csv", index=False)
        print(f"Cash Flow OK")
        
    except Exception as e:
        print(f"Erreur Alpha Vantage pour {symbol}: {e}")
        if "Thank you" in str(e):
            print("LIMITE API ATTEINTE.")

#EXÉCUTION PRINCIPALE

for t in TICKERS:
    get_prices_yfinance(t)
    get_fundamentals_alpha(t)
    
    print("-" * 30)
    time.sleep(15) 

print("\n Mise à jour terminée.")
