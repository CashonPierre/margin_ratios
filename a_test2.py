import requests
import re

def get_holdings_count_direct(ticker):
    # We use a reliable financial mirror that provides static HTML
    url = f"https://www.marketwatch.com/investing/fund/{ticker}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return f"Site Error: {response.status_code}"
        
        # We look for the 'Number of Holdings' label in the text
        # and grab the number immediately following it
        html = response.text
        match = re.search(r'Number of Holdings.*?primary">(.*?)</span>', html, re.DOTALL | re.IGNORECASE)
        
        if match:
            return match.group(1).strip()
        
        # Fallback for different site layouts
        match_alt = re.search(r'Holdings.*?(\d+[,.]?\d*)', html, re.IGNORECASE)
        return match_alt.group(1) if match_alt else "Field not found"
        
    except Exception as e:
        return f"Error: {e}"

# Test it now
for t in ['SPY', 'VWO']:
    print(f"{t} Holdings Count: {get_holdings_count_direct(t)}")