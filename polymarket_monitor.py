import requests
import time
import json
import sys
import urllib3
from urllib.parse import urlparse

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

def get_slug_from_url(url):
    """Parse the slug from a Polymarket URL."""
    path = urlparse(url).path
    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "event":
        return parts[1]
    # Handle cases like /market/slug or just the slug
    if len(parts) > 0:
        return parts[-1]
    return url

def get_event_markets(slug):
    """Fetch event details to get markets and token IDs."""
    try:
        url = f"{GAMMA_API_URL}/events"
        params = {"slug": slug}
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return []
        
        if isinstance(data, list):
            # API returns a list, usually the first one is what we want if slug is unique
            if len(data) == 0:
                return []
            return data[0].get("markets", [])
        return data.get("markets", [])
    except Exception as e:
        print(f"Error fetching event: {e}")
        return []

def get_market_data(token_id):
    """Fetch market data for a specific token ID."""
    try:
        url = f"{GAMMA_API_URL}/markets"
        params = {"clob_token_ids[]": token_id}
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        print(f"Error fetching market data: {e}")
        return None

def parse_json_field(field_value):
    """Helper to parse fields that might be JSON strings or lists."""
    if isinstance(field_value, str):
        try:
            return json.loads(field_value)
        except json.JSONDecodeError:
            return []
    return field_value

def main():
    print("Polymarket Real-time Price Monitor")
    print("----------------------------------")
    
    url_input = input("Please enter the Polymarket Market URL or Slug: ").strip()
    if not url_input:
        print("Empty input. Exiting.")
        return

    slug = get_slug_from_url(url_input)
    print(f"Resolving slug: {slug}...")
    
    markets = get_event_markets(slug)
    
    if not markets:
        print("No markets found for this URL/Slug.")
        return

    # Prepare list of tokens to monitor
    # Each market in the event might have multiple outcomes (tokens)
    monitored_tokens = []
    
    print(f"Found {len(markets)} market(s) associated with this event.")
    
    for m in markets:
        question = m.get("question", "Unknown Question")
        outcomes = parse_json_field(m.get("outcomes", []))
        clob_token_ids = parse_json_field(m.get("clobTokenIds", []))
        
        print(f"\nMarket: {question}")
        
        # Usually outcomes and clobTokenIds match by index
        # For Binary (Yes/No), usually 0 is Yes, 1 is No (or vice versa, check outcomes)
        # Polymarket UI usually shows the 'Yes' price.
        
        for idx, token_id in enumerate(clob_token_ids):
            outcome_name = outcomes[idx] if idx < len(outcomes) else f"Outcome {idx}"
            monitored_tokens.append({
                "question": question,
                "outcome": outcome_name,
                "token_id": token_id
            })
            print(f"  - Token ID: {token_id} ({outcome_name})")

    if not monitored_tokens:
        print("No valid token IDs found to monitor.")
        return

    print("\nFetching prices...")
    print("-" * 100)
    print(f"{'Time':<10} | {'Question':<40} | {'Outcome':<10} | {'Best Bid':<10} | {'Best Ask':<10}")
    print("-" * 100)

    # Fetch individually to ensure reliability
    current_time = time.strftime("%H:%M:%S")
    for t in monitored_tokens:
        try:
            url = f"{CLOB_API_URL}/book"
            params = {"token_id": t["token_id"]}
            response = requests.get(url, params=params, verify=False)
            response.raise_for_status()
            data = response.json()
            
            # data structure: {"bids": [{"price": "0.99", "size": "100"}, ...], "asks": [...]}
            # Bids are usually sorted desc, Asks asc.
            
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            best_bid = bids[0]["price"] if bids else "N/A"
            best_ask = asks[0]["price"] if asks else "N/A"
            
            # Truncate question for display
            q_display = (t['question'][:37] + '...') if len(t['question']) > 37 else t['question']
            
            print(f"{current_time} | {q_display:<40} | {t['outcome']:<10} | {best_bid:<10} | {best_ask:<10}")

        except Exception as req_err:
            print(f"Request Error for {t['outcome']}: {req_err}")

if __name__ == "__main__":
    main()
