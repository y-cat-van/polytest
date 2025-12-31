import requests
import json
import time
import urllib3

urllib3.disable_warnings()

def get_slug(ts):
    return f"btc-updown-15m-{ts}"

# Calculate a timestamp for a round that should be closed (e.g., 2 hours ago)
now = int(time.time())
# Round down to 15m
current_block_start = (now // 900) * 900
slug = "eth-updown-15m-1767148200" # Based on image: 9:30PM ET start time = 21:30 ET
# 21:30 ET is likely around 1767148200 if 9:00AM was 1767103200 (diff 12.5h = 45000s)
# 1767103200 + 45000 = 1767148200.
# Let's try to fetch this specific market that is RESOLVED in the screenshot.
print(f"Fetching slug: {slug}")

url = "https://gamma-api.polymarket.com/events"
params = {"slug": slug}

try:
    resp = requests.get(url, params=params, verify=False)
    print(f"Status Code: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        # print(json.dumps(data, indent=2))
        
        if isinstance(data, list) and len(data) > 0:
            market = data[0]["markets"][0]
            print(f"Market ID: {market.get('id')}")
            print(f"Question: {market.get('question')}")
            print(f"Closed: {market.get('closed')}")
            
            # Check outcomes/tokens in event response
            print("Event Response - Markets[0] keys:", market.keys())
            print(f"Outcomes: {market.get('outcomes')}")
            print(f"Outcome Prices: {market.get('outcomePrices')}")
            print(f"UMA Resolution Status: {market.get('umaResolutionStatus')}")
            
            # Now fetch the specific market details which might have more info
            m_id = market.get("id")
            m_url = f"https://gamma-api.polymarket.com/markets/{m_id}"
            print(f"\nFetching Market Details: {m_url}")
            m_resp = requests.get(m_url, verify=False)
            if m_resp.status_code == 200:
                m_data = m_resp.json()
                # print(json.dumps(m_data, indent=2))
                
                print(f"Market Closed: {m_data.get('closed')}")
                if "tokens" in m_data:
                    for t in m_data["tokens"]:
                        print(f"Token: {t.get('outcome')} | Winner: {t.get('winner')}")
                else:
                    print("No tokens found in market details.")
            else:
                print("Failed to fetch market details.")
        else:
            print("No event found for slug.")
    else:
        print("Failed to fetch event.")
except Exception as e:
    print(f"Error: {e}")
