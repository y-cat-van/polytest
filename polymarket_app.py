import streamlit as st
import requests
import json
import urllib3
from urllib.parse import urlparse
import pandas as pd
import time
import threading
import websocket

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

def get_slug_from_url(url):
    """Parse the slug from a Polymarket URL."""
    path = urlparse(url).path
    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "event":
        return parts[1]
    if len(parts) > 0:
        return parts[-1]
    return url

def parse_json_field(field_value):
    if isinstance(field_value, str):
        try:
            return json.loads(field_value)
        except json.JSONDecodeError:
            return []
    return field_value

def fetch_market_data(slug):
    try:
        url = f"{GAMMA_API_URL}/events"
        params = {"slug": slug}
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return None, "No event found."
        
        if isinstance(data, list):
            if len(data) == 0:
                return None, "No event found."
            return data[0].get("markets", []), None
        return data.get("markets", []), None
    except Exception as e:
        return None, f"Error fetching event: {str(e)}"

# --- WebSocket Manager ---
class PolymarketWSManager:
    def __init__(self):
        self.prices = {} # {token_id: {'bid': ..., 'ask': ...}}
        self.ws = None
        self.ws_thread = None
        self.subscribed_tokens = set()
        self.lock = threading.Lock()
        self.is_running = False

    def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.ws_thread = threading.Thread(target=self._run_ws, daemon=True)
        self.ws_thread.start()
        # Keep alive ping thread
        threading.Thread(target=self._keep_alive, daemon=True).start()

    def _keep_alive(self):
        while self.is_running:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                try:
                    self.ws.send("PING")
                except:
                    pass
            time.sleep(10)

    def _run_ws(self):
        # Disable websocket debug prints
        # websocket.enableTrace(True) 
        while self.is_running:
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever()
                time.sleep(2) # Reconnect delay
            except Exception as e:
                print(f"WS Error: {e}")
                time.sleep(5)

    def _on_open(self, ws):
        print("WS Connected")
        # Resubscribe if needed
        with self.lock:
            if self.subscribed_tokens:
                self._subscribe(list(self.subscribed_tokens))

    def _on_message(self, ws, message):
        try:
            msgs = json.loads(message)
            if not isinstance(msgs, list):
                msgs = [msgs]
                
            for msg in msgs:
                event_type = msg.get("event_type")
                
                # Initial Snapshot
                if event_type == "book":
                    asset_id = msg.get("asset_id")
                    bids = msg.get("bids", [])
                    asks = msg.get("asks", [])
                    self._update_price_from_book(asset_id, bids, asks)
                
                # Incremental Updates
                elif event_type == "price_change":
                    changes = msg.get("price_changes", [])
                    for change in changes:
                        asset_id = change.get("asset_id")
                        # The update message directly contains best_bid/best_ask!
                        # BUT, checking docs and previous logs, sometimes it might be missing or different.
                        # Let's trust it if present.
                        bb = change.get("best_bid")
                        ba = change.get("best_ask")
                        
                        # IMPORTANT: If best_bid/ask are NOT in the change message, it means they didn't change?
                        # Or do we need to maintain local orderbook state?
                        # For simple monitoring, if they are missing, we might keep old values.
                        # But wait, `_update_price_direct` handles N/A by keeping old values.
                        # However, let's verify if we need to parse `price` and `side` to update a local full book?
                        # Maintaining a full local book is complex (matching sizes, etc).
                        # Polymarket WS "price_change" event usually sends the NEW best bid/ask if they changed.
                        
                        self._update_price_direct(asset_id, bb, ba)
                
                # Handling "market" type or others if any
                        
        except Exception as e:
            print(f"Msg Parse Error: {e}")

    def _on_error(self, ws, error):
        print(f"WS Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print("WS Closed")

    def subscribe(self, token_ids):
        with self.lock:
            new_tokens = [t for t in token_ids if t not in self.subscribed_tokens]
            if new_tokens:
                self.subscribed_tokens.update(new_tokens)
                if self.ws and self.ws.sock and self.ws.sock.connected:
                    self._subscribe(new_tokens)

    def _subscribe(self, token_ids):
        msg = {
            "assets_ids": token_ids,
            "type": "market"
        }
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                self.ws.send(json.dumps(msg))
            except Exception as e:
                print(f"WS Send Error: {e}")

    def _update_price_from_book(self, token_id, bids, asks):
        # Polymarket CLOB: Bids are ASC (last is best?), Asks are DESC (last is best?)
        # Wait, previous manual check showed:
        # Bids: [0.01, 0.02, ... 0.99] -> Max is Best
        # Asks: [0.99, 0.98, ... 0.01] -> Min is Best
        
        best_bid = "N/A"
        if bids:
            # Safely find max bid
            try:
                best_bid = max(bids, key=lambda x: float(x["price"]))["price"]
            except: pass
            
        best_ask = "N/A"
        if asks:
            # Safely find min ask
            try:
                best_ask = min(asks, key=lambda x: float(x["price"]))["price"]
            except: pass
            
        with self.lock:
            self.prices[token_id] = {"bid": best_bid, "ask": best_ask}

    def _update_price_direct(self, token_id, bid, ask):
        with self.lock:
            # Only update if valid values are provided, or keep existing
            current = self.prices.get(token_id, {"bid": "N/A", "ask": "N/A"})
            new_bid = bid if bid is not None else current["bid"]
            new_ask = ask if ask is not None else current["ask"]
            self.prices[token_id] = {"bid": new_bid, "ask": new_ask}

    def get_price(self, token_id):
        with self.lock:
            return self.prices.get(token_id, {"bid": "Loading...", "ask": "Loading..."})

    def warmup_cache(self, token_ids):
        """Fetches initial prices via HTTP REST API for instant display."""
        def fetch_single(token_id):
            try:
                # Use global CLOB_API_URL
                url = f"{CLOB_API_URL}/book"
                params = {"token_id": token_id}
                response = requests.get(url, params=params, verify=False, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    self._update_price_from_book(token_id, bids, asks)
            except Exception as e:
                print(f"Warmup error for {token_id}: {e}")

        # Run in a separate thread to not block the main UI thread completely,
        # but Streamlit spinner will wait for the block if we put it there.
        # Actually, threading is better so UI renders "Loading" then updates as HTTP returns.
        threading.Thread(target=lambda: self._warmup_worker(token_ids, fetch_single), daemon=True).start()

    def _warmup_worker(self, token_ids, fetch_func):
        # Use ThreadPool to fetch concurrently
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(fetch_func, token_ids)

# Singleton for Streamlit
@st.cache_resource
def get_ws_manager():
    manager = PolymarketWSManager()
    manager.start()
    return manager

# --- Streamlit App ---
st.set_page_config(page_title="Polymarket Price Checker (WebSocket)", layout="wide")
st.title("Polymarket Real-time Price Checker (WebSocket ⚡)")

ws_manager = get_ws_manager()

# Sidebar
with st.sidebar:
    st.header("Settings")
    url_input = st.text_input("Polymarket URL / Slug:", value="https://polymarket.com/event/russia-x-ukraine-ceasefire-by-january-31-2026")
    start_monitoring = st.toggle("Start Monitoring")
    
    # Even with WS, UI needs to rerender to show new data.
    # 0.5s UI refresh rate is smooth enough without overloading browser.
    ui_refresh_rate = st.slider("UI Refresh Rate (seconds)", 0.1, 2.0, 0.5) 

if start_monitoring:
    if not url_input:
        st.warning("Please enter a URL.")
    else:
        # Initialize structure
        if "market_structure" not in st.session_state or st.session_state.get("current_url") != url_input:
            with st.spinner("Fetching market info..."):
                slug = get_slug_from_url(url_input)
                markets, error = fetch_market_data(slug)
                
                if error:
                    st.error(error)
                    st.stop()
                elif not markets:
                    st.warning("No markets found.")
                    st.stop()
                
                all_tokens = []
                token_ids_to_sub = []
                
                for m in markets:
                    question = m.get("question", "Unknown")
                    outcomes = parse_json_field(m.get("outcomes", []))
                    clob_token_ids = parse_json_field(m.get("clobTokenIds", []))
                    
                    for idx, token_id in enumerate(clob_token_ids):
                        outcome_name = outcomes[idx] if idx < len(outcomes) else f"Outcome {idx}"
                        all_tokens.append({
                            "question": question,
                            "outcome": outcome_name,
                            "token_id": token_id
                        })
                        token_ids_to_sub.append(token_id)
                
                # Subscribe to WS
                ws_manager.subscribe(token_ids_to_sub)
                # Warmup cache with HTTP for instant data
                ws_manager.warmup_cache(token_ids_to_sub)
                
                st.session_state["market_structure"] = all_tokens
                st.session_state["current_url"] = url_input
                st.success(f"Connected! Monitoring {len(all_tokens)} outcomes.")

        # Display Loop
        all_tokens = st.session_state["market_structure"]
        table_placeholder = st.empty()
        status_placeholder = st.empty()
        
        while True:
            # Build DataFrame from local memory (instant)
            results = []
            for t in all_tokens:
                price = ws_manager.get_price(t["token_id"])
                results.append({
                    "Question": t["question"],
                    "Outcome": t["outcome"],
                    "Best Bid": price["bid"],
                    "Best Ask": price["ask"],
                    # "Token ID": t["token_id"] # Optional, hide to keep clean
                })
            
            df = pd.DataFrame(results)
            with table_placeholder.container():
                st.dataframe(df, use_container_width=True, hide_index=True)
            
            status_placeholder.caption(f"Live Updates via WebSocket • Last UI Render: {time.strftime('%H:%M:%S')}")
            time.sleep(ui_refresh_rate)

else:
    st.info("Enter URL and toggle 'Start Monitoring'.")
