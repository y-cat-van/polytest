import streamlit as st
import requests
import json
import urllib3
from urllib.parse import urlparse
import pandas as pd
import time
import threading
import websocket
import datetime
import os

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
HISTORY_FILE = "btc_15m_history.csv" # Legacy file, we will use round_history.csv for pairs
ROUND_HISTORY_FILE = "round_history.csv"
ARB_OPPORTUNITY_FILE = "arb_opportunities.csv"

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

def save_result(market_slug, result, end_time):
    """Append result to CSV."""
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, "a") as f:
        if not file_exists:
            f.write("market_slug,result,end_time,timestamp\n")
        f.write(f"{market_slug},{result},{end_time},{int(time.time())}\n")

def save_round_history(btc_slug, eth_slug):
    """Save round pair to CSV."""
    timestamp = int(time.time())
    file_exists = os.path.isfile(ROUND_HISTORY_FILE)
    # Check if this pair already exists to avoid duplicates
    if file_exists:
        try:
            df = pd.read_csv(ROUND_HISTORY_FILE)
            if not df.empty and ((df['btc_slug'] == btc_slug) & (df['eth_slug'] == eth_slug)).any():
                return
        except:
            pass
            
    with open(ROUND_HISTORY_FILE, "a") as f:
        if not file_exists:
            f.write("timestamp,btc_slug,eth_slug\n")
        f.write(f"{timestamp},{btc_slug},{eth_slug}\n")

def save_arb_opportunity(btc_slug, eth_slug, type_name, sum_val, btc_ask, eth_ask):
    """Save arbitrage opportunity."""
    timestamp = int(time.time())
    file_exists = os.path.isfile(ARB_OPPORTUNITY_FILE)
    with open(ARB_OPPORTUNITY_FILE, "a") as f:
        if not file_exists:
            f.write("timestamp,type,sum,btc_ask,eth_ask,btc_slug,eth_slug\n")
        f.write(f"{timestamp},{type_name},{sum_val},{btc_ask},{eth_ask},{btc_slug},{eth_slug}\n")

# --- WebSocket Manager (Reuse) ---
class PolymarketWSManager:
    def __init__(self):
        self.prices = {} 
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
                time.sleep(2)
            except Exception as e:
                print(f"WS Error: {e}")
                time.sleep(5)

    def _on_open(self, ws):
        print("WS Connected")
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
                if event_type == "book":
                    asset_id = msg.get("asset_id")
                    bids = msg.get("bids", [])
                    asks = msg.get("asks", [])
                    self._update_price_from_book(asset_id, bids, asks)
                elif event_type == "price_change":
                    changes = msg.get("price_changes", [])
                    for change in changes:
                        asset_id = change.get("asset_id")
                        bb = change.get("best_bid")
                        ba = change.get("best_ask")
                        self._update_price_direct(asset_id, bb, ba)
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
                    # Send ALL tokens to ensure additive/complete subscription
                    self._subscribe(list(self.subscribed_tokens))

    def _subscribe(self, token_ids):
        msg = {"assets_ids": token_ids, "type": "market"}
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                self.ws.send(json.dumps(msg))
            except Exception as e:
                print(f"WS Send Error: {e}")

    def _update_price_from_book(self, token_id, bids, asks):
        best_bid = "N/A"
        if bids:
            try: best_bid = max(bids, key=lambda x: float(x["price"]))["price"]
            except: pass
        best_ask = "N/A"
        if asks:
            try: best_ask = min(asks, key=lambda x: float(x["price"]))["price"]
            except: pass
        with self.lock:
            self.prices[token_id] = {"bid": best_bid, "ask": best_ask}

    def _update_price_direct(self, token_id, bid, ask):
        with self.lock:
            current = self.prices.get(token_id, {"bid": "N/A", "ask": "N/A"})
            new_bid = bid if bid is not None else current["bid"]
            new_ask = ask if ask is not None else current["ask"]
            self.prices[token_id] = {"bid": new_bid, "ask": new_ask}

    def get_price(self, token_id):
        with self.lock:
            return self.prices.get(token_id, {"bid": "Loading...", "ask": "Loading..."})

    def warmup_cache(self, token_ids):
        def fetch_single(token_id):
            try:
                url = f"{CLOB_API_URL}/book"
                params = {"token_id": token_id}
                response = requests.get(url, params=params, verify=False, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    self._update_price_from_book(token_id, bids, asks)
            except Exception as e:
                print(f"Warmup error: {e}")
        threading.Thread(target=lambda: self._warmup_worker(token_ids, fetch_single), daemon=True).start()

    def _warmup_worker(self, token_ids, fetch_func):
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(fetch_func, token_ids)

@st.cache_resource
def get_ws_manager():
    manager = PolymarketWSManager()
    manager.start()
    return manager

# --- Main App ---
st.set_page_config(page_title="Dual Crypto Monitor", layout="wide")

ws_manager = get_ws_manager()

# --- Navigation ---
page = st.sidebar.radio("Navigation", ["Monitor", "Arb History", "Market Results History"])

# --- Helper Functions ---
def get_latest_active_slug(prefix):
    """Find the currently active slug for a given prefix (btc/eth)."""
    try:
        now = int(time.time())
        # User confirmed: Slug uses START time.
        current_block_start = (now // 900) * 900
        return f"{prefix}-{current_block_start}"
    except:
        return f"{prefix}-0"

def get_next_slug(current_slug):
    try:
        parts = current_slug.split("-")
        last_ts = int(parts[-1])
        # Always increment by 900s
        next_ts = last_ts + 900
        return "-".join(parts[:-1]) + f"-{next_ts}"
    except:
        return current_slug

# --- PAGE: Monitor ---
if page == "Monitor":
    st.title("BTC & ETH 15m Series Monitor 🚀")

    with st.sidebar:
        st.header("Control Panel")
        btc_seed_slug = st.text_input("BTC Seed Slug:", value="btc-updown-15m-1767105000")
        eth_seed_slug = st.text_input("ETH Seed Slug:", value="eth-updown-15m-1767105900")
        auto_mode = st.toggle("Enable Auto-Rollover & Recording", value=True)
        
        if st.button("Clear History"):
            if os.path.exists(HISTORY_FILE): os.remove(HISTORY_FILE)
            if os.path.exists(ARB_OPPORTUNITY_FILE): os.remove(ARB_OPPORTUNITY_FILE)
            if os.path.exists(ROUND_HISTORY_FILE): os.remove(ROUND_HISTORY_FILE)
            st.success("History cleared!")

    # State Management
    if "current_btc_slug" not in st.session_state:
        st.session_state["current_btc_slug"] = get_latest_active_slug("btc-updown-15m")
        
    if "current_eth_slug" not in st.session_state:
        st.session_state["current_eth_slug"] = get_latest_active_slug("eth-updown-15m")
        
    # Min value tracking for Arb
    if "min_sum_1" not in st.session_state: st.session_state["min_sum_1"] = 1.0 # BTC Up + ETH Down
    if "min_sum_2" not in st.session_state: st.session_state["min_sum_2"] = 1.0 # BTC Down + ETH Up

    # Use st.empty() for the entire main area to prevent duplication
    main_placeholder = st.empty()

    with main_placeholder.container():
        # Fetch Data First
        btc_slug = st.session_state["current_btc_slug"]
        eth_slug = st.session_state["current_eth_slug"]
        
        btc_markets, _ = fetch_market_data(btc_slug)
        eth_markets, _ = fetch_market_data(eth_slug)
        
        btc_tokens = []
        eth_tokens = []
        btc_closed = False
        eth_closed = False
        
        # Process BTC
        if btc_markets:
            m = btc_markets[0]
            btc_closed = m.get("closed", False)
            if not btc_closed:
                outcomes = parse_json_field(m.get("outcomes", []))
                ids = parse_json_field(m.get("clobTokenIds", []))
                for i, tid in enumerate(ids):
                    o = outcomes[i] if i < len(outcomes) else str(i)
                    btc_tokens.append({"outcome": o, "token_id": tid})

        # Process ETH
        if eth_markets:
            m = eth_markets[0]
            eth_closed = m.get("closed", False)
            if not eth_closed:
                outcomes = parse_json_field(m.get("outcomes", []))
                ids = parse_json_field(m.get("clobTokenIds", []))
                for i, tid in enumerate(ids):
                    o = outcomes[i] if i < len(outcomes) else str(i)
                    eth_tokens.append({"outcome": o, "token_id": tid})

        # Subscribe & Warmup Combined
        all_token_ids = [t["token_id"] for t in btc_tokens] + [t["token_id"] for t in eth_tokens]
        if all_token_ids:
            ws_manager.subscribe(all_token_ids)
            # Warmup if needed (using a simple session state key check to avoid spamming REST API on every rerun)
            warmup_key = f"warmed_{btc_slug}_{eth_slug}"
            if not st.session_state.get(warmup_key):
                ws_manager.warmup_cache(all_token_ids)
                st.session_state[warmup_key] = True

        # --- Real-time Loop ---
        if not btc_closed and not eth_closed and btc_tokens and eth_tokens:
            price_placeholder = st.empty()
            
            while True:
                # Fetch Prices
                btc_prices = {t["outcome"]: ws_manager.get_price(t["token_id"]) for t in btc_tokens}
                eth_prices = {t["outcome"]: ws_manager.get_price(t["token_id"]) for t in eth_tokens}
                
                # Display
                with price_placeholder.container():
                    c1, c2 = st.columns(2)
                    with c1:
                        st.subheader("BTC Market")
                        st.caption(f"Slug: {btc_slug}")
                        
                        if btc_markets:
                            m = btc_markets[0]
                            st.info(m.get("question"))
                        
                        for t in btc_tokens:
                            p = btc_prices[t["outcome"]]
                            st.metric(f"BTC {t['outcome']}", f"Ask: {p['ask']}", f"Bid: {p['bid']}")
                    with c2:
                        st.subheader("ETH Market")
                        st.caption(f"Slug: {eth_slug}")
                        
                        if eth_markets:
                            m = eth_markets[0]
                            st.info(m.get("question"))
                            
                        for t in eth_tokens:
                            p = eth_prices[t["outcome"]]
                            st.metric(f"ETH {t['outcome']}", f"Ask: {p['ask']}", f"Bid: {p['bid']}")
                    
                    # EndDate Logic for Countdown
                    try:
                        end_date_iso = None
                        if btc_markets and len(btc_markets) > 0:
                            end_date_iso = btc_markets[0].get("endDate")
                            
                        if end_date_iso:
                            end_dt = datetime.datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
                            now_dt = datetime.datetime.now(datetime.timezone.utc)
                            seconds_left = (end_dt - now_dt).total_seconds()
                            
                            if seconds_left > 0:
                                mins = int(seconds_left // 60)
                                secs = int(seconds_left % 60)
                                progress = max(0.0, min(1.0, 1.0 - (seconds_left / 900.0)))
                                st.progress(progress, text=f"Time Remaining: {mins}m {secs}s")
                            else:
                                st.progress(1.0, text="Time Remaining: 0m 0s (Closing...)")
                        else:
                            parts = btc_slug.split("-")
                            end_ts = int(parts[-1])
                            now_ts = int(time.time())
                            seconds_left = max(0, end_ts - now_ts)
                            mins = seconds_left // 60
                            secs = seconds_left % 60
                            st.progress(max(0.0, min(1.0, 1.0 - (seconds_left / 900.0))), text=f"Time Remaining: {mins}m {secs}s")
                    except Exception as e:
                         st.write("Time Remaining: N/A")

                    # --- Calc Logic ---
                    try:
                        def safe_float(val):
                            try: return float(val)
                            except: return 999.0
                        
                        def get_ask(prices, outcome_name):
                            val = prices.get(outcome_name, {}).get("ask")
                            if val: return safe_float(val)
                            if outcome_name == "Yes": return safe_float(prices.get("Up", {}).get("ask"))
                            if outcome_name == "No": return safe_float(prices.get("Down", {}).get("ask"))
                            return 999.0

                        btc_up_ask = get_ask(btc_prices, "Yes")
                        btc_down_ask = get_ask(btc_prices, "No")
                        eth_up_ask = get_ask(eth_prices, "Yes")
                        eth_down_ask = get_ask(eth_prices, "No")
                        
                        sum_1 = btc_up_ask + eth_down_ask
                        sum_2 = btc_down_ask + eth_up_ask
                        
                        st.divider()
                        st.write(f"**BTC Up + ETH Down (Ask Sum): {sum_1:.4f}**")
                        st.write(f"**BTC Down + ETH Up (Ask Sum): {sum_2:.4f}**")
                        
                        # Record Logic
                        if sum_1 <= 0.8:
                            if sum_1 < st.session_state["min_sum_1"]:
                                save_arb_opportunity(btc_slug, eth_slug, "BTC_Up_ETH_Down", sum_1, btc_up_ask, eth_down_ask)
                                st.session_state["min_sum_1"] = sum_1
                                st.toast(f"New Low Sum 1: {sum_1:.4f} Recorded!", icon="📉")
                                
                        if sum_2 <= 0.8:
                            if sum_2 < st.session_state["min_sum_2"]:
                                save_arb_opportunity(btc_slug, eth_slug, "BTC_Down_ETH_Up", sum_2, btc_down_ask, eth_up_ask)
                                st.session_state["min_sum_2"] = sum_2
                                st.toast(f"New Low Sum 2: {sum_2:.4f} Recorded!", icon="📉")
                                
                    except ValueError:
                        pass # Data loading
                        
                    st.caption(f"Last Update: {time.strftime('%H:%M:%S')}")

                time.sleep(0.5)
                
                # Check rollover every 10s
                if int(time.time()) % 10 == 0:
                    break
            
            st.rerun()

        else:
            # Handle Closed / Rollover
            if auto_mode:
                # Save the COMPLETED round info before switching
                if btc_closed or eth_closed:
                    save_round_history(btc_slug, eth_slug)
                
                if btc_closed:
                    save_result(btc_slug, "Closed", "N/A")
                    st.session_state["current_btc_slug"] = get_next_slug(btc_slug)
                    # Reset min tracker for new round
                    st.session_state["min_sum_1"] = 1.0
                    st.session_state["min_sum_2"] = 1.0
                
                if eth_closed:
                    save_result(eth_slug, "Closed", "N/A")
                    st.session_state["current_eth_slug"] = get_next_slug(eth_slug)
                    
                st.rerun()

# --- PAGE: Arb History ---
elif page == "Arb History":
    st.title("Arbitrage Opportunities History 📉")
    if os.path.exists(ARB_OPPORTUNITY_FILE):
        try:
            df_arb = pd.read_csv(ARB_OPPORTUNITY_FILE)
            if not df_arb.empty and "timestamp" in df_arb.columns:
                df_arb["time_str"] = pd.to_datetime(df_arb["timestamp"], unit="s").dt.strftime('%Y-%m-%d %H:%M:%S')
                df_arb = df_arb.sort_values(by="timestamp", ascending=False)
                st.dataframe(df_arb, use_container_width=True)
            else:
                st.write("No valid data found.")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    else:
        st.info("No arbitrage history recorded yet.")

# --- PAGE: Market Results History ---
elif page == "Market Results History":
    st.title("Market Results History 📊")
    st.caption("Fetching last 100 historical rounds from chain... ✅ = Arbitrage Valid (Same Result)")
    
    # Generate list of past 100 rounds (15m intervals)
    # Start from current 15m block (or previous one if current is not done)
    # Actually, we want COMPLETED rounds. So start from (now // 900) * 900 - 900.
    
    now = int(time.time())
    current_block_start = (now // 900) * 900
    
    # Generate last 100 timestamps (descending)
    timestamps = []
    for i in range(1, 101):
        timestamps.append(current_block_start - (i * 900))
        
    # Helper to construct slugs
    # Pattern: btc-updown-15m-{timestamp}
    # Note: timestamp in slug is START time.
    
    @st.cache_data(ttl=3600) # Cache for 1 hour since history doesn't change
    def fetch_historical_results(ts_list):
        from concurrent.futures import ThreadPoolExecutor
        
        results = []
        
        def process_round(ts):
            btc_slug = f"btc-updown-15m-{ts}"
            eth_slug = f"eth-updown-15m-{ts}"
            
            # Fetch winners
            # Reuse get_market_winner logic but make it standalone or passed in?
            # We need to define it here or outside.
            
            def get_winner(slug):
                try:
                    url = f"{GAMMA_API_URL}/events"
                    params = {"slug": slug}
                    resp = requests.get(url, params=params, verify=False, timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data and isinstance(data, list) and len(data) > 0:
                            markets = data[0].get("markets", [])
                            if markets:
                                m = markets[0]
                                
                                # Method 1: Check outcomePrices (Reliable for resolved markets)
                                # e.g. outcomePrices: ["0", "1"] means outcomes[1] won
                                prices = m.get("outcomePrices")
                                outcomes = parse_json_field(m.get("outcomes"))
                                
                                if prices and outcomes and len(prices) == len(outcomes):
                                    # Find index where price is "1"
                                    for i, p in enumerate(prices):
                                        try:
                                            if float(p) >= 0.99: # Allow slight float diff, though usually "1"
                                                return outcomes[i]
                                        except:
                                            pass
                                
                                # Method 2: Check market details (tokens winner) - Fallback
                                mid = m.get("id")
                                if mid:
                                    m_url = f"{GAMMA_API_URL}/markets/{mid}"
                                    m_resp = requests.get(m_url, verify=False, timeout=5)
                                    if m_resp.status_code == 200:
                                        m_data = m_resp.json()
                                        if "tokens" in m_data:
                                            for t in m_data["tokens"]:
                                                if t.get("winner") == True:
                                                    return t.get("outcome", "Unknown")
                                
                                # Check if it's just not resolved yet
                                if m.get("closed"):
                                    return "Pending (Closed)"
                                else:
                                    return "Active"
                                    
                        else:
                            return "Not Found"
                except:
                    pass
                return "Error"

            btc_res = get_winner(btc_slug)
            eth_res = get_winner(eth_slug)
            
            is_arb = "❌"
            if btc_res not in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"] and \
               btc_res == eth_res:
                is_arb = "✅"
            elif btc_res in ["Pending", "Pending (Closed)", "Active"] or eth_res in ["Pending", "Pending (Closed)", "Active"]:
                is_arb = "⏳"
            elif btc_res == "Error" or eth_res == "Error":
                is_arb = "⚠️"
                
            return {
                "Time": datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),
                "BTC Result": btc_res,
                "ETH Result": eth_res,
                "Arb Valid?": is_arb,
                "BTC Slug": btc_slug,
                "ETH Slug": eth_slug
            }

        # Fetch in parallel
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(process_round, ts_list))
            
        return results

    # Fetch
    with st.spinner("Loading historical data..."):
        data = fetch_historical_results(timestamps)
        
    if data:
        df_res = pd.DataFrame(data)
        st.dataframe(df_res, use_container_width=True)
        
        # Calculate stats
        valid_count = df_res[df_res["Arb Valid?"] == "✅"].shape[0]
        total = len(df_res)
        st.metric("Arbitrage Success Rate (Last 100)", f"{valid_count}/{total}", f"{(valid_count/total)*100:.1f}%")
    else:
        st.error("Failed to fetch data.")
