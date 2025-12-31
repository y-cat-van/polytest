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

SETTINGS_FILE = "monitor_settings.json"

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {}

def save_settings():
    settings = {
        "arb_sum_threshold": st.session_state.get("arb_sum_threshold", 0.8),
        "arb_drop_threshold": st.session_state.get("arb_drop_threshold", 0.03),
        "arb_time_threshold": st.session_state.get("arb_time_threshold", 5)
    }
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f)
    # Force reload to update widgets if needed? Not strictly necessary if session state is sync'd


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

@st.cache_data(ttl=15)
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

@st.cache_data(ttl=15) # Keep short TTL or remove caching if manual update
def get_market_winner(slug):
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
                    
                    # Need to handle string json parsing if outcomes is string
                    if isinstance(outcomes, str):
                        try: outcomes = json.loads(outcomes)
                        except: pass
                        
                    if isinstance(prices, str):
                        try: prices = json.loads(prices)
                        except: pass
                    
                    if prices and outcomes and len(prices) == len(outcomes):
                        # Find index where price is "1"
                        for i, p in enumerate(prices):
                            try:
                                # Debugging showed prices are strings like "1", "0"
                                if float(p) >= 0.99: 
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
            with open(ROUND_HISTORY_FILE, "r") as f:
                lines = f.readlines()
                # Check last 50 lines for duplicates
                for line in reversed(lines[-50:]):
                    if btc_slug in line and eth_slug in line:
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
        self.last_update = 0.0

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
            self.last_update = time.time()

    def _update_price_direct(self, token_id, bid, ask):
        with self.lock:
            current = self.prices.get(token_id, {"bid": "N/A", "ask": "N/A"})
            new_bid = bid if bid is not None else current["bid"]
            new_ask = ask if ask is not None else current["ask"]
            self.prices[token_id] = {"bid": new_bid, "ask": new_ask}
            self.last_update = time.time()

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

# Ensure ws_manager is initialized and running
ws_manager = get_ws_manager()
if not ws_manager.is_running:
    ws_manager.start()

# --- Navigation ---
page = st.sidebar.radio("Navigation", ["Monitor", "Arb History", "Market Results History"])

# Load settings on startup (moved after page config)
loaded_settings = load_settings()
# Always initialize session_state with loaded settings or defaults if not present
if "arb_sum_threshold" not in st.session_state:
    st.session_state["arb_sum_threshold"] = loaded_settings.get("arb_sum_threshold", 0.8)
if "arb_drop_threshold" not in st.session_state:
    st.session_state["arb_drop_threshold"] = loaded_settings.get("arb_drop_threshold", 0.03)
if "arb_time_threshold" not in st.session_state:
    st.session_state["arb_time_threshold"] = loaded_settings.get("arb_time_threshold", 5)

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
        
        with st.expander("Arbitrage Recording Settings"):
            # These sliders are now also present in "Arb History" page for user convenience.
            # But Streamlit widgets with same key must be unique.
            # So if we are on Monitor page, we show them. If on Arb History, we show them there.
            # Or we can just remove them from here if the user prefers them in Arb History.
            # User said: "Move this setting to Arb History page".
            # So we REMOVE them from here (Monitor Page Sidebar).
            pass # Removed per user request
            st.info("Settings moved to 'Arb History' page.")

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
        
        # 1. Setup Placeholders for Dynamic Content (Static Layout)
        c1, c2 = st.columns(2)
        
        # BTC Column
        with c1:
            st.subheader("BTC Market")
            st.caption(f"Slug: {btc_slug}")
            if btc_markets: st.info(btc_markets[0].get("question"))
            btc_price_placeholder = st.empty()
            
        # ETH Column
        with c2:
            st.subheader("ETH Market")
            st.caption(f"Slug: {eth_slug}")
            if eth_markets: st.info(eth_markets[0].get("question"))
            eth_price_placeholder = st.empty()
            
        # Countdown & Calc Placeholders
        timer_placeholder = st.empty()
        st.divider()
        calc_placeholder = st.empty()
        footer_placeholder = st.empty()

        last_render_ts = 0.0
        last_ui_update = 0.0
        
        while True:
            # Check for updates (Low Latency Polling)
            current_update_ts = ws_manager.last_update
            now = time.time()
            
            # Update UI if new data arrived OR if 0.5 second passed (to update timer)
            if (current_update_ts > last_render_ts) or (now - last_ui_update > 0.5):
                last_render_ts = current_update_ts
                last_ui_update = now
                
                # Fetch Prices (Fast dict lookup)
                btc_prices = {t["outcome"]: ws_manager.get_price(t["token_id"]) for t in btc_tokens}
                eth_prices = {t["outcome"]: ws_manager.get_price(t["token_id"]) for t in eth_tokens}
                
                # --- Render BTC Prices ---
                with btc_price_placeholder.container():
                    for t in btc_tokens:
                        p = btc_prices[t["outcome"]]
                        st.metric(f"BTC {t['outcome']}", f"Ask: {p['ask']}", f"Bid: {p['bid']}")

                # --- Render ETH Prices ---
                with eth_price_placeholder.container():
                    for t in eth_tokens:
                        p = eth_prices[t["outcome"]]
                        st.metric(f"ETH {t['outcome']}", f"Ask: {p['ask']}", f"Bid: {p['bid']}")
                
                # --- Render Timer ---
                with timer_placeholder.container():
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
                    except:
                        st.write("Time Remaining: N/A")

                # --- Render Calcs ---
                with calc_placeholder.container():
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
                        
                        st.write(f"**BTC Up + ETH Down (Ask Sum): {sum_1:.4f}**")
                        st.write(f"**BTC Down + ETH Up (Ask Sum): {sum_2:.4f}**")
                        
                        # Record Logic
                        threshold_sum = st.session_state.get("arb_sum_threshold", 0.8)
                        threshold_drop = st.session_state.get("arb_drop_threshold", 0.03)
                        threshold_time_mins = st.session_state.get("arb_time_threshold", 5)

                        should_record_time = True
                        try:
                            end_ts = 0
                            if btc_markets and len(btc_markets) > 0:
                                end_date_iso = btc_markets[0].get("endDate")
                                if end_date_iso:
                                    end_dt = datetime.datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
                                    end_ts = end_dt.timestamp()
                            
                            if end_ts == 0:
                                parts = btc_slug.split("-")
                                end_ts = int(parts[-1])
                            
                            now_ts = time.time()
                            seconds_left = end_ts - now_ts
                            
                            if seconds_left < (threshold_time_mins * 60):
                                should_record_time = False
                        except:
                            pass 

                        if should_record_time:
                            if sum_1 <= threshold_sum:
                                if st.session_state["min_sum_1"] == 1.0 or sum_1 < (st.session_state["min_sum_1"] - threshold_drop):
                                    save_arb_opportunity(btc_slug, eth_slug, "BTC_Up_ETH_Down", sum_1, btc_up_ask, eth_down_ask)
                                    st.session_state["min_sum_1"] = sum_1
                                    st.toast(f"New Low Sum 1: {sum_1:.4f} Recorded!", icon="📉")
                                    
                            if sum_2 <= threshold_sum:
                                if st.session_state["min_sum_2"] == 1.0 or sum_2 < (st.session_state["min_sum_2"] - threshold_drop):
                                    save_arb_opportunity(btc_slug, eth_slug, "BTC_Down_ETH_Up", sum_2, btc_down_ask, eth_up_ask)
                                    st.session_state["min_sum_2"] = sum_2
                                    st.toast(f"New Low Sum 2: {sum_2:.4f} Recorded!", icon="📉")
                                
                    except ValueError:
                        pass
                
                # --- Render Footer ---
                with footer_placeholder.container():
                    st.caption(f"Last Update: {time.strftime('%H:%M:%S')}")

            time.sleep(0.01) # Ultra-fast polling
            
            # Check rollover logic
            try:
                # 1. Immediate Rollover Check
                parts = btc_slug.split("-")
                end_ts = int(parts[-1])
                if auto_mode and time.time() >= end_ts:
                    break
            except:
                pass

            # 2. Periodic Refresh removed
            # if int(time.time()) % 30 == 0: break
        
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
    
    with st.expander("Arbitrage Recording Settings"):
        st.slider(
            "Recording Threshold (Sum)", 
            0.01, 1.0, 
            value=st.session_state["arb_sum_threshold"], 
            key="arb_sum_threshold_slider", 
            on_change=lambda: st.session_state.update({"arb_sum_threshold": st.session_state.arb_sum_threshold_slider}) or save_settings()
        )
        st.slider(
            "New Low Drop Threshold", 
            0.01, 0.2, 
            value=st.session_state["arb_drop_threshold"], 
            key="arb_drop_threshold_slider", 
            on_change=lambda: st.session_state.update({"arb_drop_threshold": st.session_state.arb_drop_threshold_slider}) or save_settings()
        )
        st.number_input(
            "Stop Recording Minutes Before Close", 
            min_value=0, max_value=14, 
            value=st.session_state["arb_time_threshold"], 
            key="arb_time_threshold_input", 
            on_change=lambda: st.session_state.update({"arb_time_threshold": st.session_state.arb_time_threshold_input}) or save_settings()
        )

    if os.path.exists(ARB_OPPORTUNITY_FILE):
        try:
            df_arb = pd.read_csv(ARB_OPPORTUNITY_FILE)
            if not df_arb.empty and "timestamp" in df_arb.columns:
                # Deduplicate
                df_arb = df_arb.drop_duplicates(subset=["timestamp", "type", "sum"])
                
                # Format time
                df_arb["Time"] = pd.to_datetime(df_arb["timestamp"], unit="s").dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Sort
                df_arb = df_arb.sort_values(by="Time", ascending=False)

                # Reorder columns
                cols = ["Time", "type", "sum", "btc_ask", "eth_ask", "btc_slug", "eth_slug"]
                df_arb = df_arb[cols]
                
                # Define a function to style rows based on Slug
                def highlight_slug_groups(row):
                    # Use the BTC slug as a unique identifier for the "round"
                    # We can hash the slug string to get a color hue?
                    # Or simpler: alternating colors based on group?
                    # Streamlit st.dataframe styling is limited for whole-row coloring based on value easily without complex logic.
                    # But we can try to color specific cells or use 'background-color' property.
                    
                    # Simple approach: Hash the btc_slug to a hex color (light pastel)
                    slug = row["btc_slug"]
                    hash_val = hash(slug)
                    # Generate a pastel color
                    r = (hash_val & 0xFF0000) >> 16
                    g = (hash_val & 0x00FF00) >> 8
                    b = (hash_val & 0x0000FF)
                    
                    # Ensure light/pastel for readability
                    r = (r + 255) // 2
                    g = (g + 255) // 2
                    b = (b + 255) // 2
                    
                    color = f"#{r:02x}{g:02x}{b:02x}"
                    return [f'background-color: {color}' for _ in row]

                st.dataframe(df_arb.style.apply(highlight_slug_groups, axis=1), use_container_width=True)
            else:
                st.write("No valid data found.")
        except Exception as e:
            st.error(f"Error loading file: {e}")
    else:
        st.info("No arbitrage history recorded yet.")

# Helper to fetch rounds data (moved out for reusability)
def fetch_rounds_data(ts_list):
    from concurrent.futures import ThreadPoolExecutor
    
    results = []
    
    def process_round(ts):
        btc_slug = f"btc-updown-15m-{ts}"
        eth_slug = f"eth-updown-15m-{ts}"
        
        btc_res = get_market_winner(btc_slug)
        eth_res = get_market_winner(eth_slug)
        
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
            "ETH Slug": eth_slug,
            "timestamp": ts # Keep for joining
        }

    # Pre-load Arb History for lookup
    df_arb_all = pd.DataFrame()
    if os.path.exists(ARB_OPPORTUNITY_FILE):
        try:
            df_arb_all = pd.read_csv(ARB_OPPORTUNITY_FILE)
        except: pass

    # Fetch in parallel
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(process_round, ts_list))
        
    # Post-process results to add Arb Info
    final_results = []
    
    # Get settings from session state (or defaults) to apply filtering
    # Note: accessed from session_state in main thread context usually works, 
    # but here we are in a function called by main thread.
    threshold_sum = st.session_state.get("arb_sum_threshold", 0.8)
    threshold_time_mins = st.session_state.get("arb_time_threshold", 5)
    
    for r in results:
        ts = r["timestamp"]
        btc_slug = r["BTC Slug"]
        
        best_sum = None
        best_type = ""
        best_time_str = ""
        
        if not df_arb_all.empty and "btc_slug" in df_arb_all.columns:
            # Filter for this round
            round_arbs = df_arb_all[df_arb_all["btc_slug"] == btc_slug]
            
            if not round_arbs.empty:
                # Apply filters: Sum <= threshold
                round_arbs = round_arbs[round_arbs["sum"] <= threshold_sum]
                
                # Apply time filter: End time - record time > threshold
                # End time is ts + 900
                end_time = ts + 900
                # round_arbs["timestamp"] is record time
                # We want: (end_time - record_time) >= (threshold_time_mins * 60)
                # => record_time <= end_time - (threshold_time_mins * 60)
                cutoff_time = end_time - (threshold_time_mins * 60)
                round_arbs = round_arbs[round_arbs["timestamp"] <= cutoff_time]
                
                if not round_arbs.empty:
                    # Find min sum
                    min_row = round_arbs.loc[round_arbs["sum"].idxmin()]
                    best_sum = min_row["sum"]
                    best_type = min_row["type"]
                    best_time_str = datetime.datetime.fromtimestamp(min_row["timestamp"]).strftime('%H:%M:%S')

        r["Best Sum"] = f"{best_sum:.4f}" if best_sum is not None else "-"
        r["Arb Type"] = best_type if best_type else "-"
        r["Arb Time"] = best_time_str if best_time_str else "-"
        
        # Keep timestamp for merging logic
        final_results.append(r)

    return final_results


# --- PAGE: Market Results History ---
if page == "Market Results History":
    st.title("Market Results History 📊")
    st.caption("✅ = Arbitrage Valid (Same Result)")
    
    # Initialize history in session state
    if "market_history_data" not in st.session_state:
        # Load local history file if exists to avoid refetching everything
        MARKET_HISTORY_CACHE = "market_history_cache.json"
        loaded_from_cache = False
        
        if os.path.exists(MARKET_HISTORY_CACHE):
            try:
                with open(MARKET_HISTORY_CACHE, "r") as f:
                    cached_data = json.load(f)
                    if cached_data:
                        st.session_state["market_history_data"] = cached_data
                        loaded_from_cache = True
            except: pass
            
        if not loaded_from_cache:
            # Initial Fetch: Last 100
            now = int(time.time())
            current_block_start = (now // 900) * 900
            timestamps = [current_block_start - (i * 900) for i in range(100)]
            
            with st.spinner("Initializing historical data (last 100 rounds)..."):
                data = fetch_rounds_data(timestamps)
                st.session_state["market_history_data"] = data

    # Refresh Button
    if st.button("🔄 Refresh Data"):
        current_history = st.session_state.get("market_history_data", [])
        
        # 1. Identify max timestamp currently loaded
        max_ts = 0
        if current_history:
            max_ts = max(item["timestamp"] for item in current_history)
        else:
            max_ts = (int(time.time()) // 900) * 900 - 900
            
        now = int(time.time())
        current_block_start = (now // 900) * 900
        
        # 2. Find new timestamps (from max_ts + 900 up to current)
        timestamps_to_update = []
        t = max_ts + 900
        while t <= current_block_start:
            timestamps_to_update.append(t)
            t += 900
            
        # 3. Find pending rounds that need status refresh
        pending_ts = [
            item["timestamp"] for item in current_history 
            if item["BTC Result"] in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"] 
            or item["ETH Result"] in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]
        ]
        
        # Combine unique timestamps
        all_ts_to_fetch = list(set(timestamps_to_update + pending_ts))
        
        if all_ts_to_fetch:
            with st.spinner(f"Updating {len(all_ts_to_fetch)} rounds..."):
                updated_items = fetch_rounds_data(all_ts_to_fetch)
                
                # Merge into session state
                history_map = {item["timestamp"]: item for item in current_history}
                for item in updated_items:
                    history_map[item["timestamp"]] = item
                    
                new_history_list = list(history_map.values())
                new_history_list.sort(key=lambda x: x["timestamp"], reverse=True)
                
                st.session_state["market_history_data"] = new_history_list
                
                # Save to local cache
                MARKET_HISTORY_CACHE = "market_history_cache.json"
                try:
                    with open(MARKET_HISTORY_CACHE, "w") as f:
                        json.dump(new_history_list, f)
                except: pass
        
        st.rerun()

    # Display current data
    current_history = st.session_state.get("market_history_data", [])
    df_res = pd.DataFrame(current_history)
    
    # Drop timestamp col for display but keep in state
    if not df_res.empty:
        df_display = df_res.drop(columns=["timestamp"]).copy()
        df_display = df_display.sort_values(by="Time", ascending=False)
        st.dataframe(df_display, use_container_width=True)
        
        # Calculate stats
        valid_count = df_res[df_res["Arb Valid?"] == "✅"].shape[0]
        total = len(df_res)
        st.metric("Arbitrage Success Rate (All Loaded)", f"{valid_count}/{total}", f"{(valid_count/total)*100:.1f}%")
    else:
        st.info("No data yet.")
