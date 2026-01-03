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
        "arb_time_threshold": st.session_state.get("arb_time_threshold", 5),
        "same_sum_threshold": st.session_state.get("same_sum_threshold", 0.8),
        "same_drop_threshold": st.session_state.get("same_drop_threshold", 0.03),
        "same_time_threshold": st.session_state.get("same_time_threshold", 5)
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
MULTI_ASSET_HISTORY_FILE = "multi_asset_history.csv"

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

def save_arb_opportunity(slug_a, slug_b, type_name, sum_val, ask_a, ask_b, pair_name="BTC-ETH"):
    """Save arbitrage opportunity."""
    timestamp = int(time.time())
    file_exists = os.path.isfile(ARB_OPPORTUNITY_FILE)
    with open(ARB_OPPORTUNITY_FILE, "a") as f:
        if not file_exists:
            # Added pair column at the end to maintain some backward compatibility for reading if robust
            f.write("timestamp,type,sum,ask_a,ask_b,slug_a,slug_b,pair\n")
        f.write(f"{timestamp},{type_name},{sum_val},{ask_a},{ask_b},{slug_a},{slug_b},{pair_name}\n")

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
            # Silence expected heartbeat/non-json errors or print truncated
            if "Expecting value" not in str(e):
                print(f"Msg Parse Error: {e} | Msg: {message[:100]}")

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
        # Try both keys just in case, though asset_ids is standard
        msg = {"asset_ids": token_ids, "type": "market"}
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
page = st.sidebar.radio("Navigation", ["Monitor", "Arb History", "同向套利", "异向套利", "多币种同向分析"])

# Load settings on startup (moved after page config)
loaded_settings = load_settings()
# Always initialize session_state with loaded settings or defaults if not present
if "arb_sum_threshold" not in st.session_state:
    st.session_state["arb_sum_threshold"] = loaded_settings.get("arb_sum_threshold", 0.8)
if "arb_drop_threshold" not in st.session_state:
    st.session_state["arb_drop_threshold"] = loaded_settings.get("arb_drop_threshold", 0.03)
if "arb_time_threshold" not in st.session_state:
    st.session_state["arb_time_threshold"] = loaded_settings.get("arb_time_threshold", 5)
if "same_sum_threshold" not in st.session_state:
    st.session_state["same_sum_threshold"] = loaded_settings.get("same_sum_threshold", 0.8)
if "same_drop_threshold" not in st.session_state:
    st.session_state["same_drop_threshold"] = loaded_settings.get("same_drop_threshold", 0.03)
if "same_time_threshold" not in st.session_state:
    st.session_state["same_time_threshold"] = loaded_settings.get("same_time_threshold", 5)

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
        sol_seed_slug = st.text_input("SOL Seed Slug:", value="sol-updown-15m-1767105000")
        xrp_seed_slug = st.text_input("XRP Seed Slug:", value="xrp-updown-15m-1767105000")
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
            
        with st.expander("Debug Info"):
            st.write(f"WS Connected: {ws_manager.ws.sock.connected if ws_manager.ws and ws_manager.ws.sock else 'False'}")
            st.write(f"Subscribed Count: {len(ws_manager.subscribed_tokens)}")
            st.write(f"Last Update: {ws_manager.last_update}")
            if "current_btc_slug" in st.session_state:
                st.caption(f"BTC: {st.session_state['current_btc_slug']}")
            if "current_eth_slug" in st.session_state:
                st.caption(f"ETH: {st.session_state['current_eth_slug']}")

    # State Management
    if "current_btc_slug" not in st.session_state:
        st.session_state["current_btc_slug"] = get_latest_active_slug("btc-updown-15m")
        
    if "current_eth_slug" not in st.session_state:
        st.session_state["current_eth_slug"] = get_latest_active_slug("eth-updown-15m")

    if "current_sol_slug" not in st.session_state:
        st.session_state["current_sol_slug"] = get_latest_active_slug("sol-updown-15m")
        
    if "current_xrp_slug" not in st.session_state:
        st.session_state["current_xrp_slug"] = get_latest_active_slug("xrp-updown-15m")

    # Min value tracking for Arb (Generalize or keep specific?)
    # For now, we will use a dict to track min values for all pairs to be dynamic
    if "pair_min_values" not in st.session_state:
        st.session_state["pair_min_values"] = {} 
        # Structure: {"BTC-ETH": {"arb_1": 1.0, "arb_2": 1.0, "same_1": 2.0, "same_2": 2.0}}
    
    # Backward compatibility for existing BTC/ETH specific keys if used elsewhere?
    # We'll just leave them but transition to the dict for the loop.
    
    # Use st.empty() for the entire main area to prevent duplication
    main_placeholder = st.empty()

    # Fetch Data First
    btc_slug = st.session_state["current_btc_slug"]
    eth_slug = st.session_state["current_eth_slug"]
    sol_slug = st.session_state["current_sol_slug"]
    xrp_slug = st.session_state["current_xrp_slug"]
    
    btc_markets, _ = fetch_market_data(btc_slug)
    eth_markets, _ = fetch_market_data(eth_slug)
    sol_markets, _ = fetch_market_data(sol_slug)
    xrp_markets, _ = fetch_market_data(xrp_slug)
    
    def extract_tokens(markets):
        tokens = []
        is_closed = False
        if markets:
            m = markets[0]
            is_closed = m.get("closed", False)
            if not is_closed:
                outcomes = parse_json_field(m.get("outcomes", []))
                ids = parse_json_field(m.get("clobTokenIds", []))
                for i, tid in enumerate(ids):
                    o = outcomes[i] if i < len(outcomes) else str(i)
                    tokens.append({"outcome": o, "token_id": tid})
        return tokens, is_closed

    btc_tokens, btc_closed = extract_tokens(btc_markets)
    eth_tokens, eth_closed = extract_tokens(eth_markets)
    sol_tokens, sol_closed = extract_tokens(sol_markets)
    xrp_tokens, xrp_closed = extract_tokens(xrp_markets)

    # Subscribe & Warmup Combined
    all_token_ids = []
    for t_list in [btc_tokens, eth_tokens, sol_tokens, xrp_tokens]:
        all_token_ids.extend([t["token_id"] for t in t_list])
        
    if all_token_ids:
        ws_manager.subscribe(all_token_ids)
        # Warmup if needed
        warmup_key = f"warmed_{btc_slug}_{eth_slug}_{sol_slug}_{xrp_slug}"
        if not st.session_state.get(warmup_key):
            ws_manager.warmup_cache(all_token_ids)
            st.session_state[warmup_key] = True

    # --- Real-time Loop ---
    # Only render if at least BTC/ETH are active, but we should process all if possible.
    # For UI simplicity, we keep showing BTC/ETH as main, but process others in background.
    if not btc_closed and not eth_closed and btc_tokens and eth_tokens:
        
        # 1. Setup Placeholders for Dynamic Content (Static Layout)
        c1, c2 = st.columns(2)
        
        # BTC Column
        with c1:
            st.subheader("BTC Market")
            st.caption(f"Slug: {btc_slug}")
            if btc_markets: st.info(btc_markets[0].get("question"))
            
        # ETH Column
        with c2:
            st.subheader("ETH Market")
            st.caption(f"Slug: {eth_slug}")
            if eth_markets: st.info(eth_markets[0].get("question"))
            
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
                def get_prices(tokens):
                    return {t["outcome"]: ws_manager.get_price(t["token_id"]) for t in tokens}

                btc_prices = get_prices(btc_tokens)
                eth_prices = get_prices(eth_tokens)
                sol_prices = get_prices(sol_tokens)
                xrp_prices = get_prices(xrp_tokens)
                
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

                # --- Render Calcs & Background Record ---
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

                        # Helper to process a pair
                        def process_pair(name_a, name_b, prices_a, prices_b, slug_a, slug_b):
                            up_a = get_ask(prices_a, "Yes")
                            down_a = get_ask(prices_a, "No")
                            up_b = get_ask(prices_b, "Yes")
                            down_b = get_ask(prices_b, "No")
                            
                            s1 = up_a + down_b # Arb 1
                            s2 = down_a + up_b # Arb 2
                            ss1 = up_a + up_b  # Same 1
                            ss2 = down_a + down_b # Same 2
                            
                            return {
                                "arb_1": (s1, "BTC_Up_ETH_Down", up_a, down_b), # Type names legacy, maybe generalize?
                                "arb_2": (s2, "BTC_Down_ETH_Up", down_a, up_b),
                                "same_1": (ss1, "BTC_Up_ETH_Up", up_a, up_b),
                                "same_2": (ss2, "BTC_Down_ETH_Down", down_a, down_b)
                            }

                        # Define all pairs to monitor
                        pairs_config = [
                            ("BTC", "ETH", btc_prices, eth_prices, btc_slug, eth_slug),
                            ("BTC", "SOL", btc_prices, sol_prices, btc_slug, sol_slug),
                            ("BTC", "XRP", btc_prices, xrp_prices, btc_slug, xrp_slug),
                            ("ETH", "SOL", eth_prices, sol_prices, eth_slug, sol_slug),
                            ("ETH", "XRP", eth_prices, xrp_prices, eth_slug, xrp_slug),
                            ("SOL", "XRP", sol_prices, xrp_prices, sol_slug, xrp_slug),
                        ]
                        
                        # Only show BTC-ETH in UI for simplicity
                        res_btc_eth = process_pair("BTC", "ETH", btc_prices, eth_prices, btc_slug, eth_slug)
                        st.write(f"**BTC Up + ETH Down (Ask Sum): {res_btc_eth['arb_1'][0]:.4f}**")
                        st.write(f"**BTC Down + ETH Up (Ask Sum): {res_btc_eth['arb_2'][0]:.4f}**")
                        st.write(f"**BTC Up + ETH Up (Same Dir): {res_btc_eth['same_1'][0]:.4f}**")
                        st.write(f"**BTC Down + ETH Down (Same Dir): {res_btc_eth['same_2'][0]:.4f}**")
                        
                        # Record Logic for ALL pairs
                        threshold_sum = st.session_state.get("arb_sum_threshold", 0.8)
                        threshold_drop = st.session_state.get("arb_drop_threshold", 0.03)
                        threshold_time_mins = st.session_state.get("arb_time_threshold", 5)

                        same_threshold_sum = st.session_state.get("same_sum_threshold", 0.8)
                        same_threshold_drop = st.session_state.get("same_drop_threshold", 0.03)
                        same_threshold_time_mins = st.session_state.get("same_time_threshold", 5)

                        should_record_time = True
                        should_record_same_time = True
                        
                        # Time check (based on BTC market end time as reference for all 15m markets)
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
                            
                            if seconds_left < (same_threshold_time_mins * 60):
                                should_record_same_time = False

                        except:
                            pass 

                        # Loop through all pairs and record
                        for p_name_a, p_name_b, p_prices_a, p_prices_b, p_slug_a, p_slug_b in pairs_config:
                            pair_key = f"{p_name_a}-{p_name_b}"
                            
                            # Skip if markets not loaded
                            if not p_prices_a or not p_prices_b: continue
                            
                            res = process_pair(p_name_a, p_name_b, p_prices_a, p_prices_b, p_slug_a, p_slug_b)
                            
                            # Get min values for this pair
                            if pair_key not in st.session_state["pair_min_values"]:
                                st.session_state["pair_min_values"][pair_key] = {
                                    "arb_1": 1.0, "arb_2": 1.0, "same_1": 2.0, "same_2": 2.0
                                }
                            mins = st.session_state["pair_min_values"][pair_key]

                            # Check Arb
                            if should_record_time:
                                # Arb 1
                                val, type_base, ask_a, ask_b = res["arb_1"]
                                # Construct type name dynamically: A_Up_B_Down
                                type_real = f"{p_name_a}_Up_{p_name_b}_Down"
                                if val <= threshold_sum:
                                    if mins["arb_1"] == 1.0 or val < (mins["arb_1"] - threshold_drop):
                                        save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                        mins["arb_1"] = val
                                        if pair_key == "BTC-ETH": # Toast only main
                                            st.toast(f"New Low Arb 1 ({pair_key}): {val:.4f}", icon="📉")
                                
                                # Arb 2
                                val, type_base, ask_a, ask_b = res["arb_2"]
                                type_real = f"{p_name_a}_Down_{p_name_b}_Up"
                                if val <= threshold_sum:
                                    if mins["arb_2"] == 1.0 or val < (mins["arb_2"] - threshold_drop):
                                        save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                        mins["arb_2"] = val
                                        if pair_key == "BTC-ETH":
                                            st.toast(f"New Low Arb 2 ({pair_key}): {val:.4f}", icon="📉")

                            # Check Same
                            if should_record_same_time:
                                # Same 1
                                val, type_base, ask_a, ask_b = res["same_1"]
                                type_real = f"{p_name_a}_Up_{p_name_b}_Up"
                                if val <= same_threshold_sum:
                                    if mins["same_1"] == 2.0 or val < (mins["same_1"] - same_threshold_drop):
                                        save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                        mins["same_1"] = val
                                        if pair_key == "BTC-ETH":
                                            st.toast(f"New Low Same 1 ({pair_key}): {val:.4f}", icon="📉")
                                
                                # Same 2
                                val, type_base, ask_a, ask_b = res["same_2"]
                                type_real = f"{p_name_a}_Down_{p_name_b}_Down"
                                if val <= same_threshold_sum:
                                    if mins["same_2"] == 2.0 or val < (mins["same_2"] - same_threshold_drop):
                                        save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                        mins["same_2"] = val
                                        if pair_key == "BTC-ETH":
                                            st.toast(f"New Low Same 2 ({pair_key}): {val:.4f}", icon="📉")
                                
                    except ValueError:
                        pass
                
                # --- Render Footer ---
                with footer_placeholder.container():
                    st.caption(f"Last Update: {time.strftime('%H:%M:%S')}")

            time.sleep(0.5) # Reduced polling frequency to prevent overheating (was 0.01)
            
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
    
    with st.expander("Recording Settings", expanded=False):
        tab1, tab2 = st.tabs(["Opposite (Arb)", "Same Direction"])
        
        with tab1:
            st.caption("Settings for BTC Up + ETH Down / BTC Down + ETH Up")
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
            
        with tab2:
            st.caption("Settings for BTC Up + ETH Up / BTC Down + ETH Down")
            st.slider(
                "Recording Threshold (Sum)", 
                0.01, 2.0, 
                value=st.session_state["same_sum_threshold"], 
                key="same_sum_threshold_slider", 
                on_change=lambda: st.session_state.update({"same_sum_threshold": st.session_state.same_sum_threshold_slider}) or save_settings()
            )
            st.slider(
                "New Low Drop Threshold", 
                0.01, 0.2, 
                value=st.session_state["same_drop_threshold"], 
                key="same_drop_threshold_slider", 
                on_change=lambda: st.session_state.update({"same_drop_threshold": st.session_state.same_drop_threshold_slider}) or save_settings()
            )
            st.number_input(
                "Stop Recording Minutes Before Close", 
                min_value=0, max_value=14, 
                value=st.session_state["same_time_threshold"], 
                key="same_time_threshold_input", 
                on_change=lambda: st.session_state.update({"same_time_threshold": st.session_state.same_time_threshold_input}) or save_settings()
            )

    # Filter UI
    filter_option = st.radio("Filter Type:", ["All", "Opposite (Arb)", "Same Direction"], horizontal=True)

    if os.path.exists(ARB_OPPORTUNITY_FILE):
        try:
            df_arb = pd.read_csv(ARB_OPPORTUNITY_FILE)
            if not df_arb.empty and "timestamp" in df_arb.columns:
                # Deduplicate
                df_arb = df_arb.drop_duplicates(subset=["timestamp", "type", "sum"])
                
                # Filter Logic
                if filter_option == "Opposite (Arb)":
                    df_arb = df_arb[df_arb["type"].isin(["BTC_Up_ETH_Down", "BTC_Down_ETH_Up"])]
                elif filter_option == "Same Direction":
                    df_arb = df_arb[df_arb["type"].isin(["BTC_Up_ETH_Up", "BTC_Down_ETH_Down"])]
                
                # Format time
                df_arb["Time"] = pd.to_datetime(df_arb["timestamp"], unit="s").dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Sort
                df_arb = df_arb.sort_values(by="Time", ascending=False)

                # Reorder columns
                # Updated to match new schema
                cols = ["Time", "type", "sum", "ask_a", "ask_b", "slug_a", "slug_b", "pair"]
                # Filter cols that exist
                cols = [c for c in cols if c in df_arb.columns]
                df_arb = df_arb[cols]
                
                # Define a function to style rows based on Slug
                def highlight_slug_groups(row):
                    # Use the slug_a as a unique identifier for the "round"
                    # We can hash the slug string to get a color hue?
                    # Or simpler: alternating colors based on group?
                    # Streamlit st.dataframe styling is limited for whole-row coloring based on value easily without complex logic.
                    # But we can try to color specific cells or use 'background-color' property.
                    
                    # Simple approach: Hash the slug_a to a hex color (light pastel)
                    slug = row.get("slug_a", "")
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
        
        # Determine Relation
        relation = "Unknown"
        if btc_res in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"] or \
           eth_res in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]:
            relation = "Pending/Error"
        elif btc_res == eth_res:
            relation = "Same"
        else:
            relation = "Opposite"
        
        return {
            "Time": datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),
            "BTC Result": btc_res,
            "ETH Result": eth_res,
            "Relation": relation,
            "BTC Slug": btc_slug,
            "ETH Slug": eth_slug,
            "timestamp": ts # Keep for joining
        }

    # Pre-load Arb History for lookup
    df_arb_all = pd.DataFrame()
    if os.path.exists(ARB_OPPORTUNITY_FILE):
        try:
            df_arb_all = pd.read_csv(ARB_OPPORTUNITY_FILE)
            # Ensure column compatibility if reading from mixed sources (though we migrated)
            if "btc_slug" in df_arb_all.columns and "slug_a" not in df_arb_all.columns:
                df_arb_all.rename(columns={"btc_slug": "slug_a", "eth_slug": "slug_b"}, inplace=True)
        except: pass

    # Fetch in parallel
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(process_round, ts_list))
        
    # Post-process results to add Arb Info
    final_results = []
    
    # Get settings from session state (or defaults) to apply filtering
    threshold_sum = st.session_state.get("arb_sum_threshold", 0.8)
    threshold_time_mins = st.session_state.get("arb_time_threshold", 5)
    
    # Same settings
    same_threshold_sum = st.session_state.get("same_sum_threshold", 0.8)
    same_threshold_time_mins = st.session_state.get("same_time_threshold", 5)
    
    for r in results:
        ts = r["timestamp"]
        btc_slug = r["BTC Slug"]
        
        # Init vars
        best_arb_sum = None
        best_arb_type = ""
        best_arb_time = ""
        
        best_same_sum = None
        best_same_type = ""
        best_same_time = ""
        
        if not df_arb_all.empty and "slug_a" in df_arb_all.columns:
            # Filter for this round
            round_arbs = df_arb_all[df_arb_all["slug_a"] == btc_slug]
            
            if not round_arbs.empty:
                # 1. Process Opposite (Arb) Bets
                arb_bets = round_arbs[round_arbs["type"].isin(["BTC_Up_ETH_Down", "BTC_Down_ETH_Up"])]
                if not arb_bets.empty:
                    # Apply filters
                    arb_bets = arb_bets[arb_bets["sum"] <= threshold_sum]
                    # Time filter
                    end_time = ts + 900
                    cutoff_time = end_time - (threshold_time_mins * 60)
                    arb_bets = arb_bets[arb_bets["timestamp"] <= cutoff_time]
                    
                    if not arb_bets.empty:
                        min_row = arb_bets.loc[arb_bets["sum"].idxmin()]
                        best_arb_sum = min_row["sum"]
                        best_arb_type = min_row["type"]
                        best_arb_time = datetime.datetime.fromtimestamp(min_row["timestamp"]).strftime('%H:%M:%S')

                # 2. Process Same Direction Bets
                same_bets = round_arbs[round_arbs["type"].isin(["BTC_Up_ETH_Up", "BTC_Down_ETH_Down"])]
                if not same_bets.empty:
                    # Apply filters
                    same_bets = same_bets[same_bets["sum"] <= same_threshold_sum]
                    # Time filter
                    end_time = ts + 900
                    cutoff_time = end_time - (same_threshold_time_mins * 60)
                    same_bets = same_bets[same_bets["timestamp"] <= cutoff_time]
                    
                    if not same_bets.empty:
                        min_row = same_bets.loc[same_bets["sum"].idxmin()]
                        best_same_sum = min_row["sum"]
                        best_same_type = min_row["type"]
                        best_same_time = datetime.datetime.fromtimestamp(min_row["timestamp"]).strftime('%H:%M:%S')

        r["Best Arb Sum"] = f"{best_arb_sum:.4f}" if best_arb_sum is not None else "-"
        r["Arb Type"] = best_arb_type if best_arb_type else "-"
        r["Arb Time"] = best_arb_time if best_arb_time else "-"
        
        r["Best Same Sum"] = f"{best_same_sum:.4f}" if best_same_sum is not None else "-"
        r["Same Type"] = best_same_type if best_same_type else "-"
        r["Same Time"] = best_same_time if best_same_time else "-"
        
        # Keep timestamp for merging logic
        final_results.append(r)

    return final_results


# --- PAGE: History Pages (Shared Logic) ---
if page in ["同向套利", "异向套利"]:
    is_same_arb_page = (page == "同向套利") # Target: Same Result. Show: Opposite Bets (Arb)
    page_title = "同向套利 (Same Result, Arb Bets)" if is_same_arb_page else "异向套利 (Opposite Result, Same Bets)"
    target_relation = "Same" if is_same_arb_page else "Opposite"
    
    st.title(f"{page_title} 📊")
    st.caption(f"✅ = {target_relation} Result | ❌ = {'Opposite' if is_same_arb_page else 'Same'} Result")
    
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
    
    if current_history:
        # Process data for specific page view
        processed_data = []
        valid_count = 0
        resolved_count = 0
        
        for item in current_history:
            # Create a copy to modify for display
            row = item.copy()
            
            # Re-calculate relation on the fly to handle old cache or stale data
            btc_res = row.get("BTC Result", "Unknown")
            eth_res = row.get("ETH Result", "Unknown")
            relation = "Unknown"
            
            if btc_res in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"] or \
               eth_res in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]:
                relation = "Pending/Error"
            elif btc_res == eth_res:
                relation = "Same"
            else:
                relation = "Opposite"
            
            # Determine Status Icon
            status = "⚠️"
            if relation == "Pending/Error":
                status = "⏳"
            elif relation == target_relation:
                status = "✅"
                valid_count += 1
                resolved_count += 1
            else:
                status = "❌"
                resolved_count += 1
            
            row["Status"] = status
            row["Relation"] = relation # Update row for display if needed
            
            # Select correct columns based on page type
            if is_same_arb_page:
                # Page: Same Result. Show: Opposite Bets (Arb)
                row["Best Sum"] = row.get("Best Arb Sum", "-")
                row["Type"] = row.get("Arb Type", "-")
                row["Rec Time"] = row.get("Arb Time", "-")
            else:
                # Page: Opposite Result. Show: Same Bets (Same)
                row["Best Sum"] = row.get("Best Same Sum", "-")
                row["Type"] = row.get("Same Type", "-")
                row["Rec Time"] = row.get("Same Time", "-")

            processed_data.append(row)
            
        df_res = pd.DataFrame(processed_data)
        
        # Drop timestamp col for display but keep in state
        if not df_res.empty:
            cols_order = ["Time", "Status", "BTC Result", "ETH Result", "Relation", "Best Sum", "Type", "Rec Time", "BTC Slug", "ETH Slug"]
            # Filter cols that exist
            cols_to_show = [c for c in cols_order if c in df_res.columns]
            
            df_display = df_res[cols_to_show].copy()
            df_display = df_display.sort_values(by="Time", ascending=False)
            st.dataframe(df_display, use_container_width=True)
            
            # Calculate stats
            if resolved_count > 0:
                st.metric(f"{target_relation} Success Rate", f"{valid_count}/{resolved_count}", f"{(valid_count/resolved_count)*100:.1f}%")
            else:
                st.info("No resolved rounds yet.")
    else:
        st.info("No data yet.")

# --- PAGE: Multi-Asset Analysis ---
elif page == "多币种同向分析":
    st.title("多币种同向分析 (Multi-Asset Correlation) 📊")
    
    # --- Sidebar Config ---
    with st.sidebar:
        st.divider()
        st.header("Multi-Asset Settings")
        # Reuse BTC/ETH seeds from top but add SOL/XRP
        sol_seed_slug = st.text_input("SOL Seed Slug:", value="sol-updown-15m-1767105000")
        xrp_seed_slug = st.text_input("XRP Seed Slug:", value="xrp-updown-15m-1767105000")
        
        # Action Buttons
        if st.button("📥 加载/更新历史 (1000条)"):
            # 1. Load existing
            existing_data = {}
            if os.path.exists(MULTI_ASSET_HISTORY_FILE):
                try:
                    df_exist = pd.read_csv(MULTI_ASSET_HISTORY_FILE)
                    if not df_exist.empty:
                        for _, row in df_exist.iterrows():
                            existing_data[row["timestamp"]] = row.to_dict()
                except: pass
            
            # 2. Determine range (Last 1000 rounds)
            now = int(time.time())
            current_block_start = (now // 900) * 900
            target_timestamps = [current_block_start - (i * 900) for i in range(1000)]
            
            # 3. Identify missing or pending
            missing_ts = []
            
            # Statuses that need refresh
            pending_statuses = ["Active", "Pending", "Pending (Closed)", "Error", "Not Found", "Unknown"]
            
            for ts in target_timestamps:
                if ts not in existing_data:
                    missing_ts.append(ts)
                else:
                    # Check if any asset result is pending/active
                    row = existing_data[ts]
                    needs_refresh = False
                    for key in ["btc_res", "eth_res", "sol_res", "xrp_res"]:
                        if row.get(key) in pending_statuses:
                            needs_refresh = True
                            break
                    if needs_refresh:
                        missing_ts.append(ts)
            
            # Remove duplicates just in case
            missing_ts = list(set(missing_ts))
            
            # 4. Fetch Missing (Batch)
            if missing_ts:
                from concurrent.futures import ThreadPoolExecutor
                
                # Progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Parse prefixes from user input
                sol_prefix = "-".join(sol_seed_slug.split("-")[:-1])
                xrp_prefix = "-".join(xrp_seed_slug.split("-")[:-1])
                
                # Use a safe fallback that doesn't depend on global widget variables
                btc_prefix = "btc-updown-15m"
                eth_prefix = "eth-updown-15m"
                
                try:
                    # Attempt to parse from sol prefix (assuming user keeps consistent format)
                    # e.g., sol-updown-15m -> btc-updown-15m
                    parts = sol_prefix.split("-")
                    if len(parts) > 1:
                        suffix = "-".join(parts[1:])
                        btc_prefix = f"btc-{suffix}"
                        eth_prefix = f"eth-{suffix}"
                except:
                    pass

                def fetch_multi_round(ts):
                    # Construct slugs
                    btc_s = f"{btc_prefix}-{ts}"
                    eth_s = f"{eth_prefix}-{ts}"
                    sol_s = f"{sol_prefix}-{ts}"
                    xrp_s = f"{xrp_prefix}-{ts}"
                    
                    return {
                        "timestamp": ts,
                        "Time": datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),
                        "btc_res": get_market_winner(btc_s),
                        "eth_res": get_market_winner(eth_s),
                        "sol_res": get_market_winner(sol_s),
                        "xrp_res": get_market_winner(xrp_s)
                    }

                new_records = []
                batch_size = 20
                total_batches = (len(missing_ts) + batch_size - 1) // batch_size
                
                for i in range(0, len(missing_ts), batch_size):
                    batch = missing_ts[i:i+batch_size]
                    with ThreadPoolExecutor(max_workers=10) as executor: # Limit concurrency
                        results = list(executor.map(fetch_multi_round, batch))
                        new_records.extend(results)
                    
                    # Update progress
                    current_batch = (i // batch_size) + 1
                    progress = min(current_batch / total_batches, 1.0)
                    progress_bar.progress(progress)
                    status_text.text(f"Fetching batch {current_batch}/{total_batches}...")
                    time.sleep(0.5) # Rate limit protection
                
                # Merge and Save
                for r in new_records:
                    existing_data[r["timestamp"]] = r
                
                # Convert back to list and sort
                final_list = list(existing_data.values())
                final_list.sort(key=lambda x: x["timestamp"], reverse=True)
                
                # Save
                pd.DataFrame(final_list).to_csv(MULTI_ASSET_HISTORY_FILE, index=False)
                st.success(f"Updated {len(new_records)} new records!")
                st.rerun()
            else:
                st.info("Data is already up to date.")

    # --- Main Analysis UI ---
    
    # 1. Load Data
    df = pd.DataFrame()
    if os.path.exists(MULTI_ASSET_HISTORY_FILE):
        try:
            df = pd.read_csv(MULTI_ASSET_HISTORY_FILE)
        except: pass
        
    if not df.empty:
        # 2. Filters
        c1, c2 = st.columns([1, 3])
        with c1:
            pair = st.selectbox("选择交易对 (Select Pair)", [
                "BTC vs ETH", "BTC vs SOL", "BTC vs XRP",
                "ETH vs SOL", "ETH vs XRP",
                "SOL vs XRP"
            ])
            
        asset_a_name, asset_b_name = pair.split(" vs ")
        col_a = f"{asset_a_name.lower()}_res"
        col_b = f"{asset_b_name.lower()}_res"
        
        # 3. Process Data
        # Filter out invalid rows for statistics
        valid_df = df[
            (~df[col_a].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"])) &
            (~df[col_b].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]))
        ].copy()
        
        # Calculate Relation
        valid_df["is_same"] = valid_df[col_a] == valid_df[col_b]
        
        # Stats
        total_samples = len(valid_df)
        same_count = valid_df["is_same"].sum()
        corr_rate = (same_count / total_samples * 100) if total_samples > 0 else 0
        
        # Display Stats
        st.metric(
            label=f"同向占比 (Correlation Rate) - {pair}",
            value=f"{corr_rate:.1f}%",
            delta=f"样本数: {total_samples}"
        )

        # --- Best Price Integration ---
        # Load Arb Data
        best_prices = {} # Map: round_ts -> min_sum
        if os.path.exists(ARB_OPPORTUNITY_FILE):
            try:
                df_arb = pd.read_csv(ARB_OPPORTUNITY_FILE)
                if not df_arb.empty:
                    # Handle backward compatibility for 'pair' column and slug names
                    if "pair" not in df_arb.columns:
                        df_arb["pair"] = "BTC-ETH"
                        # Rename legacy columns to match new generic schema
                        rename_map = {}
                        if "btc_slug" in df_arb.columns: rename_map["btc_slug"] = "slug_a"
                        if "eth_slug" in df_arb.columns: rename_map["eth_slug"] = "slug_b"
                        if rename_map:
                            df_arb.rename(columns=rename_map, inplace=True)
                    
                    # Filter for current selected pair
                    target_pair = f"{asset_a_name}-{asset_b_name}"
                    df_arb_pair = df_arb[df_arb["pair"] == target_pair].copy()
                    
                    if not df_arb_pair.empty:
                        # Extract round timestamp from slug_a
                        # Assuming slug format ends with -{timestamp}
                        def extract_round_ts(slug):
                            try:
                                return int(slug.split("-")[-1])
                            except:
                                return 0
                        
                        df_arb_pair["round_ts"] = df_arb_pair["slug_a"].apply(extract_round_ts)
                        
                        # Filter for SAME direction types
                        # Dynamic type check: {AssetA}_Up_{AssetB}_Up or {AssetA}_Down_{AssetB}_Down
                        type_up_up = f"{asset_a_name}_Up_{asset_b_name}_Up"
                        type_down_down = f"{asset_a_name}_Down_{asset_b_name}_Down"
                        
                        df_same = df_arb_pair[df_arb_pair["type"].isin([type_up_up, type_down_down])]
                        
                        if not df_same.empty:
                            # Group by round_ts and take min sum
                            min_series = df_same.groupby("round_ts")["sum"].min()
                            best_prices = min_series.to_dict()
            except Exception as e:
                # st.error(f"Error loading best prices: {e}")
                pass

        # 4. Detailed Table
        st.subheader("详细历史数据")
        
        # Prepare display dataframe from ORIGINAL df to show pending ones too
        display_df = df.copy()
        display_df["Relation"] = display_df.apply(
            lambda row: "✅ 同向" if row[col_a] == row[col_b] else "❌ 异向", axis=1
        )
        
        # Handle pending/error visual
        def refine_relation(row):
            v_a = row[col_a]
            v_b = row[col_b]
            if v_a in ["Pending", "Active", "Pending (Closed)"] or v_b in ["Pending", "Active", "Pending (Closed)"]:
                return "⏳ Pending"
            if v_a == "Error" or v_b == "Error":
                return "⚠️ Error"
            return "✅ 同向" if v_a == v_b else "❌ 异向"
            
        display_df["Relation"] = display_df.apply(refine_relation, axis=1)
        
        # Add Best Price Column
        def get_best_price(row):
            ts = row["timestamp"]
            return f"{best_prices[ts]:.4f}" if ts in best_prices else "-"
            
        display_df["Same Dir Best Price"] = display_df.apply(get_best_price, axis=1)
        
        # Select columns
        cols = ["Time", col_a, col_b, "Relation", "Same Dir Best Price"]
        display_df = display_df[cols].sort_values(by="Time", ascending=False)
        
        # Rename for display
        display_df.columns = ["Time", f"{asset_a_name} Result", f"{asset_b_name} Result", "Relation", "Same Dir Best Price"]
        
        st.dataframe(display_df, use_container_width=True)
        
    else:
        st.info("No data loaded. Please click '📥 加载/更新历史' in the sidebar.")
