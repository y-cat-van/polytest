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
from concurrent.futures import ThreadPoolExecutor

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

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
HISTORY_FILE = "btc_15m_history.csv" 
ROUND_HISTORY_FILE = "round_history.csv"
ARB_OPPORTUNITY_FILE = "arb_opportunities.csv"
ARB_OPPORTUNITY_FILE_1H = "arb_opportunities_1h.csv"
MULTI_ASSET_HISTORY_FILE = "multi_asset_history.csv"

# --- Helper Functions ---
def get_current_slug_1h(asset="BTC"):
    name_map = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "XRP": "ripple"
    }
    
    from datetime import timezone, timedelta
    
    now_utc = datetime.datetime.now(timezone.utc)
    now_et = now_utc - timedelta(hours=5) 
    
    month_name = now_et.strftime("%B").lower()
    day = now_et.day
    hour_24 = now_et.hour
    
    if hour_24 == 0:
        hour_str = "12am"
    elif hour_24 < 12:
        hour_str = f"{hour_24}am"
    elif hour_24 == 12:
        hour_str = "12pm"
    else:
        hour_str = f"{hour_24-12}pm"
        
    asset_full = name_map.get(asset, asset.lower())
    return f"{asset_full}-up-or-down-{month_name}-{day}-{hour_str}-et"

def get_slug_from_url(url):
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

@st.cache_data(ttl=15)
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
                    prices = m.get("outcomePrices")
                    outcomes = parse_json_field(m.get("outcomes"))
                    
                    if isinstance(outcomes, str):
                        try: outcomes = json.loads(outcomes)
                        except: pass
                        
                    if isinstance(prices, str):
                        try: prices = json.loads(prices)
                        except: pass
                    
                    if prices and outcomes and len(prices) == len(outcomes):
                        for i, p in enumerate(prices):
                            try:
                                if float(p) >= 0.99: 
                                    return outcomes[i]
                            except:
                                pass
                    
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
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, "a") as f:
        if not file_exists:
            f.write("market_slug,result,end_time,timestamp\n")
        f.write(f"{market_slug},{result},{end_time},{int(time.time())}\n")

def save_round_history(btc_slug, eth_slug):
    timestamp = int(time.time())
    file_exists = os.path.isfile(ROUND_HISTORY_FILE)
    if file_exists:
        try:
            with open(ROUND_HISTORY_FILE, "r") as f:
                lines = f.readlines()
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
    timestamp = int(time.time())
    file_exists = os.path.isfile(ARB_OPPORTUNITY_FILE)
    with open(ARB_OPPORTUNITY_FILE, "a") as f:
        if not file_exists:
            f.write("timestamp,type,sum,ask_a,ask_b,slug_a,slug_b,pair\n")
        f.write(f"{timestamp},{type_name},{sum_val},{ask_a},{ask_b},{slug_a},{slug_b},{pair_name}\n")

# --- WebSocket Manager ---
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
            if "Expecting value" not in str(e):
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
                    self._subscribe(list(self.subscribed_tokens))

    def _subscribe(self, token_ids):
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
if not ws_manager.is_running:
    ws_manager.start()

# --- Navigation ---
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to:", ["Monitor", "Monitor 1H", "Arb History", "同向套利 (同向结果复盘)", "异向套利 (异向结果复盘)", "多币种AAA策略分析", "多币种AAA策略分析-1h"])

st.sidebar.divider()

st.sidebar.subheader("Global Settings")
auto_mode = st.sidebar.checkbox("Auto-Rollover Markets (15m)", value=True, help="Automatically switch to next 15m market when current one ends.")
auto_mode_1h = st.sidebar.checkbox("Auto-Rollover Markets (1H)", value=True, help="Automatically switch to next 1H market when current one ends.")

if st.sidebar.button("🗑️ Clear All Caches"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

loaded_settings = load_settings()
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

# --- Additional Helpers ---
def get_latest_active_slug(prefix):
    try:
        now = int(time.time())
        current_block_start = (now // 900) * 900
        return f"{prefix}-{current_block_start}"
    except:
        return f"{prefix}-0"

def get_next_slug(current_slug):
    try:
        parts = current_slug.split("-")
        last_ts = int(parts[-1])
        next_ts = last_ts + 900
        return "-".join(parts[:-1]) + f"-{next_ts}"
    except:
        return current_slug

def fetch_rounds_data(ts_list):
    results = []
    
    def process_round(ts):
        btc_slug = f"btc-updown-15m-{ts}"
        eth_slug = f"eth-updown-15m-{ts}"
        
        btc_res = get_market_winner(btc_slug)
        eth_res = get_market_winner(eth_slug)
        
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
            "timestamp": ts 
        }

    df_arb_all = pd.DataFrame()
    if os.path.exists(ARB_OPPORTUNITY_FILE):
        try:
            df_arb_all = pd.read_csv(ARB_OPPORTUNITY_FILE)
            if "btc_slug" in df_arb_all.columns and "slug_a" not in df_arb_all.columns:
                df_arb_all.rename(columns={"btc_slug": "slug_a", "eth_slug": "slug_b"}, inplace=True)
        except: pass

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(process_round, ts_list))
        
    final_results = []
    threshold_sum = st.session_state.get("arb_sum_threshold", 0.8)
    threshold_time_mins = st.session_state.get("arb_time_threshold", 5)
    same_threshold_sum = st.session_state.get("same_sum_threshold", 0.8)
    same_threshold_time_mins = st.session_state.get("same_time_threshold", 5)
    
    for r in results:
        ts = r["timestamp"]
        btc_slug = r["BTC Slug"]
        
        best_arb_sum = None
        best_arb_type = ""
        best_arb_time = ""
        best_same_sum = None
        best_same_type = ""
        best_same_time = ""
        
        if not df_arb_all.empty and "slug_a" in df_arb_all.columns:
            round_arbs = df_arb_all[df_arb_all["slug_a"] == btc_slug]
            if not round_arbs.empty:
                arb_bets = round_arbs[round_arbs["type"].isin(["BTC_Up_ETH_Down", "BTC_Down_ETH_Up"])]
                if not arb_bets.empty:
                    arb_bets = arb_bets[arb_bets["sum"] <= threshold_sum]
                    end_time = ts + 900
                    cutoff_time = end_time - (threshold_time_mins * 60)
                    arb_bets = arb_bets[arb_bets["timestamp"] <= cutoff_time]
                    if not arb_bets.empty:
                        min_row = arb_bets.loc[arb_bets["sum"].idxmin()]
                        best_arb_sum = min_row["sum"]
                        best_arb_type = min_row["type"]
                        best_arb_time = datetime.datetime.fromtimestamp(min_row["timestamp"]).strftime('%H:%M:%S')

                same_bets = round_arbs[round_arbs["type"].isin(["BTC_Up_ETH_Up", "BTC_Down_ETH_Down"])]
                if not same_bets.empty:
                    same_bets = same_bets[same_bets["sum"] <= same_threshold_sum]
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
        final_results.append(r)

    return final_results

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
            pass 
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

    if "current_btc_slug" not in st.session_state:
        st.session_state["current_btc_slug"] = get_latest_active_slug("btc-updown-15m")
    if "current_eth_slug" not in st.session_state:
        st.session_state["current_eth_slug"] = get_latest_active_slug("eth-updown-15m")
    if "current_sol_slug" not in st.session_state:
        st.session_state["current_sol_slug"] = get_latest_active_slug("sol-updown-15m")
    if "current_xrp_slug" not in st.session_state:
        st.session_state["current_xrp_slug"] = get_latest_active_slug("xrp-updown-15m")

    if "pair_min_values" not in st.session_state:
        st.session_state["pair_min_values"] = {} 
    
    main_placeholder = st.empty()

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

    all_token_ids = []
    for t_list in [btc_tokens, eth_tokens, sol_tokens, xrp_tokens]:
        all_token_ids.extend([t["token_id"] for t in t_list])
        
    if all_token_ids:
        ws_manager.subscribe(all_token_ids)
        warmup_key = f"warmed_{btc_slug}_{eth_slug}_{sol_slug}_{xrp_slug}"
        if not st.session_state.get(warmup_key):
            ws_manager.warmup_cache(all_token_ids)
            st.session_state[warmup_key] = True

    if not btc_closed and not eth_closed and btc_tokens and eth_tokens:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("BTC Market")
            st.caption(f"Slug: {btc_slug}")
            if btc_markets: st.info(btc_markets[0].get("question"))
        with c2:
            st.subheader("ETH Market")
            st.caption(f"Slug: {eth_slug}")
            if eth_markets: st.info(eth_markets[0].get("question"))
            
        st.divider()
        st.success("✅ Monitoring Active! Data is being processed in the background. Check 'Arb History' for results.")
        
        if st.button("🔄 Refresh UI"):
            st.rerun()
        
        status_placeholder = st.empty()
        last_process_ts = 0.0
        
        while True:
            current_update_ts = ws_manager.last_update
            
            if current_update_ts > last_process_ts:
                last_process_ts = current_update_ts
                
                def get_prices(tokens):
                    return {t["outcome"]: ws_manager.get_price(t["token_id"]) for t in tokens}

                btc_prices = get_prices(btc_tokens)
                eth_prices = get_prices(eth_tokens)
                sol_prices = get_prices(sol_tokens)
                xrp_prices = get_prices(xrp_tokens)
                
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

                    def process_pair(name_a, name_b, prices_a, prices_b, slug_a, slug_b):
                        up_a = get_ask(prices_a, "Yes")
                        down_a = get_ask(prices_a, "No")
                        up_b = get_ask(prices_b, "Yes")
                        down_b = get_ask(prices_b, "No")
                        
                        s1 = up_a + down_b 
                        s2 = down_a + up_b 
                        ss1 = up_a + up_b  
                        ss2 = down_a + down_b 
                        
                        return {
                            "arb_1": (s1, "BTC_Up_ETH_Down", up_a, down_b), 
                            "arb_2": (s2, "BTC_Down_ETH_Up", down_a, up_b),
                            "same_1": (ss1, "BTC_Up_ETH_Up", up_a, up_b),
                            "same_2": (ss2, "BTC_Down_ETH_Down", down_a, down_b)
                        }

                    pairs_config = [
                        ("BTC", "ETH", btc_prices, eth_prices, btc_slug, eth_slug),
                        ("BTC", "SOL", btc_prices, sol_prices, btc_slug, sol_slug),
                        ("BTC", "XRP", btc_prices, xrp_prices, btc_slug, xrp_slug),
                        ("ETH", "SOL", eth_prices, sol_prices, eth_slug, sol_slug),
                        ("ETH", "XRP", eth_prices, xrp_prices, eth_slug, xrp_slug),
                        ("SOL", "XRP", sol_prices, xrp_prices, sol_slug, xrp_slug),
                    ]
                    
                    threshold_sum = st.session_state.get("arb_sum_threshold", 0.8)
                    threshold_drop = st.session_state.get("arb_drop_threshold", 0.03)
                    threshold_time_mins = st.session_state.get("arb_time_threshold", 5)

                    same_threshold_sum = st.session_state.get("same_sum_threshold", 0.8)
                    same_threshold_drop = st.session_state.get("same_drop_threshold", 0.03)
                    same_threshold_time_mins = st.session_state.get("same_time_threshold", 5)

                    should_record_time = True
                    should_record_same_time = True
                    
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

                    for p_name_a, p_name_b, p_prices_a, p_prices_b, p_slug_a, p_slug_b in pairs_config:
                        pair_key = f"{p_name_a}-{p_name_b}"
                        if not p_prices_a or not p_prices_b: continue
                        res = process_pair(p_name_a, p_name_b, p_prices_a, p_prices_b, p_slug_a, p_slug_b)
                        
                        if pair_key not in st.session_state["pair_min_values"]:
                            st.session_state["pair_min_values"][pair_key] = {
                                "arb_1": 1.0, "arb_2": 1.0, "same_1": 2.0, "same_2": 2.0
                            }
                        mins = st.session_state["pair_min_values"][pair_key]

                        if should_record_time:
                            val, type_base, ask_a, ask_b = res["arb_1"]
                            type_real = f"{p_name_a}_Up_{p_name_b}_Down"
                            if val <= threshold_sum:
                                if mins["arb_1"] == 1.0 or val < (mins["arb_1"] - threshold_drop):
                                    save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                    mins["arb_1"] = val
                                    status_placeholder.toast(f"New Low Arb 1 ({pair_key}): {val:.4f}", icon="📉")
                            
                            val, type_base, ask_a, ask_b = res["arb_2"]
                            type_real = f"{p_name_a}_Down_{p_name_b}_Up"
                            if val <= threshold_sum:
                                if mins["arb_2"] == 1.0 or val < (mins["arb_2"] - threshold_drop):
                                    save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                    mins["arb_2"] = val
                                    status_placeholder.toast(f"New Low Arb 2 ({pair_key}): {val:.4f}", icon="📉")

                        if should_record_same_time:
                            val, type_base, ask_a, ask_b = res["same_1"]
                            type_real = f"{p_name_a}_Up_{p_name_b}_Up"
                            if val <= same_threshold_sum:
                                if mins["same_1"] == 2.0 or val < (mins["same_1"] - same_threshold_drop):
                                    save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                    mins["same_1"] = val
                                    status_placeholder.toast(f"New Low Same 1 ({pair_key}): {val:.4f}", icon="📉")
                            
                            val, type_base, ask_a, ask_b = res["same_2"]
                            type_real = f"{p_name_a}_Down_{p_name_b}_Down"
                            if val <= same_threshold_sum:
                                if mins["same_2"] == 2.0 or val < (mins["same_2"] - same_threshold_drop):
                                    save_arb_opportunity(p_slug_a, p_slug_b, type_real, val, ask_a, ask_b, pair_key)
                                    mins["same_2"] = val
                                    status_placeholder.toast(f"New Low Same 2 ({pair_key}): {val:.4f}", icon="📉")
                            
                except ValueError:
                    pass
                
            time.sleep(0.01) 
            
            try:
                parts = btc_slug.split("-")
                end_ts = int(parts[-1])
                if auto_mode and time.time() >= end_ts:
                    st.rerun() 
            except:
                pass

    else:
            if auto_mode:
                if btc_closed or eth_closed:
                    save_round_history(btc_slug, eth_slug)
                
                if btc_closed:
                    save_result(btc_slug, "Closed", "N/A")
                    st.session_state["current_btc_slug"] = get_next_slug(btc_slug)
                    st.session_state["min_sum_1"] = 1.0
                    st.session_state["min_sum_2"] = 1.0
                
                if eth_closed:
                    save_result(eth_slug, "Closed", "N/A")
                    st.session_state["current_eth_slug"] = get_next_slug(eth_slug)
                    
                st.rerun()

# --- PAGE: Monitor 1H ---
elif page == "Monitor 1H":
    st.title("BTC & ETH 1H Series Monitor 🕐")

    with st.sidebar:
        st.header("Control Panel (1H)")
        
        default_btc_1h = get_current_slug_1h("BTC")
        default_eth_1h = get_current_slug_1h("ETH")
        
        btc_slug_1h = st.text_input("BTC 1H Slug:", value=default_btc_1h)
        eth_slug_1h = st.text_input("ETH 1H Slug:", value=default_eth_1h)
        
        if st.button("Apply 1H Slugs"):
            st.session_state["btc_slug_1h"] = btc_slug_1h
            st.session_state["eth_slug_1h"] = eth_slug_1h
            ws_manager.subscribed_tokens = set() 
            st.rerun()
            
    current_btc_slug_1h = st.session_state.get("btc_slug_1h", btc_slug_1h)
    current_eth_slug_1h = st.session_state.get("eth_slug_1h", eth_slug_1h)
    
    btc_markets, btc_err = fetch_market_data(current_btc_slug_1h)
    eth_markets, eth_err = fetch_market_data(current_eth_slug_1h)
    
    if btc_err: st.error(f"BTC 1H Error: {btc_err}")
    if eth_err: st.error(f"ETH 1H Error: {eth_err}")
    
    btc_tokens = []
    if btc_markets:
        for m in btc_markets:
            tokens = m.get("tokens", [])
            for t in tokens:
                btc_tokens.append({"token_id": t["token_id"], "outcome": t["outcome"], "market_id": m["id"]})
                
    eth_tokens = []
    if eth_markets:
        for m in eth_markets:
            tokens = m.get("tokens", [])
            for t in tokens:
                eth_tokens.append({"token_id": t["token_id"], "outcome": t["outcome"], "market_id": m["id"]})
                
    all_token_ids = [t["token_id"] for t in btc_tokens + eth_tokens]
    if all_token_ids:
        ws_manager.subscribe(all_token_ids)
        missing = [tid for tid in all_token_ids if tid not in ws_manager.prices]
        if missing:
            ws_manager.warmup_cache(missing)
            
    pair_key_1h = "BTC-ETH-1H"
    if "pair_min_values_1h" not in st.session_state:
        st.session_state["pair_min_values_1h"] = {}
    if pair_key_1h not in st.session_state["pair_min_values_1h"]:
        st.session_state["pair_min_values_1h"][pair_key_1h] = {
            "arb_1": 1.0, "arb_2": 1.0, "same_1": 2.0, "same_2": 2.0
        }

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("BTC 1H Market")
        st.caption(f"Slug: {current_btc_slug_1h}")
        if btc_markets: st.info(btc_markets[0].get("question"))
        
    with c2:
        st.subheader("ETH 1H Market")
        st.caption(f"Slug: {current_eth_slug_1h}")
        if eth_markets: st.info(eth_markets[0].get("question"))
        
    st.divider()
    st.success("✅ 1H Monitoring Active! Data is being processed in the background.")
    
    if st.button("🔄 Refresh UI (1H)"):
        st.rerun()
        
    status_placeholder = st.empty()
    last_process_ts = 0.0
    
    while True:
        current_update_ts = ws_manager.last_update
        if current_update_ts > last_process_ts:
            last_process_ts = current_update_ts
            
            def get_prices(tokens):
                return {t["outcome"]: ws_manager.get_price(t["token_id"]) for t in tokens}

            btc_prices = get_prices(btc_tokens)
            eth_prices = get_prices(eth_tokens)
            
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
                    
                up_a = get_ask(btc_prices, "Yes")
                down_a = get_ask(btc_prices, "No")
                up_b = get_ask(eth_prices, "Yes")
                down_b = get_ask(eth_prices, "No")
                
                s1 = up_a + down_b
                s2 = down_a + up_b
                ss1 = up_a + up_b
                ss2 = down_a + down_b
                
                threshold_sum = st.session_state.get("arb_sum_threshold", 0.8)
                threshold_drop = st.session_state.get("arb_drop_threshold", 0.03)
                
                mins = st.session_state["pair_min_values_1h"][pair_key_1h]
                
                if s1 <= threshold_sum:
                    if mins["arb_1"] == 1.0 or s1 < (mins["arb_1"] - threshold_drop):
                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        with open(ARB_OPPORTUNITY_FILE_1H, "a") as f:
                            if os.stat(ARB_OPPORTUNITY_FILE_1H).st_size == 0:
                                f.write("timestamp,slug_a,slug_b,type,sum,price_a,price_b,pair\n")
                            f.write(f"{timestamp},{current_btc_slug_1h},{current_eth_slug_1h},BTC_Up_ETH_Down,{s1},{up_a},{down_b},BTC-ETH\n")
                            
                        mins["arb_1"] = s1
                        status_placeholder.toast(f"New Low 1H Arb 1: {s1:.4f}", icon="🕐")
                        
                if s2 <= threshold_sum:
                    if mins["arb_2"] == 1.0 or s2 < (mins["arb_2"] - threshold_drop):
                        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        with open(ARB_OPPORTUNITY_FILE_1H, "a") as f:
                            if os.path.exists(ARB_OPPORTUNITY_FILE_1H) and os.stat(ARB_OPPORTUNITY_FILE_1H).st_size == 0:
                                f.write("timestamp,slug_a,slug_b,type,sum,price_a,price_b,pair\n")
                            elif not os.path.exists(ARB_OPPORTUNITY_FILE_1H):
                                f.write("timestamp,slug_a,slug_b,type,sum,price_a,price_b,pair\n")
                            f.write(f"{timestamp},{current_btc_slug_1h},{current_eth_slug_1h},BTC_Down_ETH_Up,{s2},{down_a},{up_b},BTC-ETH\n")
                        
                        mins["arb_2"] = s2
                        status_placeholder.toast(f"New Low 1H Arb 2: {s2:.4f}", icon="🕐")

            except ValueError:
                pass
                
        time.sleep(0.01)
        
        try:
            if auto_mode_1h:
                end_date_iso = None
                if btc_markets: end_date_iso = btc_markets[0].get("endDate")
                if end_date_iso:
                    end_dt = datetime.datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
                    if datetime.datetime.now(datetime.timezone.utc) > end_dt:
                        new_btc = get_current_slug_1h("BTC")
                        if new_btc != current_btc_slug_1h:
                             st.session_state["btc_slug_1h"] = new_btc
                             st.session_state["eth_slug_1h"] = get_current_slug_1h("ETH")
                             st.rerun()
        except:
            pass

# --- PAGE: Arb History ---
elif page == "Arb History":
    st.title("Arbitrage Opportunities History 📉")
    
    with st.expander("Recording Settings", expanded=False):
        tab1, tab2 = st.tabs(["AAA", "BBB"])
        
        with tab1:
            st.caption("Settings for AAA (BTC Up + ETH Down / BTC Down + ETH Up)")
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
            st.caption("Settings for BBB (BTC Up + ETH Up / BTC Down + ETH Down)")
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

    filter_option = st.radio("Filter Type:", ["All", "AAA", "BBB"], horizontal=True)

    if os.path.exists(ARB_OPPORTUNITY_FILE):
        try:
            df_arb = pd.read_csv(ARB_OPPORTUNITY_FILE)
            if not df_arb.empty and "timestamp" in df_arb.columns:
                df_arb = df_arb.drop_duplicates(subset=["timestamp", "type", "sum"])
                
                if filter_option == "AAA":
                    df_arb = df_arb[df_arb["type"].isin(["BTC_Up_ETH_Down", "BTC_Down_ETH_Up", "BTC_Up_SOL_Down", "BTC_Down_SOL_Up", "BTC_Up_XRP_Down", "BTC_Down_XRP_Up", "ETH_Up_SOL_Down", "ETH_Down_SOL_Up", "ETH_Up_XRP_Down", "ETH_Down_XRP_Up", "SOL_Up_XRP_Down", "SOL_Down_XRP_Up"])]
                elif filter_option == "BBB":
                    def get_category(t):
                        if "Up" in t and "Down" in t: return "AAA"
                        return "BBB"
                    
                    df_arb["category"] = df_arb["type"].apply(get_category)
                    
                    if filter_option == "AAA":
                        df_arb = df_arb[df_arb["category"] == "AAA"]
                    elif filter_option == "BBB":
                        df_arb = df_arb[df_arb["category"] == "BBB"]
                
                df_arb["Time"] = pd.to_datetime(df_arb["timestamp"], unit="s").dt.strftime('%Y-%m-%d %H:%M:%S')
                df_arb = df_arb.sort_values(by="Time", ascending=False)

                cols = ["Time", "type", "sum", "ask_a", "ask_b", "slug_a", "slug_b", "pair"]
                cols = [c for c in cols if c in df_arb.columns]
                df_arb = df_arb[cols]
                
                def highlight_slug_groups(row):
                    slug = row.get("slug_a", "")
                    hash_val = hash(slug)
                    r = (hash_val & 0xFF0000) >> 16
                    g = (hash_val & 0x00FF00) >> 8
                    b = (hash_val & 0x0000FF)
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

# --- PAGE: History Pages (Shared Logic) ---
elif page in ["同向套利 (同向结果复盘)", "异向套利 (异向结果复盘)"]:
    is_same_arb_page = (page == "同向套利 (同向结果复盘)") 
    page_title = "同向套利 (Same Result, Arb Bets)" if is_same_arb_page else "异向套利 (Opposite Result, Same Bets)"
    target_relation = "Same" if is_same_arb_page else "Opposite"
    
    st.title(f"{page_title} 📊")
    st.caption(f"✅ = {target_relation} Result | ❌ = {'Opposite' if is_same_arb_page else 'Same'} Result")
    
    if "market_history_data" not in st.session_state:
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
            now = int(time.time())
            current_block_start = (now // 900) * 900
            timestamps = [current_block_start - (i * 900) for i in range(100)]
            
            with st.spinner("Initializing historical data (last 100 rounds)..."):
                data = fetch_rounds_data(timestamps)
                st.session_state["market_history_data"] = data

    if st.button("🔄 Refresh Data"):
        current_history = st.session_state.get("market_history_data", [])
        
        max_ts = 0
        if current_history:
            max_ts = max(item["timestamp"] for item in current_history)
        else:
            max_ts = (int(time.time()) // 900) * 900 - 900
            
        now = int(time.time())
        current_block_start = (now // 900) * 900
        
        timestamps_to_update = []
        t = max_ts + 900
        while t <= current_block_start:
            timestamps_to_update.append(t)
            t += 900
            
        pending_ts = [
            item["timestamp"] for item in current_history 
            if item["BTC Result"] in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"] 
            or item["ETH Result"] in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]
        ]
        
        all_ts_to_fetch = list(set(timestamps_to_update + pending_ts))
        
        if all_ts_to_fetch:
            with st.spinner(f"Updating {len(all_ts_to_fetch)} rounds..."):
                updated_items = fetch_rounds_data(all_ts_to_fetch)
                
                history_map = {item["timestamp"]: item for item in current_history}
                for item in updated_items:
                    history_map[item["timestamp"]] = item
                    
                new_history_list = list(history_map.values())
                new_history_list.sort(key=lambda x: x["timestamp"], reverse=True)
                
                st.session_state["market_history_data"] = new_history_list
                
                MARKET_HISTORY_CACHE = "market_history_cache.json"
                try:
                    with open(MARKET_HISTORY_CACHE, "w") as f:
                        json.dump(new_history_list, f)
                except: pass
        
        st.rerun()

    current_history = st.session_state.get("market_history_data", [])
    
    if current_history:
        processed_data = []
        valid_count = 0
        resolved_count = 0
        
        for item in current_history:
            row = item.copy()
            
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
            row["Relation"] = relation
            
            if is_same_arb_page:
                row["Best Sum"] = row.get("Best Arb Sum", "-")
                row["Type"] = row.get("Arb Type", "-")
                row["Rec Time"] = row.get("Arb Time", "-")
            else:
                row["Best Sum"] = row.get("Best Same Sum", "-")
                row["Type"] = row.get("Same Type", "-")
                row["Rec Time"] = row.get("Same Time", "-")

            processed_data.append(row)
            
        df_res = pd.DataFrame(processed_data)
        
        if not df_res.empty:
            cols_order = ["Time", "Status", "BTC Result", "ETH Result", "Relation", "Best Sum", "Type", "Rec Time", "BTC Slug", "ETH Slug"]
            cols_to_show = [c for c in cols_order if c in df_res.columns]
            
            df_display = df_res[cols_to_show].copy()
            df_display = df_display.sort_values(by="Time", ascending=False)
            st.dataframe(df_display, use_container_width=True)
            
            if resolved_count > 0:
                st.metric(f"{target_relation} Success Rate", f"{valid_count}/{resolved_count}", f"{(valid_count/resolved_count)*100:.1f}%")
            else:
                st.info("No resolved rounds yet.")
    else:
        st.info("No data yet.")

# --- PAGE: Multi-Asset Analysis ---
elif page == "多币种AAA策略分析":
    st.title("多币种AAA策略分析 (Multi-Asset Correlation) 📊")
    
    with st.sidebar:
        st.divider()
        st.header("Multi-Asset Settings")
        sol_seed_slug = st.text_input("SOL Seed Slug:", value="sol-updown-15m-1767105000")
        xrp_seed_slug = st.text_input("XRP Seed Slug:", value="xrp-updown-15m-1767105000")
        
        if st.button("📥 加载/更新历史 (1000条)"):
            existing_data = {}
            if os.path.exists(MULTI_ASSET_HISTORY_FILE):
                try:
                    df_exist = pd.read_csv(MULTI_ASSET_HISTORY_FILE)
                    if not df_exist.empty:
                        for _, row in df_exist.iterrows():
                            existing_data[row["timestamp"]] = row.to_dict()
                except: pass
            
            now = int(time.time())
            current_block_start = (now // 900) * 900
            target_timestamps = [current_block_start - (i * 900) for i in range(1000)]
            
            missing_ts = []
            pending_statuses = ["Active", "Pending", "Pending (Closed)", "Error", "Not Found", "Unknown"]
            
            for ts in target_timestamps:
                if ts not in existing_data:
                    missing_ts.append(ts)
                else:
                    row = existing_data[ts]
                    needs_refresh = False
                    for key in ["btc_res", "eth_res", "sol_res", "xrp_res"]:
                        if row.get(key) in pending_statuses:
                            needs_refresh = True
                            break
                    if needs_refresh:
                        missing_ts.append(ts)
            
            missing_ts = list(set(missing_ts))
            
            if missing_ts:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                sol_prefix = "-".join(sol_seed_slug.split("-")[:-1])
                xrp_prefix = "-".join(xrp_seed_slug.split("-")[:-1])
                
                btc_prefix = "btc-updown-15m"
                eth_prefix = "eth-updown-15m"
                
                try:
                    parts = sol_prefix.split("-")
                    if len(parts) > 1:
                        suffix = "-".join(parts[1:])
                        btc_prefix = f"btc-{suffix}"
                        eth_prefix = f"eth-{suffix}"
                except:
                    pass

                def fetch_multi_round(ts):
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
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        results = list(executor.map(fetch_multi_round, batch))
                        new_records.extend(results)
                    
                    current_batch = (i // batch_size) + 1
                    progress = min(current_batch / total_batches, 1.0)
                    progress_bar.progress(progress)
                    status_text.text(f"Fetching batch {current_batch}/{total_batches}...")
                    time.sleep(0.5) 
                
                for r in new_records:
                    existing_data[r["timestamp"]] = r
                
                final_list = list(existing_data.values())
                final_list.sort(key=lambda x: x["timestamp"], reverse=True)
                
                pd.DataFrame(final_list).to_csv(MULTI_ASSET_HISTORY_FILE, index=False)
                st.success(f"Updated {len(new_records)} new records!")
                st.rerun()
            else:
                st.info("Data is already up to date.")

    df = pd.DataFrame()
    if os.path.exists(MULTI_ASSET_HISTORY_FILE):
        try:
            df = pd.read_csv(MULTI_ASSET_HISTORY_FILE)
        except: pass
        
    if not df.empty:
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
        
        valid_df = df[
            (~df[col_a].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"])) &
            (~df[col_b].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]))
        ].copy()
        
        valid_df["is_same"] = valid_df[col_a] == valid_df[col_b]
        
        total_samples = len(valid_df)
        same_count = valid_df["is_same"].sum()
        corr_rate = (same_count / total_samples * 100) if total_samples > 0 else 0
        
        st.metric(
            label=f"同向占比 (Correlation Rate) - {pair}",
            value=f"{corr_rate:.1f}%",
            delta=f"样本数: {total_samples}"
        )

        best_prices = {} 
        if os.path.exists(ARB_OPPORTUNITY_FILE):
            try:
                df_arb = pd.read_csv(ARB_OPPORTUNITY_FILE)
                if not df_arb.empty:
                    if "pair" not in df_arb.columns:
                        df_arb["pair"] = "BTC-ETH"
                        rename_map = {}
                        if "btc_slug" in df_arb.columns: rename_map["btc_slug"] = "slug_a"
                        if "eth_slug" in df_arb.columns: rename_map["eth_slug"] = "slug_b"
                        if rename_map:
                            df_arb.rename(columns=rename_map, inplace=True)
                    
                    target_pair = f"{asset_a_name}-{asset_b_name}"
                    target_pair_rev = f"{asset_b_name}-{asset_a_name}"
                    
                    df_arb_pair = df_arb[df_arb["pair"].isin([target_pair, target_pair_rev])].copy()
                    
                    if not df_arb_pair.empty:
                        def extract_round_ts(slug):
                            try:
                                return int(slug.split("-")[-1])
                            except:
                                return 0
                        
                        df_arb_pair["round_ts"] = df_arb_pair["slug_a"].apply(extract_round_ts)
                        
                        t1 = f"{asset_a_name}_Up_{asset_b_name}_Down"
                        t2 = f"{asset_a_name}_Down_{asset_b_name}_Up"
                        t3 = f"{asset_b_name}_Up_{asset_a_name}_Down"
                        t4 = f"{asset_b_name}_Down_{asset_a_name}_Up"
                        
                        df_same = df_arb_pair[df_arb_pair["type"].isin([t1, t2, t3, t4])]
                        
                        if not df_same.empty:
                            min_series = df_same.groupby("round_ts")["sum"].min()
                            best_prices = min_series.to_dict()
            except Exception as e:
                pass

        st.subheader("详细历史数据")
        
        display_df = df.copy()
        
        def refine_relation(row):
            v_a = row[col_a]
            v_b = row[col_b]
            if v_a in ["Pending", "Active", "Pending (Closed)"] or v_b in ["Pending", "Active", "Pending (Closed)"]:
                return "⏳ Pending"
            if v_a == "Error" or v_b == "Error":
                return "⚠️ Error"
            return "✅ 同向" if v_a == v_b else "❌ 异向"
            
        display_df["Relation"] = display_df.apply(refine_relation, axis=1)
        
        def get_best_price(row):
            ts = row["timestamp"]
            return f"{best_prices[ts]:.4f}" if ts in best_prices else "-"
            
        display_df["AAA Best Price"] = display_df.apply(get_best_price, axis=1)
        
        cols = ["Time", col_a, col_b, "Relation", "AAA Best Price"]
        display_df = display_df[cols].sort_values(by="Time", ascending=False)
        
        display_df.columns = ["Time", f"{asset_a_name} Result", f"{asset_b_name} Result", "Relation", "AAA Best Price"]
        
        st.dataframe(display_df, use_container_width=True)
        
# --- PAGE: Multi-Asset AAA Analysis 1H ---
elif page == "多币种AAA策略分析-1h":
    st.title("多币种AAA策略分析 (1H) 📊")
    
    st.info("Analyzing historical correlation (Both Up or Both Down) for 1H markets.")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        asset_a_name = st.selectbox("Asset A", ["BTC", "ETH", "SOL", "XRP"], index=0, key="1h_a")
    with c2:
        asset_b_name = st.selectbox("Asset B", ["BTC", "ETH", "SOL", "XRP"], index=1, key="1h_b")
    with c3:
        limit_count = st.number_input("Fetch Count (Last N)", min_value=10, max_value=500, value=50, step=10, key="1h_limit")

    pair = f"{asset_a_name}-{asset_b_name}"
    
    if st.button("📥 加载/更新历史 (1H)"):
        with st.spinner(f"Fetching last {limit_count} rounds for {pair} (1H)..."):
            import datetime
            from datetime import timezone, timedelta
            
            now_utc = datetime.datetime.now(timezone.utc)
            current_hour_utc = now_utc.replace(minute=0, second=0, microsecond=0)
            
            def generate_1h_slug(dt_utc, asset_code):
                name_map = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple"}
                dt_et = dt_utc - timedelta(hours=5)
                month_name = dt_et.strftime("%B").lower()
                day = dt_et.day
                hour_24 = dt_et.hour
                if hour_24 == 0: hour_str = "12am"
                elif hour_24 < 12: hour_str = f"{hour_24}am"
                elif hour_24 == 12: hour_str = "12pm"
                else: hour_str = f"{hour_24-12}pm"
                asset_full = name_map.get(asset_code, asset_code.lower())
                return f"{asset_full}-up-or-down-{month_name}-{day}-{hour_str}-et"
                
            ts_list = []
            for i in range(limit_count):
                past_time = current_hour_utc - timedelta(hours=i)
                ts_list.append(past_time) 
            
            results = []
            
            from concurrent.futures import ThreadPoolExecutor
            
            def process_round_1h(dt_obj):
                slug_a = generate_1h_slug(dt_obj, asset_a_name)
                slug_b = generate_1h_slug(dt_obj, asset_b_name)
                
                res_a = get_market_winner(slug_a)
                res_b = get_market_winner(slug_b)
                
                relation = "❌"
                if res_a not in ["Pending", "Pending (Closed)", "Active", "Error", "Not Found"] and \
                   res_a == res_b:
                    relation = "✅"
                
                return {
                    "Time": dt_obj.strftime('%Y-%m-%d %H:%M ET'),
                    f"{asset_a_name} Result": res_a,
                    f"{asset_b_name} Result": res_b,
                    "Relation": relation,
                    "slug_a": slug_a,
                    "timestamp": int(dt_obj.timestamp())
                }

            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(process_round_1h, ts_list))
                
            df_1h = pd.DataFrame(results)
            st.session_state[f"history_1h_{pair}"] = df_1h
            st.success("Done!")
            
    if f"history_1h_{pair}" in st.session_state:
        df = st.session_state[f"history_1h_{pair}"]
        col_a = f"{asset_a_name} Result"
        col_b = f"{asset_b_name} Result"
        
        valid_df = df[
            (~df[col_a].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"])) &
            (~df[col_b].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]))
        ].copy()
        
        valid_df["is_same"] = valid_df[col_a] == valid_df[col_b]
        
        total = len(valid_df)
        same_count = valid_df["is_same"].sum()
        rate = (same_count / total * 100) if total > 0 else 0
        
        st.metric(f"同向占比 (Correlation Rate) - {pair} (1H)", f"{rate:.1f}%", f"Samples: {total}")
        
        best_prices = {}
        if os.path.exists(ARB_OPPORTUNITY_FILE_1H):
            try:
                df_arb_1h = pd.read_csv(ARB_OPPORTUNITY_FILE_1H)
                if not df_arb_1h.empty:
                    target = f"{asset_a_name}-{asset_b_name}"
                    target_rev = f"{asset_b_name}-{asset_a_name}"
                    if "pair" in df_arb_1h.columns:
                        df_arb_pair = df_arb_1h[df_arb_1h["pair"].isin([target, target_rev])]
                    else:
                        df_arb_pair = df_arb_1h 
                        
                    if not df_arb_pair.empty:
                        t1 = f"{asset_a_name}_Up_{asset_b_name}_Down"
                        t2 = f"{asset_a_name}_Down_{asset_b_name}_Up"
                        t3 = f"{asset_b_name}_Up_{asset_a_name}_Down"
                        t4 = f"{asset_b_name}_Down_{asset_a_name}_Up"
                        
                        df_aaa = df_arb_pair[df_arb_pair["type"].isin([t1, t2, t3, t4])]
                        
                        if not df_aaa.empty:
                            min_series = df_aaa.groupby("slug_a")["sum"].min()
                            best_prices = min_series.to_dict()
            except:
                pass
                
        display_df = df.copy()
        
        def get_aaa_price(row):
            slug = row["slug_a"]
            return f"{best_prices[slug]:.4f}" if slug in best_prices else "-"
            
        display_df["AAA Best Price"] = display_df.apply(get_aaa_price, axis=1)
        
        cols = ["Time", col_a, col_b, "Relation", "AAA Best Price"]
        st.dataframe(display_df[cols], use_container_width=True)
        
    else:
        st.info("Please load history.")

else:
    st.error(f"⚠️ Page not found: {page}")
    st.write(f"Selected Page Value: '{page}'")
