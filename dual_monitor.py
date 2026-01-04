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
        "public_url": st.session_state.get("public_url", "")
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
MULTI_ASSET_HISTORY_1H_FILE = "multi_asset_history_1h.csv"

# --- Helper Functions ---
def get_current_slug_1h(asset="BTC"):
    name_map = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "XRP": "xrp"
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

def parse_json_field(field_value):
    if isinstance(field_value, str):
        try:
            return json.loads(field_value)
        except json.JSONDecodeError:
            return []
    return field_value

@st.cache_data(ttl=5)
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

def save_arb_opportunity_1h(slug_a, slug_b, type_name, sum_val, ask_a, ask_b, pair_name="BTC-ETH"):
    timestamp = int(time.time())
    file_exists = os.path.isfile(ARB_OPPORTUNITY_FILE_1H) and os.stat(ARB_OPPORTUNITY_FILE_1H).st_size > 0
    with open(ARB_OPPORTUNITY_FILE_1H, "a") as f:
        if not file_exists:
            f.write("timestamp,slug_a,slug_b,type,sum,price_a,price_b,pair\n")
        f.write(f"{timestamp},{slug_a},{slug_b},{type_name},{sum_val},{ask_a},{ask_b},{pair_name}\n")

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
            self.prices[token_id] = {"bid": best_bid, "ask": best_ask, "ts": time.time()}
            self.last_update = time.time()

    def _update_price_direct(self, token_id, bid, ask):
        with self.lock:
            current = self.prices.get(token_id, {"bid": "N/A", "ask": "N/A", "ts": 0})
            new_bid = bid if bid is not None else current["bid"]
            new_ask = ask if ask is not None else current["ask"]
            self.prices[token_id] = {"bid": new_bid, "ask": new_ask, "ts": time.time()}
            self.last_update = time.time()

    def get_price(self, token_id):
        with self.lock:
            return self.prices.get(token_id, {"bid": "Loading...", "ask": "Loading...", "ts": 0})

    def fetch_snapshot(self, token_ids):
        """Force fetch latest prices via HTTP for given tokens."""
        def fetch_single(token_id):
            try:
                url = f"{CLOB_API_URL}/book"
                params = {"token_id": token_id}
                response = requests.get(url, params=params, verify=False, timeout=3)
                if response.status_code == 200:
                    data = response.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    self._update_price_from_book(token_id, bids, asks)
            except Exception as e:
                print(f"Snapshot error for {token_id}: {e}")
        
        # Use a thread pool to fetch in parallel, but wait for completion
        with ThreadPoolExecutor(max_workers=20) as executor:
            list(executor.map(fetch_single, token_ids))

# --- Background Monitor Core ---
class MonitorCore:
    def __init__(self, ws_manager):
        self.ws_manager = ws_manager
        self.running = False
        self.thread = None
        self.lock = threading.Lock()
        self.processing_lock = threading.Lock()
        
        # State
        self.settings = load_settings()
        self.mins_15m = {}
        self.mins_1h = {}
        
        # Real-time latest sums (no threshold filtering)
        self.latest_15m = {}
        self.latest_1h = {}
        
        # Slugs (15m)
        self.btc_slug_15m = get_latest_active_slug("btc-updown-15m")
        self.eth_slug_15m = get_latest_active_slug("eth-updown-15m")
        self.sol_slug_15m = get_latest_active_slug("sol-updown-15m")
        self.xrp_slug_15m = get_latest_active_slug("xrp-updown-15m")
        
        # Slugs (1H)
        self.btc_slug_1h = get_current_slug_1h("BTC")
        self.eth_slug_1h = get_current_slug_1h("ETH")
        self.sol_slug_1h = get_current_slug_1h("SOL")
        self.xrp_slug_1h = get_current_slug_1h("XRP")
        
        # Tokens
        self.tokens_15m = {} # {slug: [tokens]}
        self.tokens_1h = {} 
        
        # Init
        self._update_tokens_15m()
        self._update_tokens_1h()
        
        # Keep-Alive
        threading.Thread(target=self._keep_alive_loop, daemon=True).start()

    def start(self):
        if self.running: return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        
    def update_settings(self, new_settings):
        with self.lock:
            self.settings.update(new_settings)

    def _keep_alive_loop(self):
        """
        Periodically pings localhost and public URL to prevent Streamlit Cloud sleep.
        Runs every 15 seconds.
        """
        while True:
            time.sleep(15)
            # 1. Log heartbeat (shows activity in Streamlit Cloud logs)
            print(f"[KeepAlive] Heartbeat at {datetime.datetime.now().strftime('%H:%M:%S')}")
            
            # 2. Ping Localhost (Internal traffic)
            try:
                requests.get("http://localhost:8501/_stcore/health", timeout=5)
            except:
                pass
                
            # 3. Ping Public URL (Simulate external user)
            public_url = self.settings.get("public_url", "")
            if public_url and public_url.startswith("http"):
                try:
                    requests.get(public_url, timeout=10)
                    print(f"[KeepAlive] Pinged public URL: {public_url}")
                except Exception as e:
                    print(f"[KeepAlive] Failed to ping public URL: {e}")
            
    def _update_tokens_15m(self):
        for slug in [self.btc_slug_15m, self.eth_slug_15m, self.sol_slug_15m, self.xrp_slug_15m]:
            markets, _ = fetch_market_data(slug)
            tokens = []
            if markets:
                m = markets[0]
                if not m.get("closed"):
                    outcomes = parse_json_field(m.get("outcomes", []))
                    ids = parse_json_field(m.get("clobTokenIds", []))
                    for i, tid in enumerate(ids):
                        o = outcomes[i] if i < len(outcomes) else str(i)
                        tokens.append({"outcome": o, "token_id": tid})
            self.tokens_15m[slug] = tokens
            if tokens:
                self.ws_manager.subscribe([t["token_id"] for t in tokens])
                # Warmup via fetch_snapshot (async non-blocking for init)
                threading.Thread(target=lambda: self.ws_manager.fetch_snapshot([t["token_id"] for t in tokens]), daemon=True).start()

    def _update_tokens_1h(self):
        for slug in [self.btc_slug_1h, self.eth_slug_1h, self.sol_slug_1h, self.xrp_slug_1h]:
            markets, _ = fetch_market_data(slug)
            tokens = []
            if markets:
                m = markets[0]
                if not m.get("closed"):
                    outcomes = parse_json_field(m.get("outcomes", []))
                    ids = parse_json_field(m.get("clobTokenIds", []))
                    for i, tid in enumerate(ids):
                        o = outcomes[i] if i < len(outcomes) else str(i)
                        tokens.append({"outcome": o, "token_id": tid})
            self.tokens_1h[slug] = tokens
            if tokens:
                self.ws_manager.subscribe([t["token_id"] for t in tokens])
                threading.Thread(target=lambda: self.ws_manager.fetch_snapshot([t["token_id"] for t in tokens]), daemon=True).start()

    def _loop(self):
        while self.running:
            time.sleep(0.5)
            try:
                self._sync_slugs_15m()
                
                # Retry token fetch if missing
                if not self.tokens_15m.get(self.btc_slug_15m):
                     self._update_tokens_15m()
                
                with self.processing_lock:
                    self._process_15m()
                    self._process_1h()
                
                self._check_rollover_15m()
                self._check_rollover_1h()
            except Exception as e:
                print(f"Monitor Loop Error: {e}")

    def force_process(self):
        """Force an immediate calculation update."""
        # Check staleness and force fetch if needed
        all_tokens = []
        for slug, tokens in self.tokens_15m.items():
            all_tokens.extend([t["token_id"] for t in tokens])
        for slug, tokens in self.tokens_1h.items():
            all_tokens.extend([t["token_id"] for t in tokens])
        
        # Identify stale tokens (older than 5s)
        now = time.time()
        stale_ids = []
        for tid in all_tokens:
            p_data = self.ws_manager.prices.get(tid)
            if not p_data or (now - p_data.get("ts", 0) > 5):
                stale_ids.append(tid)
        
        if stale_ids:
            # print(f"Refreshing {len(stale_ids)} stale tokens via HTTP...")
            self.ws_manager.fetch_snapshot(stale_ids)

        with self.processing_lock:
            self._process_15m()
            self._process_1h()

    def _sync_slugs_15m(self):
        # Safety check: if we are ahead of time (e.g. due to previous bug), reset to current.
        expected_btc = get_latest_active_slug("btc-updown-15m")
        try:
            curr_ts = int(self.btc_slug_15m.split("-")[-1])
            exp_ts = int(expected_btc.split("-")[-1])
            
            # If current slug is in future OR significantly in past (more than 2 rounds/30 mins behind)
            if curr_ts > exp_ts or (exp_ts - curr_ts) > 1800:
                print(f"Correcting slug from {self.btc_slug_15m} to {expected_btc}")
                self.btc_slug_15m = expected_btc
                self.eth_slug_15m = get_latest_active_slug("eth-updown-15m")
                self.sol_slug_15m = get_latest_active_slug("sol-updown-15m")
                self.xrp_slug_15m = get_latest_active_slug("xrp-updown-15m")
                self.mins_15m = {}
                self._update_tokens_15m()
        except: pass

    def _get_prices(self, tokens):
        return {t["outcome"]: self.ws_manager.get_price(t["token_id"]) for t in tokens}

    def _safe_float(self, val):
        try: return float(val)
        except: return 999.0

    def _get_ask(self, prices, outcome_name):
        val = prices.get(outcome_name, {}).get("ask")
        if val: return self._safe_float(val)
        if outcome_name == "Yes": return self._safe_float(prices.get("Up", {}).get("ask"))
        if outcome_name == "No": return self._safe_float(prices.get("Down", {}).get("ask"))
        return 999.0

    def _process_15m(self):
        # Prepare Data
        btc_p = self._get_prices(self.tokens_15m.get(self.btc_slug_15m, []))
        eth_p = self._get_prices(self.tokens_15m.get(self.eth_slug_15m, []))
        sol_p = self._get_prices(self.tokens_15m.get(self.sol_slug_15m, []))
        xrp_p = self._get_prices(self.tokens_15m.get(self.xrp_slug_15m, []))
        
        pairs = [
            ("BTC", "ETH", btc_p, eth_p, self.btc_slug_15m, self.eth_slug_15m),
            ("BTC", "SOL", btc_p, sol_p, self.btc_slug_15m, self.sol_slug_15m),
            ("BTC", "XRP", btc_p, xrp_p, self.btc_slug_15m, self.xrp_slug_15m),
            ("ETH", "SOL", eth_p, sol_p, self.eth_slug_15m, self.sol_slug_15m),
            ("ETH", "XRP", eth_p, xrp_p, self.eth_slug_15m, self.xrp_slug_15m),
            ("SOL", "XRP", sol_p, xrp_p, self.sol_slug_15m, self.xrp_slug_15m),
        ]
        
        # --- Update Real-time Snapshot (Always run this) ---
        if not hasattr(self, 'latest_15m'): self.latest_15m = {}
        
        for na, nb, pa, pb, _, _ in pairs:
            if not pa or not pb: continue
            
            up_a = self._get_ask(pa, "Yes")
            down_a = self._get_ask(pa, "No")
            up_b = self._get_ask(pb, "Yes")
            down_b = self._get_ask(pb, "No")
            
            s1 = up_a + down_b
            s2 = down_a + up_b
            
            key = f"{na}-{nb}"
            self.latest_15m[key] = {
                "Up A": round(up_a, 4),
                "Down B": round(down_b, 4),
                "Sum 1 (Up A+Down B)": round(s1, 4),
                "Down A": round(down_a, 4),
                "Up B": round(up_b, 4),
                "Sum 2 (Down A+Up B)": round(s2, 4),
                "Time": datetime.datetime.now().strftime("%H:%M:%S")
            }

        # --- Recording Logic (Conditional) ---
        should_record = True
        try:
            parts = self.btc_slug_15m.split("-")
            start_ts = int(parts[-1])
            end_ts = start_ts + 900
            # If current time is past the recording cutoff (end - threshold), don't record
            if (end_ts - time.time()) < (self.settings.get("arb_time_threshold", 5) * 60):
                should_record = False
        except: pass

        # Always update mins_15m structure even if not recording, so UI shows default 1.0 or current low
        for na, nb, _, _, _, _ in pairs:
             key = f"{na}-{nb}"
             if key not in self.mins_15m: self.mins_15m[key] = {"arb_1": 1.0, "arb_2": 1.0}

        if should_record:
            for na, nb, pa, pb, sa, sb in pairs:
                if not pa or not pb: continue
                
                up_a = self._get_ask(pa, "Yes")
                down_a = self._get_ask(pa, "No")
                up_b = self._get_ask(pb, "Yes")
                down_b = self._get_ask(pb, "No")
                
                s1 = up_a + down_b
                s2 = down_a + up_b
                
                key = f"{na}-{nb}"
                
                thresh = self.settings.get("arb_sum_threshold", 0.8)
                drop = self.settings.get("arb_drop_threshold", 0.03)
                
                # Arb 1 (Up A + Down B)
                if s1 <= thresh:
                    if self.mins_15m[key]["arb_1"] == 1.0 or s1 < (self.mins_15m[key]["arb_1"] - drop):
                        save_arb_opportunity(sa, sb, f"{na}_Up_{nb}_Down", s1, up_a, down_b, key)
                        self.mins_15m[key]["arb_1"] = s1
                
                # Arb 2 (Down A + Up B)
                if s2 <= thresh:
                    if self.mins_15m[key]["arb_2"] == 1.0 or s2 < (self.mins_15m[key]["arb_2"] - drop):
                        save_arb_opportunity(sa, sb, f"{na}_Down_{nb}_Up", s2, down_a, up_b, key)
                        self.mins_15m[key]["arb_2"] = s2
        else:
             # Even if not recording to CSV, we can optionally update the UI "mins" 
             # to show the lowest value seen SO FAR in this session, 
             # OR we keep it as is (only recording updates mins).
             # User issue: Real-time snapshot shows low values (e.g. 0.91), but Current Mins shows 1.0 or 0.89.
             # The issue is likely that "Current Mins" only updates when conditions are met AND within time window.
             # Let's make "Current Mins" reflect the lowest value seen in this round, regardless of recording.
             
            for na, nb, pa, pb, _, _ in pairs:
                if not pa or not pb: continue
                
                up_a = self._get_ask(pa, "Yes")
                down_a = self._get_ask(pa, "No")
                up_b = self._get_ask(pb, "Yes")
                down_b = self._get_ask(pb, "No")
                
                s1 = up_a + down_b
                s2 = down_a + up_b
                
                key = f"{na}-{nb}"
                
                # Just update in-memory minimums for UI display
                if s1 < self.mins_15m[key]["arb_1"]:
                    self.mins_15m[key]["arb_1"] = s1
                if s2 < self.mins_15m[key]["arb_2"]:
                    self.mins_15m[key]["arb_2"] = s2

    def _process_1h(self):
        btc_p = self._get_prices(self.tokens_1h.get(self.btc_slug_1h, []))
        eth_p = self._get_prices(self.tokens_1h.get(self.eth_slug_1h, []))
        sol_p = self._get_prices(self.tokens_1h.get(self.sol_slug_1h, []))
        xrp_p = self._get_prices(self.tokens_1h.get(self.xrp_slug_1h, []))
        
        pairs = [
            ("BTC", "ETH", btc_p, eth_p, self.btc_slug_1h, self.eth_slug_1h),
            ("BTC", "SOL", btc_p, sol_p, self.btc_slug_1h, self.sol_slug_1h),
            ("BTC", "XRP", btc_p, xrp_p, self.btc_slug_1h, self.xrp_slug_1h),
            ("ETH", "SOL", eth_p, sol_p, self.eth_slug_1h, self.sol_slug_1h),
            ("ETH", "XRP", eth_p, xrp_p, self.eth_slug_1h, self.xrp_slug_1h),
            ("SOL", "XRP", sol_p, xrp_p, self.sol_slug_1h, self.xrp_slug_1h),
        ]
        
        # --- Update Real-time Snapshot (Always run this) ---
        if not hasattr(self, 'latest_1h'): self.latest_1h = {}
        
        for na, nb, pa, pb, _, _ in pairs:
            if not pa or not pb: continue
            
            up_a = self._get_ask(pa, "Yes")
            down_a = self._get_ask(pa, "No")
            up_b = self._get_ask(pb, "Yes")
            down_b = self._get_ask(pb, "No")
            
            s1 = up_a + down_b
            s2 = down_a + up_b
            
            key = f"{na}-{nb}"
            self.latest_1h[key] = {
                "Up A": round(up_a, 4),
                "Down B": round(down_b, 4),
                "Sum 1 (Up A+Down B)": round(s1, 4),
                "Down A": round(down_a, 4),
                "Up B": round(up_b, 4),
                "Sum 2 (Down A+Up B)": round(s2, 4),
                "Time": datetime.datetime.now().strftime("%H:%M:%S")
            }

        # --- Recording Logic ---
        # 1H recording usually doesn't have strict time threshold like 15m (5 mins before close), 
        # but we check thresholds.

        # Check time threshold for 1H markets as well
        should_record_1h = True
        try:
            # 1H markets end at the top of the hour.
            # We can parse the slug or just assume it closes at next :00.
            # Actually fetching market data gives 'endDate'.
            # For simplicity, let's assume standard 1H markets close at the next hour mark relative to their creation.
            # Or simpler: if current minute > (60 - threshold), stop recording.
            # e.g. if threshold is 5 mins, stop recording at XX:55.
            current_minute = datetime.datetime.now().minute
            if current_minute >= (60 - self.settings.get("arb_time_threshold", 5)):
                should_record_1h = False
        except: pass

        # Always update mins_1h structure regardless of recording condition
        for na, nb, _, _, _, _ in pairs:
             key = f"{na}-{nb}"
             if key not in self.mins_1h: self.mins_1h[key] = {"arb_1": 1.0, "arb_2": 1.0}

        for na, nb, pa, pb, sa, sb in pairs:
            if not pa or not pb: continue
            
            up_a = self._get_ask(pa, "Yes")
            down_a = self._get_ask(pa, "No")
            up_b = self._get_ask(pb, "Yes")
            down_b = self._get_ask(pb, "No")
            
            s1 = up_a + down_b
            s2 = down_a + up_b
            
            key = f"{na}-{nb}"
            
            thresh = self.settings.get("arb_sum_threshold", 0.8)
            drop = self.settings.get("arb_drop_threshold", 0.03)
            
            # --- UI Display Update (Always update lowest seen value) ---
            if s1 < self.mins_1h[key]["arb_1"]:
                self.mins_1h[key]["arb_1"] = s1
            if s2 < self.mins_1h[key]["arb_2"]:
                self.mins_1h[key]["arb_2"] = s2

            # --- Recording to CSV (Conditional) ---
            # Arb 1 (Up A + Down B)
            if s1 <= thresh:
                # We only save if it's a new low or first time (using a separate tracking mechanism ideally, 
                # but reusing mins_1h logic for "significant drop" check is fine for now, 
                # provided we understand mins_1h is now "session low")
                # To avoid spamming CSV, we might need a separate tracker for "last recorded value".
                # But for simplicity, we'll just save every time it hits threshold if we want full history?
                # The original logic was: save if < current_min - drop.
                # Since mins_1h is now real-time low, this logic still holds somewhat, 
                # but if s1 slowly creeps down, it will update mins_1h but might not trigger "drop" threshold.
                # Actually, original logic was: if s1 < (self.mins_1h[key]["arb_1"] - drop).
                # Since we just updated self.mins_1h[key]["arb_1"] = s1 above, this condition s1 < s1 - drop is impossible.
                # FIX: We need to separate "Session Low" (for UI) from "Last Recorded Low" (for CSV throttling).
                pass 
                # Re-implementing correctly below:
            
            # Let's use a separate dict for recording thresholds to avoid conflict with UI display
            if not hasattr(self, 'last_recorded_1h'): self.last_recorded_1h = {}
            if key not in self.last_recorded_1h: self.last_recorded_1h[key] = {"arb_1": 1.0, "arb_2": 1.0}
            
            if not should_record_1h: continue

            if s1 <= thresh:
                 if self.last_recorded_1h[key]["arb_1"] == 1.0 or s1 < (self.last_recorded_1h[key]["arb_1"] - drop):
                    save_arb_opportunity_1h(sa, sb, f"{na}_Up_{nb}_Down", s1, up_a, down_b, key)
                    self.last_recorded_1h[key]["arb_1"] = s1

            if s2 <= thresh:
                 if self.last_recorded_1h[key]["arb_2"] == 1.0 or s2 < (self.last_recorded_1h[key]["arb_2"] - drop):
                    save_arb_opportunity_1h(sa, sb, f"{na}_Down_{nb}_Up", s2, down_a, up_b, key)
                    self.last_recorded_1h[key]["arb_2"] = s2

    def _check_rollover_15m(self):
        try:
            parts = self.btc_slug_15m.split("-")
            start_ts = int(parts[-1])
            end_ts = start_ts + 900
            if time.time() >= end_ts:
                # Save Round History
                save_round_history(self.btc_slug_15m, self.eth_slug_15m)
                save_result(self.btc_slug_15m, "Closed", "N/A")
                save_result(self.eth_slug_15m, "Closed", "N/A")
                
                # Advance Slugs
                self.btc_slug_15m = get_next_slug(self.btc_slug_15m)
                self.eth_slug_15m = get_next_slug(self.eth_slug_15m)
                self.sol_slug_15m = get_next_slug(self.sol_slug_15m)
                self.xrp_slug_15m = get_next_slug(self.xrp_slug_15m)
                
                # Reset Mins
                self.mins_15m = {}
                
                # Update Tokens
                self._update_tokens_15m()
        except: pass

    def _check_rollover_1h(self):
        try:
            markets, _ = fetch_market_data(self.btc_slug_1h)
            if markets:
                end_date_iso = markets[0].get("endDate")
                if end_date_iso:
                    end_dt = datetime.datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
                    if datetime.datetime.now(datetime.timezone.utc) > end_dt:
                        new_btc = get_current_slug_1h("BTC")
                        if new_btc != self.btc_slug_1h:
                            self.btc_slug_1h = new_btc
                            self.eth_slug_1h = get_current_slug_1h("ETH")
                            self.sol_slug_1h = get_current_slug_1h("SOL")
                            self.xrp_slug_1h = get_current_slug_1h("XRP")
                            self.mins_1h = {}
                            self._update_tokens_1h()
        except: pass

@st.cache_resource
def get_ws_manager():
    manager = PolymarketWSManager()
    manager.start()
    return manager

@st.cache_resource
def get_monitor_core():
    ws = get_ws_manager()
    core = MonitorCore(ws)
    core.start()
    return core

# --- Main App ---
st.set_page_config(page_title="Dual Crypto Monitor", layout="wide")

core = get_monitor_core()

# --- Hotfix for cached instance (Schema Migration) ---
if not hasattr(core, 'sol_slug_1h'):
    core.sol_slug_1h = get_current_slug_1h("SOL")
if not hasattr(core, 'xrp_slug_1h'):
    core.xrp_slug_1h = get_current_slug_1h("XRP")
    # Trigger token update since we added new slugs
    core._update_tokens_1h()

# --- Sync Settings from UI to Core ---
loaded = load_settings()
if "arb_sum_threshold" not in st.session_state:
    st.session_state["arb_sum_threshold"] = loaded.get("arb_sum_threshold", 0.8)
if "arb_drop_threshold" not in st.session_state:
    st.session_state["arb_drop_threshold"] = loaded.get("arb_drop_threshold", 0.03)
if "arb_time_threshold" not in st.session_state:
    st.session_state["arb_time_threshold"] = loaded.get("arb_time_threshold", 5)
if "public_url" not in st.session_state:
    st.session_state["public_url"] = loaded.get("public_url", "")

core.update_settings({
    "arb_sum_threshold": st.session_state["arb_sum_threshold"],
    "arb_drop_threshold": st.session_state["arb_drop_threshold"],
    "arb_time_threshold": st.session_state["arb_time_threshold"],
    "public_url": st.session_state["public_url"]
})

# --- Navigation ---
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to:", ["Monitor", "Monitor 1H", "Arb History", "多币种AAA策略分析", "多币种AAA策略分析-1h"])

st.sidebar.divider()
st.sidebar.caption("✅ Background Monitor Running")
st.sidebar.caption(f"15m: {core.btc_slug_15m}")
st.sidebar.caption(f"    {core.sol_slug_15m}")
st.sidebar.caption(f"    {core.xrp_slug_15m}")
st.sidebar.caption(f"1H: {core.btc_slug_1h}")
st.sidebar.caption(f"    {core.sol_slug_1h}")
st.sidebar.caption(f"    {core.xrp_slug_1h}")

if st.sidebar.button("🗑️ Clear All Caches"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

with st.sidebar.expander("🛠️ Debug Info", expanded=False):
        st.write(f"System Time: {datetime.datetime.now().strftime('%H:%M:%S')}")
        st.write(f"Monitor Running: {core.running}")
        st.write(f"Expected 15m Slug: {get_latest_active_slug('btc-updown-15m')}")
        
        # Show token counts to verify data fetching
        btc_toks = len(core.tokens_15m.get(core.btc_slug_15m, []))
        eth_toks = len(core.tokens_15m.get(core.eth_slug_15m, []))
        st.write(f"Tokens Found: BTC({btc_toks}), ETH({eth_toks})")
        
        if st.button("Force Sync Slugs"):
             core._sync_slugs_15m()
             st.rerun()

        st.divider()
        st.subheader("Raw Prices (15m)")
        
        # Display WS Last Update Time for each token
        if core.tokens_15m:
            token_status = []
            for slug, tokens in core.tokens_15m.items():
                for t in tokens:
                    tid = t["token_id"]
                    p_data = core.ws_manager.prices.get(tid, {})
                    last_ts = p_data.get("ts", 0)
                    lag = time.time() - last_ts if last_ts > 0 else 999
                    token_status.append({
                        "Slug": slug,
                        "Token": t["outcome"],
                        "Price": p_data.get("ask"),
                        "Lag (s)": f"{lag:.1f}"
                    })
            st.dataframe(pd.DataFrame(token_status), use_container_width=True)

        if st.button("Show Raw Prices"):
            # Fetch current prices for BTC/ETH
            btc_raw = core._get_prices(core.tokens_15m.get(core.btc_slug_15m, []))
            eth_raw = core._get_prices(core.tokens_15m.get(core.eth_slug_15m, []))
            st.json({"BTC": btc_raw, "ETH": eth_raw})

# --- Additional Helpers ---
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
    
    for r in results:
        ts = r["timestamp"]
        btc_slug = r["BTC Slug"]
        
        best_arb_sum = None
        best_arb_type = ""
        best_arb_time = ""
        
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

        r["Best Arb Sum"] = f"{best_arb_sum:.4f}" if best_arb_sum is not None else "-"
        r["Arb Type"] = best_arb_type if best_arb_type else "-"
        r["Arb Time"] = best_arb_time if best_arb_time else "-"
        final_results.append(r)

    return final_results

# --- PAGE: Monitor ---
if page == "Monitor":
    st.title("BTC & ETH 15m Series Monitor 🚀")
    
    # Just display data from core
    st.info(f"Monitoring: {core.btc_slug_15m} | {core.eth_slug_15m} | {core.sol_slug_15m} | {core.xrp_slug_15m}")
    
    if st.button("🔄 Refresh View"):
        core.force_process()
        st.rerun()
        
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Current Mins (15m)")
        # st.json(core.mins_15m)
        if core.mins_15m:
            data = []
            for k, v in core.mins_15m.items():
                row = {"Pair": k}
                row.update(v)
                data.append(row)
            df_mins = pd.DataFrame(data).set_index("Pair")
            st.dataframe(df_mins, use_container_width=True)
        else:
            st.info("No arbitrage opportunities yet.")

    with c2:
        st.subheader("Real-time Snapshot (No Filter)")
        if hasattr(core, 'latest_15m') and core.latest_15m:
            # st.json(core.latest_15m)
            data = []
            for k, v in core.latest_15m.items():
                row = {"Pair": k}
                row.update(v)
                data.append(row)
            df_snap = pd.DataFrame(data).set_index("Pair")
            st.dataframe(df_snap, use_container_width=True)
        else:
            st.info("Waiting for first update...")
        
    # Auto-refresh UI for visual feedback (optional, not strictly needed for logic anymore)
    # time.sleep(1)
    # st.rerun()

# --- PAGE: Monitor 1H ---
elif page == "Monitor 1H":
    st.title("BTC & ETH 1H Series Monitor 🕐")
    
    st.info(f"Monitoring: {core.btc_slug_1h} | {core.eth_slug_1h} | {core.sol_slug_1h} | {core.xrp_slug_1h}")
    
    if st.button("🔄 Refresh View"):
        core.force_process()
        st.rerun()
        
    st.subheader("Current Mins (1H)")
    # st.json(core.mins_1h)
    if core.mins_1h:
        data = []
        for k, v in core.mins_1h.items():
            row = {"Pair": k}
            row.update(v)
            data.append(row)
        df_mins = pd.DataFrame(data).set_index("Pair")
        st.dataframe(df_mins, use_container_width=True)
    else:
        st.info("No arbitrage opportunities yet.")

    st.subheader("Real-time Snapshot (1H) (No Filter)")
    if hasattr(core, 'latest_1h') and core.latest_1h:
        # st.json(core.latest_1h)
        data = []
        for k, v in core.latest_1h.items():
            row = {"Pair": k}
            row.update(v)
            data.append(row)
        df_snap = pd.DataFrame(data).set_index("Pair")
        st.dataframe(df_snap, use_container_width=True)
    else:
        st.info("Waiting for first update...")
    
    # time.sleep(1)
    # st.rerun()

# --- PAGE: Arb History ---
elif page == "Arb History":
    st.title("Arbitrage Opportunities History 📉")
    
    col_refresh, _ = st.columns([1, 5])
    with col_refresh:
        if st.button("🔄 Refresh Data"):
            st.rerun()

    with st.expander("Recording Settings", expanded=False):
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
        st.text_input(
            "App Public URL (Anti-Sleep)", 
            value=st.session_state.get("public_url", ""), 
            key="public_url_input",
            help="Enter your Streamlit Cloud App URL here. The monitor will ping it every minute to prevent sleep.",
            on_change=lambda: st.session_state.update({"public_url": st.session_state.public_url_input}) or save_settings()
        )

    # Filter Options
    filter_col1, _ = st.columns([2, 4])
    with filter_col1:
        time_filter = st.radio("Market Duration:", ["All", "15m", "1H"], horizontal=True)

    # --- Helper to load and normalize data ---
    def load_15m_data():
        if os.path.exists(ARB_OPPORTUNITY_FILE):
            try:
                df = pd.read_csv(ARB_OPPORTUNITY_FILE)
                if not df.empty and "timestamp" in df.columns:
                    df["Duration"] = "15m"
                    return df
            except Exception as e:
                st.error(f"Error loading 15m file: {e}")
        return pd.DataFrame()

    def load_1h_data():
        if os.path.exists(ARB_OPPORTUNITY_FILE_1H):
            try:
                df = pd.read_csv(ARB_OPPORTUNITY_FILE_1H)
                if not df.empty and "timestamp" in df.columns:
                    # Rename columns to match 15m data
                    df.rename(columns={"price_a": "ask_a", "price_b": "ask_b"}, inplace=True)
                    df["Duration"] = "1H"
                    return df
            except Exception as e:
                st.error(f"Error loading 1H file: {e}")
        return pd.DataFrame()

    # --- Data Loading Logic (Lazy Load) ---
    df_arb = pd.DataFrame()
    
    if time_filter == "15m":
        df_arb = load_15m_data()
    elif time_filter == "1H":
        df_arb = load_1h_data()
    else: # All
        df_15m = load_15m_data()
        df_1h = load_1h_data()
        df_arb = pd.concat([df_15m, df_1h], ignore_index=True)

    # --- Display Logic ---
    if not df_arb.empty:
        # Deduplicate
        if "Duration" in df_arb.columns:
            df_arb = df_arb.drop_duplicates(subset=["timestamp", "type", "sum", "Duration"])
        else:
            df_arb = df_arb.drop_duplicates(subset=["timestamp", "type", "sum"])
        
        # Filter for AAA types only (Opposite bets)
        df_arb = df_arb[df_arb["type"].isin([
            "BTC_Up_ETH_Down", "BTC_Down_ETH_Up", 
            "BTC_Up_SOL_Down", "BTC_Down_SOL_Up", 
            "BTC_Up_XRP_Down", "BTC_Down_XRP_Up", 
            "ETH_Up_SOL_Down", "ETH_Down_SOL_Up", 
            "ETH_Up_XRP_Down", "ETH_Down_XRP_Up", 
            "SOL_Up_XRP_Down", "SOL_Down_XRP_Up"
        ])]
        
        if not df_arb.empty:
            def safe_convert_time(ts):
                try:
                    return pd.to_datetime(float(ts), unit='s')
                except:
                    return pd.to_datetime(ts)

            df_arb["dt_obj"] = df_arb["timestamp"].apply(safe_convert_time)
            df_arb["Time"] = df_arb["dt_obj"].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_arb = df_arb.sort_values(by="dt_obj", ascending=False)

            cols = ["Time", "Duration", "type", "sum", "ask_a", "ask_b", "slug_a", "slug_b", "pair"]
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
            st.info("No data matches the current filter.")
    else:
        if time_filter == "All":
            st.info("No arbitrage history recorded yet.")
        else:
            st.info(f"No recorded data for {time_filter} duration.")

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
    
    with st.sidebar:
        st.divider()
        st.header("Multi-Asset Settings (1H)")
        
        if st.button("📥 加载/更新历史 (200条)"):
            existing_data = {}
            if os.path.exists(MULTI_ASSET_HISTORY_1H_FILE):
                try:
                    df_exist = pd.read_csv(MULTI_ASSET_HISTORY_1H_FILE)
                    if not df_exist.empty:
                        for _, row in df_exist.iterrows():
                            existing_data[row["timestamp"]] = row.to_dict()
                except: pass
            
            # Generate target timestamps (Top of hour)
            from datetime import timezone, timedelta
            now_utc = datetime.datetime.now(timezone.utc)
            current_hour_utc = now_utc.replace(minute=0, second=0, microsecond=0)
            
            target_timestamps = [int((current_hour_utc - timedelta(hours=i)).timestamp()) for i in range(200)]
            
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
                
                def generate_1h_slug(ts, asset_code):
                    dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
                    name_map = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "xrp"}
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

                def fetch_multi_round_1h(ts):
                    btc_s = generate_1h_slug(ts, "BTC")
                    eth_s = generate_1h_slug(ts, "ETH")
                    sol_s = generate_1h_slug(ts, "SOL")
                    xrp_s = generate_1h_slug(ts, "XRP")
                    
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
                        results = list(executor.map(fetch_multi_round_1h, batch))
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
                
                pd.DataFrame(final_list).to_csv(MULTI_ASSET_HISTORY_1H_FILE, index=False)
                st.success(f"Updated {len(new_records)} new records!")
                st.rerun()
            else:
                st.info("Data is already up to date.")

    df = pd.DataFrame()
    if os.path.exists(MULTI_ASSET_HISTORY_1H_FILE):
        try:
            df = pd.read_csv(MULTI_ASSET_HISTORY_1H_FILE)
        except: pass
        
    if not df.empty:
        c1, c2 = st.columns([1, 3])
        with c1:
            pair_1h = st.selectbox("选择交易对 (Select Pair) (1H)", [
                "BTC vs ETH", "BTC vs SOL", "BTC vs XRP",
                "ETH vs SOL", "ETH vs XRP",
                "SOL vs XRP"
            ])
            
        asset_a_name, asset_b_name = pair_1h.split(" vs ")
        col_a = f"{asset_a_name.lower()}_res"
        col_b = f"{asset_b_name.lower()}_res"
        
        valid_df = df[
            (~df[col_a].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"])) &
            (~df[col_b].isin(["Pending", "Pending (Closed)", "Active", "Error", "Not Found"]))
        ].copy()
        
        valid_df["is_same"] = valid_df[col_a] == valid_df[col_b]
        
        total = len(valid_df)
        same_count = valid_df["is_same"].sum()
        rate = (same_count / total * 100) if total > 0 else 0
        
        st.metric(f"同向占比 (Correlation Rate) - {pair_1h} (1H)", f"{rate:.1f}%", f"Samples: {total}")
        
        # Match with recorded arb prices
        best_prices = {}
        if os.path.exists(ARB_OPPORTUNITY_FILE_1H):
            try:
                df_arb_1h = pd.read_csv(ARB_OPPORTUNITY_FILE_1H)
                if not df_arb_1h.empty:
                    if "price_a" in df_arb_1h.columns:
                        df_arb_1h.rename(columns={"price_a": "ask_a", "price_b": "ask_b"}, inplace=True)

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
                            # 1H slug structure: ...-hour-et. The slug is unique per hour.
                            # Group by slug_a
                            min_series = df_aaa.groupby("slug_a")["sum"].min()
                            best_prices = min_series.to_dict()
            except:
                pass
                
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
        
        def get_aaa_price(row):
            # Try to reconstruct slug or match by timestamp if possible, 
            # but simpler to generate slug again for lookup or save slug in CSV.
            # We didn't save slug in fetch_multi_round_1h to keep it clean, but we can regen it.
            # Or better: check if we can match by timestamp?
            # ARB_OPPORTUNITY_FILE_1H stores 'slug_a'.
            # We can just generate slug_a for this row and check dict.
            ts = row["timestamp"]
            slug_a = generate_1h_slug(ts, asset_a_name)
            return f"{best_prices[slug_a]:.4f}" if slug_a in best_prices else "-"

        # Need generate_1h_slug available here.
        def generate_1h_slug(ts, asset_code):
            from datetime import timedelta
            dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
            name_map = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "xrp"}
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

        display_df["AAA Best Price"] = display_df.apply(get_aaa_price, axis=1)
        
        # Display Table
        cols = ["Time", col_a, col_b, "Relation", "AAA Best Price"]
        display_df = display_df[cols].sort_values(by="Time", ascending=False)
        display_df.columns = ["Time", f"{asset_a_name} Result", f"{asset_b_name} Result", "Relation", "AAA Best Price"]
        
        st.dataframe(display_df, use_container_width=True)
        
    else:
        st.info("Please click '加载/更新历史' to fetch data.")

else:
    st.error(f"⚠️ Page not found: {page}")
    st.write(f"Selected Page Value: '{page}'")