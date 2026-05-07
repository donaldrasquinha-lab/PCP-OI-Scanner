"""
PCP & OI Scanner — Production Backend
=======================================
Rate-limited, staggered-poll, batch-optimized Upstox API proxy.

  ┌──────────┐      ┌────────────────┐      ┌──────────────┐
  │ Browser  │◄────►│  Flask :5050   │◄────►│  Upstox API  │
  └──────────┘ JSON │                │ REST └──────────────┘
                    │ Background     │  5 symbols / 2s
                    │ Scanner Thread │  ≤50 calls/min
                    │ Token Bucket   │  Exponential backoff
                    │ TTL Cache      │  Batch LTP (500 keys)
                    └────────────────┘

Run:  pip install flask requests && python server.py
Open: http://localhost:5050
"""

import os, sys, json, time, math, threading, logging
from datetime import datetime, timedelta
from pathlib import Path
from collections import deque

try:
    from flask import Flask, request, jsonify, send_file
    import requests as http_req
except ImportError:
    print("\n  pip install flask requests\n"); sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-5s │ %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("pcp")
app = Flask(__name__)

# ═══ CONFIG ══════════════════════════════════════════════════════
UPSTOX_BASE = "https://api.upstox.com"
NIFTY_INDEX_KEY = "NSE_INDEX|Nifty 50"
MAX_CALLS_PER_MIN = 50
STAGGER_BATCH = 5        # symbols per batch
STAGGER_DELAY = 2.0      # seconds between batches
POLL_INTERVAL = 15        # seconds between full sweeps
LTP_CHUNK = 50            # max keys per /v3/market-quote/ltp
CACHE_TTL_CHAIN = 12
CACHE_TTL_LTP = 8
CACHE_TTL_EXPIRY = 300
BACKOFF_INIT = 5
BACKOFF_MAX = 60
BACKOFF_MULT = 2

NIFTY50 = {
    "RELIANCE":"NSE_EQ|INE002A01018","TCS":"NSE_EQ|INE467B01029","HDFCBANK":"NSE_EQ|INE040A01034",
    "INFY":"NSE_EQ|INE009A01021","ICICIBANK":"NSE_EQ|INE090A01021","HINDUNILVR":"NSE_EQ|INE030A01027",
    "ITC":"NSE_EQ|INE154A01025","SBIN":"NSE_EQ|INE062A01020","BHARTIARTL":"NSE_EQ|INE397D01024",
    "KOTAKBANK":"NSE_EQ|INE237A01028","LT":"NSE_EQ|INE018A01030","AXISBANK":"NSE_EQ|INE238A01034",
    "BAJFINANCE":"NSE_EQ|INE296A01024","ASIANPAINT":"NSE_EQ|INE021A01026","MARUTI":"NSE_EQ|INE585B01010",
    "TITAN":"NSE_EQ|INE280A01028","SUNPHARMA":"NSE_EQ|INE044A01036","ULTRACEMCO":"NSE_EQ|INE481G01011",
    "WIPRO":"NSE_EQ|INE075A01022","NESTLEIND":"NSE_EQ|INE239A01016","HCLTECH":"NSE_EQ|INE860A01027",
    "TATAMOTORS":"NSE_EQ|INE155A01022","NTPC":"NSE_EQ|INE733E01010","POWERGRID":"NSE_EQ|INE752E01010",
    "M&M":"NSE_EQ|INE101A01026","TATASTEEL":"NSE_EQ|INE081A01020","ONGC":"NSE_EQ|INE213A01029",
    "JSWSTEEL":"NSE_EQ|INE019A01038","ADANIPORTS":"NSE_EQ|INE742F01042","COALINDIA":"NSE_EQ|INE522F01014",
    "BAJAJFINSV":"NSE_EQ|INE918I01018","GRASIM":"NSE_EQ|INE047A01021","TECHM":"NSE_EQ|INE669C01036",
    "DRREDDY":"NSE_EQ|INE089A01023","HINDALCO":"NSE_EQ|INE038A01020","CIPLA":"NSE_EQ|INE059A01026",
    "BPCL":"NSE_EQ|INE029A01011","DIVISLAB":"NSE_EQ|INE361B01024","APOLLOHOSP":"NSE_EQ|INE437A01024",
    "EICHERMOT":"NSE_EQ|INE066A01021","TATACONSUM":"NSE_EQ|INE192A01025","SBILIFE":"NSE_EQ|INE123W01016",
    "BRITANNIA":"NSE_EQ|INE216A01030","INDUSINDBK":"NSE_EQ|INE095A01012","HEROMOTOCO":"NSE_EQ|INE158A01026",
    "HDFCLIFE":"NSE_EQ|INE795G01014","BAJAJ-AUTO":"NSE_EQ|INE917I01010","UPL":"NSE_EQ|INE628A01036",
    "LTIM":"NSE_EQ|INE214T01019","SHRIRAMFIN":"NSE_EQ|INE721A01013",
}

# ═══ RATE LIMITER — Token Bucket (50/min sliding window) ════════
class RateLimiter:
    def __init__(self, mx=MAX_CALLS_PER_MIN):
        self.mx = mx; self.ts = deque(); self.lock = threading.Lock()
        self.total = 0; self.throttled = 0

    def acquire(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                now = time.time()
                while self.ts and self.ts[0] < now - 60: self.ts.popleft()
                if len(self.ts) < self.mx:
                    self.ts.append(now); self.total += 1; return True
                wait = self.ts[0] + 60 - now
            self.throttled += 1
            time.sleep(min(wait + 0.1, 2.0))
        return False

    @property
    def window(self):
        with self.lock:
            now = time.time()
            while self.ts and self.ts[0] < now - 60: self.ts.popleft()
            return len(self.ts)

    @property
    def stats(self):
        return {"calls_in_window": self.window, "max": self.mx, "total": self.total, "throttled": self.throttled}

rl = RateLimiter()

# ═══ CACHE — TTL per key ════════════════════════════════════════
class Cache:
    def __init__(self):
        self.store = {}; self.lock = threading.Lock(); self.hits = 0; self.misses = 0

    def get(self, key):
        with self.lock:
            if key in self.store:
                ts, ttl, data = self.store[key]
                if time.time() - ts < ttl: self.hits += 1; return data
                del self.store[key]
            self.misses += 1; return None

    def set(self, key, data, ttl):
        with self.lock: self.store[key] = (time.time(), ttl, data)

    def invalidate(self, pfx=""):
        with self.lock:
            for k in [k for k in self.store if k.startswith(pfx)]: del self.store[k]

    @property
    def stats(self):
        with self.lock:
            v = sum(1 for _, (ts, ttl, _) in self.store.items() if time.time() - ts < ttl)
        return {"valid": v, "total": len(self.store), "hits": self.hits, "misses": self.misses,
                "rate": f"{self.hits/max(self.hits+self.misses,1)*100:.0f}%"}

cache = Cache()

# ═══ EXPONENTIAL BACKOFF ════════════════════════════════════════
class Backoff:
    def __init__(self):
        self.f = {}; self.lock = threading.Lock()

    def skip(self, ep):
        with self.lock:
            if ep not in self.f: return False
            _, lt, bo = self.f[ep]
            return time.time() - lt < bo

    def fail(self, ep):
        with self.lock:
            if ep in self.f:
                n, _, prev = self.f[ep]
                self.f[ep] = (n+1, time.time(), min(prev*BACKOFF_MULT, BACKOFF_MAX))
            else:
                self.f[ep] = (1, time.time(), BACKOFF_INIT)
            log.warning(f"Backoff {ep}: {self.f[ep][2]}s (#{self.f[ep][0]})")

    def ok(self, ep):
        with self.lock:
            if ep in self.f: del self.f[ep]

    @property
    def active(self):
        with self.lock:
            now = time.time()
            return {k: {"fails": v[0], "backoff": v[2], "resume_in": max(0, round(v[1]+v[2]-now,1))}
                    for k, v in self.f.items() if now - v[1] < v[2]}

bo = Backoff()

# ═══ API CALLER ═════════════════════════════════════════════════
def upstox_get(url, token, ttl=CACHE_TTL_CHAIN, label=""):
    ck = f"{url}|{token[:8]}"
    c = cache.get(ck)
    if c is not None: return c, True

    ep = url.split("?")[0].replace(UPSTOX_BASE, "")
    if bo.skip(ep): raise Exception(f"Backing off {ep}")
    if not rl.acquire(15): raise Exception("Rate limit: 50/min exceeded")

    hdrs = {"Content-Type":"application/json","Accept":"application/json","Authorization":f"Bearer {token}"}
    try:
        r = http_req.get(url, headers=hdrs, timeout=12)
        if r.status_code == 429:
            bo.fail(ep); raise Exception(f"429 rate limited")
        if r.status_code >= 500:
            bo.fail(ep); raise Exception(f"Server {r.status_code}")
        r.raise_for_status()
        data = r.json(); bo.ok(ep); cache.set(ck, data, ttl)
        log.info(f"OK │ {label or ep} │ {rl.window}/{MAX_CALLS_PER_MIN}/min")
        return data, False
    except http_req.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code in (401,403):
            raise Exception(f"Auth {e.response.status_code}")
        bo.fail(ep); raise
    except http_req.exceptions.Timeout:
        bo.fail(ep); raise Exception(f"Timeout {ep}")
    except http_req.exceptions.ConnectionError:
        bo.fail(ep); raise Exception(f"Connection error {ep}")

# ═══ BATCH LTP (v3 — up to 500 keys per call) ══════════════════
def batch_ltp(keys, token):
    results = {}
    for i in range(0, len(keys), LTP_CHUNK):
        chunk = keys[i:i+LTP_CHUNK]
        url = f"{UPSTOX_BASE}/v3/market-quote/ltp?instrument_key={','.join(chunk)}"
        try:
            data, _ = upstox_get(url, token, CACHE_TTL_LTP, f"ltp_batch[{len(chunk)}]")
            if data.get("status") == "success" and data.get("data"):
                for k, v in data["data"].items():
                    results[k] = v.get("ltp", 0) if isinstance(v, dict) else v
        except Exception as e:
            log.warning(f"Batch LTP err: {e}")
    return results

# ═══ HELPERS ════════════════════════════════════════════════════
def pcp(c, p, s, k, d, r=.065):
    T = max(d,1)/365; v = (c-p)-(s-k*math.exp(-r*T))
    return {"violation": round(v,4), "pct": round(v/s*100,4) if s else 0}

def dte(exp):
    try: return max(0, (datetime.strptime(exp,"%Y-%m-%d").replace(hour=15,minute=30)-datetime.now()).days+1)
    except: return 7

def next_expiries():
    today = datetime.now(); exps = []
    d = today
    while d.weekday() != 3: d += timedelta(days=1)
    if today.weekday() == 3 and today.hour >= 15 and today.minute >= 30: d += timedelta(days=7)
    for _ in range(6): exps.append(d.strftime("%Y-%m-%d")); d += timedelta(days=7)
    m, y = today.month, today.year
    for _ in range(3):
        ld = datetime(y, m+1, 1)-timedelta(days=1) if m < 12 else datetime(y+1,1,1)-timedelta(days=1)
        while ld.weekday() != 3: ld -= timedelta(days=1)
        s = ld.strftime("%Y-%m-%d")
        if s not in exps and ld > today: exps.append(s)
        m += 1
        if m > 12: m = 1; y += 1
    return sorted(set(exps))

def proc_chain(sym, rows, expiry):
    if not rows: return None
    spot = rows[0].get("underlying_spot_price", 0)
    atm = min(rows, key=lambda r: abs(r["strike_price"]-spot))["strike_price"]
    d = dte(expiry); chain = []
    for r in rows:
        K = r["strike_price"]
        cm = (r.get("call_options") or {}).get("market_data") or {}
        pm = (r.get("put_options") or {}).get("market_data") or {}
        cg = (r.get("call_options") or {}).get("option_greeks") or {}
        pg = (r.get("put_options") or {}).get("option_greeks") or {}
        cl, pl = cm.get("ltp",0) or 0, pm.get("ltp",0) or 0
        co, po = cm.get("oi",0) or 0, pm.get("oi",0) or 0
        p = pcp(cl, pl, spot, K, d)
        chain.append({"strike":K,"spot":spot,"callLTP":cl,"putLTP":pl,"callOI":co,"putOI":po,
            "callOIChg":co-(cm.get("prev_oi",0)or 0),"putOIChg":po-(pm.get("prev_oi",0)or 0),
            "callVol":cm.get("volume",0)or 0,"putVol":pm.get("volume",0)or 0,
            "pcpViolation":p["violation"],"pcpPct":p["pct"],
            "callIV":cg.get("iv",0)or 0,"putIV":pg.get("iv",0)or 0,
            "callDelta":cg.get("delta",0)or 0,"putDelta":pg.get("delta",0)or 0,
            "pcr":round(po/max(co,1),4)})
    tc = sum(c["callOI"] for c in chain); tp = sum(c["putOI"] for c in chain)
    tcc = sum(c["callOIChg"] for c in chain); tpc = sum(c["putOIChg"] for c in chain)
    mv = max(chain, key=lambda c: abs(c["pcpPct"])) if chain else chain[0]
    pcr = round(tp/max(tc,1),4); ar = next((c for c in chain if c["strike"]==atm), chain[len(chain)//2])
    sig = "NEUTRAL"
    if pcr > 1.3 and tpc > 5000: sig = "BULLISH"
    elif pcr < 0.7 and tcc > 5000: sig = "BEARISH"
    elif abs(mv["pcpPct"]) > 0.5: sig = "ARBITRAGE"
    return {"symbol":sym,"spot":spot,"atmStrike":atm,"iv":ar.get("callIV",0),"pcr":pcr,
        "totalCallOI":tc,"totalPutOI":tp,"totalCallOIChg":tcc,"totalPutOIChg":tpc,
        "maxPCPViolation":mv["pcpViolation"],"maxPCPPct":mv["pcpPct"],
        "violationStrike":mv["strike"],"signal":sig,"chain":chain,"expiry":expiry}

def proc_nifty_oi(rows, expiry):
    spot = rows[0].get("underlying_spot_price",0) if rows else 0; chain = []
    for r in rows:
        cm = (r.get("call_options")or{}).get("market_data")or{}
        pm = (r.get("put_options")or{}).get("market_data")or{}
        cg = (r.get("call_options")or{}).get("option_greeks")or{}
        pg = (r.get("put_options")or{}).get("option_greeks")or{}
        co, po = cm.get("oi",0)or 0, pm.get("oi",0)or 0
        chain.append({"strike":r["strike_price"],"callOI":co,"putOI":po,
            "callOIChg":co-(cm.get("prev_oi",0)or 0),"putOIChg":po-(pm.get("prev_oi",0)or 0),
            "callVol":cm.get("volume",0)or 0,"putVol":pm.get("volume",0)or 0,
            "callLTP":cm.get("ltp",0)or 0,"putLTP":pm.get("ltp",0)or 0,
            "callIV":cg.get("iv",0)or 0,"putIV":pg.get("iv",0)or 0,
            "pcr":round(po/max(co,1),4)})
    tc = sum(c["callOI"] for c in chain); tp = sum(c["putOI"] for c in chain)
    mc = max(chain, key=lambda c:c["callOI"]) if chain else {}
    mp = max(chain, key=lambda c:c["putOI"]) if chain else {}
    return {"spot":spot,"chain":chain,"totalCallOI":tc,"totalPutOI":tp,
        "pcr":round(tp/max(tc,1),4),"maxCallOIStrike":mc,"maxPutOIStrike":mp,"expiry":expiry}

# ═══ BACKGROUND SCANNER (Staggered) ════════════════════════════
class Scanner:
    """5 symbols / 2s → full sweep ~20s → ≤50 calls/min steady."""
    def __init__(self):
        self.lock = threading.Lock()
        self.token = None; self.expiry = None; self.syms = []
        self.running = False; self.thread = None
        self.stocks = {}; self.noi = None
        self.last_sweep = None; self.errs = []
        self.progress = {"done":0,"total":0,"phase":"idle"}

    def start(self, token, expiry, syms):
        with self.lock:
            self.token = token; self.expiry = expiry; self.syms = syms
            if self.running: return
            self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        log.info(f"Scanner ON: {len(syms)} syms, exp={expiry}, batch={STAGGER_BATCH}/{STAGGER_DELAY}s")

    def stop(self):
        with self.lock: self.running = False
        log.info("Scanner OFF")

    def config(self, expiry=None, syms=None):
        with self.lock:
            if expiry: self.expiry = expiry; cache.invalidate()
            if syms is not None: self.syms = syms

    @property
    def snapshot(self):
        with self.lock:
            return {"stocks":list(self.stocks.values()),"niftyOI":self.noi,
                "lastSweep":self.last_sweep.isoformat() if self.last_sweep else None,
                "progress":dict(self.progress),"errors":list(self.errs),
                "rateLimit":rl.stats,"cache":cache.stats,"backoffs":bo.active}

    def _loop(self):
        while True:
            with self.lock:
                if not self.running: break
                tok, exp, syms = self.token, self.expiry, list(self.syms)
            try: self._sweep(tok, exp, syms)
            except Exception as e: log.error(f"Sweep: {e}")
            for _ in range(int(POLL_INTERVAL*10)):
                time.sleep(0.1)
                with self.lock:
                    if not self.running: return

    def _sweep(self, tok, exp, syms):
        errs = []; total = len(syms)+1; done = 0
        with self.lock: self.progress = {"done":0,"total":total,"phase":"nifty_oi"}

        # 1. Nifty Index OI
        try:
            url = f"{UPSTOX_BASE}/v2/option/chain?instrument_key={NIFTY_INDEX_KEY}&expiry_date={exp}"
            data, _ = upstox_get(url, tok, CACHE_TTL_CHAIN, "NIFTY_OI")
            if data.get("status")=="success" and data.get("data"):
                with self.lock: self.noi = proc_nifty_oi(data["data"], exp)
        except Exception as e: errs.append(f"NIFTY_OI: {e}")
        done += 1

        # 2. Batch LTP (1 call per 50 symbols — saves ~49 calls)
        with self.lock: self.progress = {"done":done,"total":total,"phase":"ltp"}
        try:
            keys = [NIFTY50[s] for s in syms if s in NIFTY50]
            ltp_map = batch_ltp(keys, tok)
            log.info(f"Batch LTP: {len(ltp_map)} prices")
        except: ltp_map = {}

        # 3. Staggered chain fetch
        with self.lock: self.progress["phase"] = "chains"
        for i in range(0, len(syms), STAGGER_BATCH):
            batch = syms[i:i+STAGGER_BATCH]
            for sym in batch:
                ik = NIFTY50.get(sym)
                if not ik: errs.append(f"{sym}: no key"); done += 1; continue
                try:
                    url = f"{UPSTOX_BASE}/v2/option/chain?instrument_key={ik}&expiry_date={exp}"
                    data, fc = upstox_get(url, tok, CACHE_TTL_CHAIN, sym)
                    if data.get("status")=="success" and data.get("data"):
                        p = proc_chain(sym, data["data"], exp)
                        if p:
                            with self.lock: self.stocks[sym] = p
                    else: errs.append(f"{sym}: empty")
                except Exception as e: errs.append(f"{sym}: {e}")
                done += 1
                with self.lock: self.progress = {"done":done,"total":total,"phase":"chains"}
            # Stagger
            if i + STAGGER_BATCH < len(syms):
                log.info(f"Stagger: {done}/{total} │ {rl.window}/{MAX_CALLS_PER_MIN}/min │ pause {STAGGER_DELAY}s")
                time.sleep(STAGGER_DELAY)

        with self.lock:
            self.last_sweep = datetime.now(); self.errs = errs
            self.progress = {"done":total,"total":total,"phase":"done"}
        log.info(f"Sweep done: {total-len(errs)}/{total} OK │ cache {cache.stats['rate']} │ {rl.window}/{MAX_CALLS_PER_MIN}/min")

scanner = Scanner()

# ═══ ROUTES ═════════════════════════════════════════════════════
@app.route("/")
def index():
    p = Path(__file__).parent / "dashboard.html"
    return send_file(p) if p.exists() else ("<h1>Put dashboard.html next to server.py</h1>", 404)

@app.route("/api/expiries")
def api_expiries():
    tok = request.headers.get("Authorization","").replace("Bearer ","")
    comp = next_expiries()
    if tok:
        try:
            data, _ = upstox_get(f"{UPSTOX_BASE}/v2/option/contract?instrument_key={NIFTY_INDEX_KEY}", tok, CACHE_TTL_EXPIRY, "expiries")
            if data.get("status")=="success" and data.get("data"):
                return jsonify({"status":"success","source":"live","expiries":sorted(set(r["expiry"] for r in data["data"]))})
        except: pass
    return jsonify({"status":"success","source":"computed","expiries":comp})

@app.route("/api/start", methods=["POST"])
def api_start():
    b = request.get_json(force=True)
    tok, exp = b.get("token",""), b.get("expiry", next_expiries()[0])
    syms = b.get("symbols", list(NIFTY50.keys())[:10])
    if not tok: return jsonify({"status":"error","message":"No token"}), 401
    scanner.start(tok, exp, syms)
    return jsonify({"status":"success","config":{"symbols":len(syms),"expiry":exp,
        "batch":STAGGER_BATCH,"delay_s":STAGGER_DELAY,"poll_s":POLL_INTERVAL}})

@app.route("/api/stop", methods=["POST"])
def api_stop():
    scanner.stop(); return jsonify({"status":"success"})

@app.route("/api/config", methods=["POST"])
def api_config():
    b = request.get_json(force=True)
    scanner.config(b.get("expiry"), b.get("symbols"))
    return jsonify({"status":"success"})

@app.route("/api/snapshot")
def api_snapshot():
    return jsonify({"status":"success", **scanner.snapshot})

@app.route("/api/nifty-oi")
def api_nifty_oi():
    tok = request.headers.get("Authorization","").replace("Bearer ","")
    exp = request.args.get("expiry", next_expiries()[0])
    with scanner.lock:
        if scanner.noi: return jsonify({"status":"success","data":scanner.noi,"source":"scanner"})
    if not tok: return jsonify({"status":"error","message":"No token"}), 401
    try:
        url = f"{UPSTOX_BASE}/v2/option/chain?instrument_key={NIFTY_INDEX_KEY}&expiry_date={exp}"
        data, _ = upstox_get(url, tok, CACHE_TTL_CHAIN, "nifty_oi")
        if data.get("status")=="success" and data.get("data"):
            return jsonify({"status":"success","data":proc_nifty_oi(data["data"], exp)})
        return jsonify({"status":"error","message":"No data"}), 404
    except Exception as e: return jsonify({"status":"error","message":str(e)}), 500

@app.route("/api/scan")
def api_scan():
    tok = request.headers.get("Authorization","").replace("Bearer ","")
    exp = request.args.get("expiry", next_expiries()[0])
    sp = request.args.get("symbols","")
    syms = [s.strip() for s in sp.split(",") if s.strip()] if sp else list(NIFTY50.keys())[:10]
    if not tok: return jsonify({"status":"error","message":"No token"}), 401
    results, errs = [], []
    for i in range(0, len(syms), STAGGER_BATCH):
        batch = syms[i:i+STAGGER_BATCH]
        for sym in batch:
            ik = NIFTY50.get(sym)
            if not ik: errs.append(f"{sym}: no key"); continue
            try:
                url = f"{UPSTOX_BASE}/v2/option/chain?instrument_key={ik}&expiry_date={exp}"
                data, _ = upstox_get(url, tok, CACHE_TTL_CHAIN, sym)
                if data.get("status")=="success" and data.get("data"):
                    p = proc_chain(sym, data["data"], exp); 
                    if p: results.append(p)
                else: errs.append(f"{sym}: no data")
            except Exception as e: errs.append(f"{sym}: {e}")
        if i + STAGGER_BATCH < len(syms): time.sleep(STAGGER_DELAY)
    return jsonify({"status":"success","data":results,"errors":errs,"rateLimit":rl.stats})

@app.route("/api/chain")
def api_chain():
    tok = request.headers.get("Authorization","").replace("Bearer ","")
    sym = request.args.get("symbol",""); exp = request.args.get("expiry", next_expiries()[0])
    with scanner.lock:
        if sym in scanner.stocks: return jsonify({"status":"success","data":scanner.stocks[sym],"source":"scanner"})
    if not tok: return jsonify({"status":"error","message":"No token"}), 401
    ik = NIFTY50.get(sym)
    if not ik: return jsonify({"status":"error","message":f"Unknown: {sym}"}), 400
    try:
        url = f"{UPSTOX_BASE}/v2/option/chain?instrument_key={ik}&expiry_date={exp}"
        data, _ = upstox_get(url, tok, CACHE_TTL_CHAIN, sym)
        if data.get("status")=="success" and data.get("data"):
            return jsonify({"status":"success","data":proc_chain(sym, data["data"], exp)})
        return jsonify({"status":"error","message":"No data"}), 404
    except Exception as e: return jsonify({"status":"error","message":str(e)}), 500

@app.route("/api/symbols")
def api_symbols(): return jsonify({"status":"success","data":NIFTY50})

@app.route("/api/health")
def api_health():
    return jsonify({"status":"ok","scanner":scanner.progress.get("phase","idle"),
        "rate":rl.stats,"cache":cache.stats,"backoffs":bo.active})

# ═══ MAIN ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  PCP & OI Scanner — Production Backend                      ║
║  http://localhost:{port}                                       ║
║                                                              ║
║  Rate Control:                                               ║
║    Token bucket  : {MAX_CALLS_PER_MIN} calls / 60s sliding window           ║
║    Stagger       : {STAGGER_BATCH} symbols / {STAGGER_DELAY}s → sweep ~{(len(NIFTY50)//STAGGER_BATCH)*STAGGER_DELAY:.0f}s        ║
║    Poll interval : {POLL_INTERVAL}s between sweeps                       ║
║    Backoff       : {BACKOFF_INIT}s → {BACKOFF_MAX}s exponential on errors         ║
║    Batch LTP     : {LTP_CHUNK} keys/call (v3 API)                      ║
║    Cache         : {CACHE_TTL_CHAIN}s chains, {CACHE_TTL_LTP}s LTP, {CACHE_TTL_EXPIRY}s expiries          ║
║                                                              ║
║  Flow: POST /api/start → GET /api/snapshot (poll)            ║
║  Health: GET /api/health                                     ║
╚══════════════════════════════════════════════════════════════╝
""")
    app.run(host="0.0.0.0", port=port, debug=False)
