import pandas as pd
import numpy as np
from pybit.unified_trading import HTTP
import os
import time

# ============================================================
# CONFIG
# ============================================================
API_KEY    = os.environ.get('API_KEY', '')
API_SECRET = os.environ.get('API_SECRET', '')
CATEGORY   = "linear"
TESTNET    = os.environ.get('TESTNET', 'false').lower() == 'true'

if not API_KEY or not API_SECRET:
    raise ValueError("❌ API_KEY dan API_SECRET belum diset!")

session = HTTP(testnet=TESTNET, api_key=API_KEY, api_secret=API_SECRET)

SYMBOLS = [
    'XVGUSDT', 'BELUSDT', 'TAOUSDT', '1000BONKUSDT', 'BERAUSDT',
    'APTUSDT', 'DASHUSDT', 'DOGEUSDT', 'USUALUSDT',
    'FARTCOINUSDT', '1000PEPEUSDT',
]

# ============================================================
# STATE PER COIN
# ============================================================
# pending[coin] = {
#   'type'         : "Long" | "Short"   -- arah BOS H1
#   'df_h1'        : DataFrame          -- snapshot H1 saat BOS (freeze)
#   'fvg_list'     : list               -- semua FVG internal BOS H1
#   'fvg_idx'      : int                -- FVG aktif yang sedang dipantau
#   'fvg_touch_ts' : int                -- ts candle H1 pertama kali wick FVG
#   'tp'           : float              -- target profit
#   'bos_ts'       : int                -- timestamp BOS H1
#   'phase'        : str                -- phase aktif
#
#   Phase urutan:
#   "WAIT_FVG_TOUCH"  → tunggu FVG H1 diwick
#   "WAIT_IDM_TOUCH"  → tunggu high/low IDM M5 disentuh harga
#   "WAIT_BOS_BREAK"  → IDM tersentuh, freeze M5, tunggu break struktur
#   "WAIT_MSS"        → BOS M5 terbentuk, IDM disentuh lagi, tunggu MSS
#
#   'm5_freeze_low'  : float  -- low snapshot M5 saat IDM tersentuh (untuk cek BOS break)
#   'm5_freeze_high' : float  -- high snapshot M5 saat IDM tersentuh
#   'm5_freeze_ts'   : int    -- ts saat M5 di-freeze
#   'idm_list'       : list   -- IDM yang ditemukan di M5
#   'idm_touched_val': float  -- nilai IDM yang terakhir disentuh
# }

pending          = {}
active_positions = {}


# ============================================================
# FUNGSI DATA
# ============================================================

def get_data(symbol, interval, limit=200):
    try:
        res = session.get_kline(
            category=CATEGORY, symbol=symbol,
            interval=interval, limit=limit
        )
        if res['retCode'] == 0:
            df = pd.DataFrame(
                res['result']['list'],
                columns=['ts','open','high','low','close','vol','turnover']
            )
            df[['open','high','low','close','ts']] = \
                df[['open','high','low','close','ts']].apply(pd.to_numeric)
            return df.iloc[::-1].reset_index(drop=True)
        print(f"⚠️ get_data {symbol} {interval}: {res.get('retMsg','')}")
        return None
    except Exception as e:
        print(f"⚠️ get_data {symbol} {interval}: {e}")
        return None


# ============================================================
# FUNGSI SWING
# ============================================================

def find_swings(df, left=2, right=2):
    highs, lows = [], []
    for i in range(left, len(df) - right):
        h, l = df['high'].iloc[i], df['low'].iloc[i]
        if all(df['high'].iloc[i-j] < h for j in range(1, left+1)) and \
           all(df['high'].iloc[i+j] <= h for j in range(1, right+1)):
            highs.append({'val': h, 'idx': i, 'ts': df['ts'].iloc[i]})
        if all(df['low'].iloc[i-j] > l for j in range(1, left+1)) and \
           all(df['low'].iloc[i+j] >= l for j in range(1, right+1)):
            lows.append({'val': l, 'idx': i, 'ts': df['ts'].iloc[i]})
    return highs, lows


# ============================================================
# FUNGSI IDM
# ============================================================

def find_idm(df, stype):
    """
    Cari semua IDM (BOS single move) di M5.

    IDM = pola A → [B..I konsolidasi] → J:
      A        = candle yang bikin ekstrem (high untuk bearish, low untuk bullish)
      B..I     = berapapun candle yang TIDAK melewati ekstrem A
      J        = candle pertama yang melewati ekstrem A (IDM diambil)

    H1 Bullish (Long) → cari IDM BEARISH:
      A bikin high, konsolidasi tidak tembus high A, J tembus high A ke atas
      → high A = level IDM, low A = swing low IDM

    H1 Bearish (Short) → cari IDM BULLISH:
      A bikin low, konsolidasi tidak tembus low A, J tembus low A ke bawah
      → low A = level IDM, high A = swing high IDM
    """
    idm_list = []

    if stype == "Long":
        # IDM bearish: A bikin high → konsolidasi → J tembus high A ke atas
        for i in range(0, len(df) - 2):
            high_a = df['high'].iloc[i]
            low_a  = df['low'].iloc[i]
            has_consolidation = False
            for j in range(i + 1, len(df)):
                if df['high'].iloc[j] <= high_a:
                    has_consolidation = True
                else:
                    # J melewati high A
                    if has_consolidation:
                        idm_list.append({
                            'high'     : high_a,   # level IDM yang disentuh
                            'low'      : low_a,    # swing low IDM (untuk cek BOS break)
                            'idx'      : i,
                            'swept_idx': j,
                            'ts'       : df['ts'].iloc[i]
                        })
                    break

    else:  # Short
        # IDM bullish: A bikin low → konsolidasi → J tembus low A ke bawah
        for i in range(0, len(df) - 2):
            low_a  = df['low'].iloc[i]
            high_a = df['high'].iloc[i]
            has_consolidation = False
            for j in range(i + 1, len(df)):
                if df['low'].iloc[j] >= low_a:
                    has_consolidation = True
                else:
                    # J melewati low A
                    if has_consolidation:
                        idm_list.append({
                            'low'      : low_a,    # level IDM yang disentuh
                            'high'     : high_a,   # swing high IDM (untuk cek BOS break)
                            'idx'      : i,
                            'swept_idx': j,
                            'ts'       : df['ts'].iloc[i]
                        })
                    break

    return idm_list


def find_latest_idm_touched(df, idm_list, stype, after_ts=0):
    """
    Dari list IDM, cari IDM terbaru yang sudah disentuh harga
    (high-nya disentuh untuk Long, low-nya disentuh untuk Short)
    setelah timestamp tertentu.
    """
    touched = None
    for idm in idm_list:
        if idm['ts'] <= after_ts:
            continue
        if stype == "Long":
            # IDM bearish: tunggu harga naik sentuh high IDM
            idm_idx = idm['idx']
            for j in range(idm['swept_idx'], len(df)):
                if df['high'].iloc[j] >= idm['high']:
                    touched = idm
                    touched['touch_idx'] = j
                    touched['touch_ts']  = df['ts'].iloc[j]
                    break
        else:
            # IDM bullish: tunggu harga turun sentuh low IDM
            idm_idx = idm['idx']
            for j in range(idm['swept_idx'], len(df)):
                if df['low'].iloc[j] <= idm['low']:
                    touched = idm
                    touched['touch_idx'] = j
                    touched['touch_ts']  = df['ts'].iloc[j]
                    break
    return touched


# ============================================================
# FUNGSI FVG H1
# ============================================================

def get_internal_gaps(df, stype, start_idx):
    """Cari semua FVG fresh dalam range internal BOS H1."""
    gaps    = []
    end_idx = len(df) - 2
    for i in range(end_idx, start_idx + 2, -1):
        gap = None
        if stype == "Long" and df['high'].iloc[i-2] < df['low'].iloc[i]:
            gap = {"top": df['low'].iloc[i], "bottom": df['high'].iloc[i-2]}
        elif stype == "Short" and df['low'].iloc[i-2] > df['high'].iloc[i]:
            gap = {"top": df['low'].iloc[i-2], "bottom": df['high'].iloc[i]}
        if gap:
            is_fresh = True
            for j in range(i + 1, len(df)):
                if (stype == "Long"  and df['close'].iloc[j] < gap['top']) or \
                   (stype == "Short" and df['close'].iloc[j] > gap['bottom']):
                    is_fresh = False; break
            if is_fresh:
                gaps.append(gap)
    return gaps


def price_in_fvg(price_high, price_low, fvg):
    return price_low <= fvg['top'] and price_high >= fvg['bottom']


def body_breaks_fvg(candle, fvg, stype):
    body_top    = max(candle['open'], candle['close'])
    body_bottom = min(candle['open'], candle['close'])
    if stype == "Long":
        # Body masuk ke bawah bottom FVG
        return body_bottom < fvg['bottom'] and body_top > fvg['bottom']
    else:
        # Body masuk ke atas top FVG
        return body_top > fvg['top'] and body_bottom < fvg['top']


def wick_only_touch(candle, fvg, stype):
    body_top    = max(candle['open'], candle['close'])
    body_bottom = min(candle['open'], candle['close'])
    if stype == "Long":
        # Wick bawah masuk zona, tapi body close di atas bottom FVG
        return candle['low'] <= fvg['bottom'] and body_bottom >= fvg['bottom']
    else:
        # Wick atas masuk zona, tapi body close di bawah top FVG
        return candle['high'] >= fvg['top'] and body_top <= fvg['top']


# ============================================================
# FUNGSI ORDER
# ============================================================

def place_limit_order(symbol, side, entry, sl, tp):
    try:
        res_bal  = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        balance  = float(res_bal['result']['list'][0]['totalEquity'])
        risk_usd = balance * 0.01
        dist     = abs(entry - sl)
        if dist == 0:
            print(f"⚠️ {symbol}: dist entry-SL = 0, skip.")
            return False
        qty = round(risk_usd / dist, 2)
        print(f"   Balance:{balance:.2f} Risk:{risk_usd:.2f} Dist:{dist} Qty:{qty}")
        res = session.place_order(
            category=CATEGORY, symbol=symbol, side=side,
            orderType="Limit", qty=str(qty), price=str(entry),
            stopLoss=str(sl), takeProfit=str(tp),
            timeInForce="GTC"
        )
        if res['retCode'] == 0:
            return True
        print(f"⚠️ {symbol}: Order ditolak → {res.get('retMsg','')} (code:{res['retCode']})")
        return False
    except Exception as e:
        print(f"⚠️ {symbol}: place_order error → {e}")
        return False


def get_open_position(symbol):
    try:
        res = session.get_positions(category=CATEGORY, symbol=symbol)
        if res['retCode'] == 0:
            for pos in res['result']['list']:
                if float(pos['size']) > 0:
                    return pos
        return None
    except:
        return None


def move_sl(symbol, new_sl):
    try:
        res = session.set_trading_stop(
            category=CATEGORY, symbol=symbol,
            stopLoss=str(new_sl), positionIdx=0
        )
        return res['retCode'] == 0
    except:
        return False


# ============================================================
# TRAILING SL — pindah ke +1% saat untung 2%
# ============================================================

def check_trailing_sl(coin):
    if coin not in active_positions: return
    p = active_positions[coin]
    if p.get('sl_moved'): return
    pos = get_open_position(coin)
    if pos is None:
        print(f"📭 {coin}: Posisi tutup.")
        del active_positions[coin]
        return
    entry = p['entry']
    side  = p['side']
    try:
        curr = float(pos['markPrice'])
    except:
        return
    if side == "Buy":
        pnl_pct = (curr - entry) / entry * 100
        if pnl_pct >= 2.0:
            new_sl = round(entry * 1.01, 8)
            if move_sl(coin, new_sl):
                active_positions[coin]['sl_moved'] = True
                print(f"🔒 {coin} LONG +{pnl_pct:.2f}% → SL ke +1% ({new_sl})")
    elif side == "Sell":
        pnl_pct = (entry - curr) / entry * 100
        if pnl_pct >= 2.0:
            new_sl = round(entry * 0.99, 8)
            if move_sl(coin, new_sl):
                active_positions[coin]['sl_moved'] = True
                print(f"🔒 {coin} SHORT +{pnl_pct:.2f}% → SL ke -1% ({new_sl})")


# ============================================================
# CEK TREN H1 BERUBAH
# ============================================================

def h1_trend_broken(curr_h1, setup, sh_h1, sl_h1):
    """
    Setup batal jika tren H1 lanjut melampaui TP
    (berarti tidak ada pullback, harga terus searah)
    """
    if setup['type'] == "Long"  and sl_h1 and curr_h1['close'] < sl_h1[-1]['val']:
        return True
    if setup['type'] == "Short" and sh_h1 and curr_h1['close'] > sh_h1[-1]['val']:
        return True
    return False


# ============================================================
# KONEKSI
# ============================================================

def test_connection():
    try:
        res = session.get_server_time()
        if res['retCode'] == 0:
            print(f"✅ Koneksi Bybit OK | Server time: {res['result']['timeSecond']}")
            return True
        print(f"❌ Bybit error: {res}")
        return False
    except Exception as e:
        print(f"❌ Gagal konek: {e}")
        return False


# ============================================================
# REPLAY H1 — reconstruct state saat startup/restart
# ============================================================

def replay_h1(coin, df_h1):
    """Baca candle H1 kiri ke kanan untuk rebuild state setelah restart."""
    sh_h1, sl_h1 = find_swings(df_h1, left=25, right=25)
    if not sh_h1 or not sl_h1:
        return None

    closed_h1 = df_h1.iloc[-2]
    is_long  = closed_h1['close'] > sh_h1[-1]['val']
    is_short = closed_h1['close'] < sl_h1[-1]['val']
    if not (is_long or is_short):
        return None

    stype   = "Long" if is_long else "Short"
    ref_idx = sl_h1[-1]['idx'] if is_long else sh_h1[-1]['idx']

    df_snap = df_h1.copy()
    gaps    = get_internal_gaps(df_snap, stype, ref_idx)
    if not gaps:
        return None

    since_bos = df_snap.iloc[ref_idx:]
    tp_val    = since_bos['high'].max() if stype == "Long" else since_bos['low'].min()
    bos_ts    = df_snap['ts'].iloc[ref_idx]

    state = {
        'type': stype, 'df_h1': df_snap,
        'fvg_list': gaps, 'fvg_idx': 0,
        'tp': tp_val, 'bos_ts': bos_ts,
        'phase': "WAIT_FVG_TOUCH", 'fvg_touch_ts': 0,
        'm5_freeze_high': None, 'm5_freeze_low': None, 'm5_freeze_ts': None,
        'idm_list': [], 'idm_touched_val': None,
    }

    fvg_idx      = 0
    fvg_touch_ts = 0
    phase        = "WAIT_FVG_TOUCH"

    candles_after_bos = df_h1.iloc[ref_idx + 1 : -1]

    for _, candle in candles_after_bos.iterrows():
        if fvg_idx >= len(gaps):
            return None
        active_fvg = gaps[fvg_idx]

        if phase == "WAIT_FVG_TOUCH":
            if stype == "Long" and candle['close'] >= tp_val:
                return None
            if stype == "Short" and candle['close'] <= tp_val:
                return None
            if not price_in_fvg(candle['high'], candle['low'], active_fvg):
                continue
            if body_breaks_fvg(candle, active_fvg, stype):
                fvg_idx += 1
                continue
            if wick_only_touch(candle, active_fvg, stype):
                phase        = "WAIT_IDM_TOUCH"
                fvg_touch_ts = candle['ts']
                continue

        elif phase in ("WAIT_IDM_TOUCH", "WAIT_BOS_BREAK", "WAIT_MSS"):
            if stype == "Long" and candle['close'] >= tp_val:
                return None
            if stype == "Short" and candle['close'] <= tp_val:
                return None
            continue

    state['fvg_idx']      = fvg_idx
    state['phase']        = phase
    state['fvg_touch_ts'] = fvg_touch_ts

    print(f"\n📊 {coin} | BOS {stype} | H:{sh_h1[-1]['val']} C:{curr_h1['close']} L:{sl_h1[-1]['val']}")
    print(f"🔄 {coin}: Replay → Phase:{phase} FVG:{fvg_idx+1}/{len(gaps)}")
    return state


def reconstruct_state():
    print("🔍 Reconstruct state dari H1...")
    for coin in SYMBOLS:
        try:
            time.sleep(1)
            df_h1 = get_data(coin, "60", limit=150)
            if df_h1 is None: continue
            state = replay_h1(coin, df_h1)
            if state:
                pending[coin] = state
                print(f"✅ {coin}: Restored → {state['phase']}")
           
        except Exception as e:
            print(f"⚠️ Replay {coin}: {e}")
    print(f"🔍 Selesai. {len(pending)} coin dimonitor.\n")


# ============================================================
# CORE LOOP
# ============================================================

def run_bot():
    print("🚀 SNIPER V4 | SMC FULL LOGIC | ACTIVE")
    if not test_connection():
        print("⛔ Tidak bisa konek ke Bybit.")
        return
    reconstruct_state()

    while True:

        # Trailing SL
        for coin in list(active_positions.keys()):
            try:
                check_trailing_sl(coin)
            except Exception as e:
                print(f"⚠️ Trailing SL {coin}: {e}")

        for coin in SYMBOLS:
            try:
                time.sleep(2)  # Hindari rate limit

                df_h1_live = get_data(coin, "60", limit=150)
                if df_h1_live is None: continue

                sh_h1, sl_h1 = find_swings(df_h1_live, left=25, right=25)
                if not sh_h1 or not sl_h1: continue

                curr_h1   = df_h1_live.iloc[-1]
                closed_h1 = df_h1_live.iloc[-2]

                

                # ── PROSES SETUP PENDING ──────────────────────────────
                if coin in pending:
                    setup    = pending[coin]
                    stype    = setup['type']
                    fvg_list = setup['fvg_list']
                    fvg_idx  = setup['fvg_idx']

                    # Cek tren H1 berubah → batal semua
                    if h1_trend_broken(curr_h1, setup, sh_h1, sl_h1):
                        print(f"🔄 {coin}: Tren H1 berubah. Batal.")
                        del pending[coin]; continue

                    # Semua FVG habis
                    if fvg_idx >= len(fvg_list):
                        print(f"🗑️ {coin}: Semua FVG habis.")
                        del pending[coin]; continue

                    active_fvg = fvg_list[fvg_idx]

                    # ── PHASE 1: TUNGGU FVG H1 DIWICK ────────────────
                    if setup['phase'] == "WAIT_FVG_TOUCH":
                        if not price_in_fvg(closed_h1['high'], closed_h1['low'], active_fvg):
                            # Cek TP kena duluan
                            if stype == "Long" and curr_h1['close'] >= setup['tp']:
                                print(f"🗑️ {coin}: TP kena sebelum FVG."); del pending[coin]
                            elif stype == "Short" and curr_h1['close'] <= setup['tp']:
                                print(f"🗑️ {coin}: TP kena sebelum FVG."); del pending[coin]
                            continue

                        if body_breaks_fvg(closed_h1, active_fvg, stype):
                            print(f"❌ {coin}: FVG {fvg_idx+1} ditembus body → coba berikutnya.")
                            pending[coin]['fvg_idx'] += 1
                            continue

                        if wick_only_touch(closed_h1, active_fvg, stype):
                            print(f"✅ {coin}: FVG {fvg_idx+1} diwick. Freeze H1, masuk M5.")
                            pending[coin]['phase']        = "WAIT_IDM_TOUCH"
                            pending[coin]['fvg_touch_ts'] = closed_h1['ts']
                        continue

                    # ── AMBIL DATA M5 ─────────────────────────────────
                    time.sleep(1)
                    df_m5_live = get_data(coin, "5", limit=200)
                    if df_m5_live is None: continue

                    touch_ts = setup.get('fvg_touch_ts', setup['bos_ts'])
                    df_m5    = df_m5_live[df_m5_live['ts'] >= touch_ts].reset_index(drop=True)
                    if len(df_m5) < 5:
                        df_m5 = df_m5_live.tail(80).reset_index(drop=True)

                    curr_m5   = df_m5.iloc[-1]
                    closed_m5 = df_m5.iloc[-2] if len(df_m5) >= 2 else curr_m5

                    # ── PHASE 2: TUNGGU IDM TERSENTUH ────────────────
                    # Cari semua IDM di M5, tunggu harga menyentuh
                    # high IDM (Long) atau low IDM (Short)
                    if setup['phase'] == "WAIT_IDM_TOUCH":
                        idm_list = find_idm(df_m5, stype)
                        if not idm_list:
                            # Cek TP kena tanpa IDM
                            if stype == "Long" and curr_m5['close'] >= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa IDM."); del pending[coin]
                            elif stype == "Short" and curr_m5['close'] <= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa IDM."); del pending[coin]
                            continue

                        pending[coin]['idm_list'] = idm_list

                        # Cek apakah ada IDM yang sudah disentuh
                        touched = find_latest_idm_touched(df_m5, idm_list, stype)
                        if touched:
                            print(f"💧 {coin}: IDM tersentuh @ {touched['high'] if stype == 'Long' else touched['low']}. Freeze M5.")
                            pending[coin]['phase']          = "WAIT_BOS_BREAK"
                            pending[coin]['idm_touched_val'] = touched['high'] if stype == "Long" else touched['low']
                            # Freeze: simpan high/low M5 saat IDM tersentuh sebagai struktur
                            df_m5_at_touch = df_m5[df_m5['ts'] <= touched['touch_ts']]
                            pending[coin]['m5_freeze_high'] = df_m5_at_touch['high'].max()
                            pending[coin]['m5_freeze_low']  = df_m5_at_touch['low'].min()
                            pending[coin]['m5_freeze_ts']   = touched['touch_ts']
                        else:
                            if stype == "Long" and curr_m5['close'] >= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa IDM touch."); del pending[coin]
                            elif stype == "Short" and curr_m5['close'] <= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa IDM touch."); del pending[coin]
                        continue

                    # ── PHASE 3: TUNGGU BOS BREAK ─────────────────────
                    # IDM sudah disentuh, M5 di-freeze.
                    # Long  → tunggu break ke BAWAH (BOS bearish M5)
                    # Short → tunggu break ke ATAS  (BOS bullish M5)
                    if setup['phase'] == "WAIT_BOS_BREAK":
                        freeze_low  = setup['m5_freeze_low']
                        freeze_high = setup['m5_freeze_high']
                        freeze_ts   = setup['m5_freeze_ts']

                        # Hanya cek candle setelah freeze
                        df_after = df_m5[df_m5['ts'] > freeze_ts]
                        if df_after.empty:
                            continue

                        bos_broken = False
                        for _, c in df_after.iterrows():
                            if stype == "Long" and c['close'] < freeze_low:
                                # BOS bearish M5 terbentuk
                                bos_broken = True
                                print(f"📉 {coin}: BOS Bearish M5 @ {c['close']}. Cari IDM baru.")
                                break
                            elif stype == "Short" and c['close'] > freeze_high:
                                # BOS bullish M5 terbentuk
                                bos_broken = True
                                print(f"📈 {coin}: BOS Bullish M5 @ {c['close']}. Cari IDM baru.")
                                break

                        if bos_broken:
                            # BOS terbentuk → cari IDM baru dari BOS ini
                            # Reset ke WAIT_IDM_TOUCH dengan timestamp BOS sebagai anchor baru
                            pending[coin]['phase']          = "WAIT_IDM_TOUCH"
                            pending[coin]['m5_freeze_high'] = None
                            pending[coin]['m5_freeze_low']  = None
                            pending[coin]['m5_freeze_ts']   = None
                            pending[coin]['idm_touched_val'] = None
                            pending[coin]['fvg_touch_ts']   = c['ts']  # scan IDM dari candle BOS
                        else:
                            # Cek apakah TP kena (setup gagal)
                            if stype == "Long" and curr_m5['close'] >= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa BOS M5."); del pending[coin]
                            elif stype == "Short" and curr_m5['close'] <= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa BOS M5."); del pending[coin]
                        continue

                    # ── PHASE 4: TUNGGU MSS ───────────────────────────
                    # Setelah BOS M5 terbentuk, IDM baru ditemukan dan disentuh lagi.
                    # Sekarang tunggu MSS:
                    # Long  → break ATAS (bullish) patahkan BOS bearish M5
                    # Short → break BAWAH (bearish) patahkan BOS bullish M5
                    if setup['phase'] == "WAIT_MSS":
                        freeze_low  = setup['m5_freeze_low']
                        freeze_high = setup['m5_freeze_high']
                        freeze_ts   = setup['m5_freeze_ts']

                        df_after = df_m5[df_m5['ts'] > freeze_ts]
                        if df_after.empty:
                            continue

                        mss_candle    = None
                        reset_to_idm  = False

                        for _, c in df_after.iterrows():
                            if stype == "Long":
                                if c['close'] > freeze_high:
                                    # MSS bullish confirmed
                                    mss_candle = c
                                    break
                                elif c['close'] < freeze_low:
                                    # Malah break ke bawah → BOS baru, balik cari IDM
                                    reset_to_idm = True
                                    print(f"🔄 {coin}: Break bawah lagi. Cari IDM baru.")
                                    pending[coin]['phase']          = "WAIT_IDM_TOUCH"
                                    pending[coin]['m5_freeze_high'] = None
                                    pending[coin]['m5_freeze_low']  = None
                                    pending[coin]['m5_freeze_ts']   = None
                                    pending[coin]['fvg_touch_ts']   = c['ts']
                                    break
                            else:  # Short
                                if c['close'] < freeze_low:
                                    # MSS bearish confirmed
                                    mss_candle = c
                                    break
                                elif c['close'] > freeze_high:
                                    # Malah break ke atas → BOS baru, balik cari IDM
                                    reset_to_idm = True
                                    print(f"🔄 {coin}: Break atas lagi. Cari IDM baru.")
                                    pending[coin]['phase']          = "WAIT_IDM_TOUCH"
                                    pending[coin]['m5_freeze_high'] = None
                                    pending[coin]['m5_freeze_low']  = None
                                    pending[coin]['m5_freeze_ts']   = None
                                    pending[coin]['fvg_touch_ts']   = c['ts']
                                    break

                        if reset_to_idm or mss_candle is None:
                            if not reset_to_idm:
                                if stype == "Long" and curr_m5['close'] >= setup['tp']:
                                    print(f"🗑️ {coin}: TP kena tanpa MSS."); del pending[coin]
                                elif stype == "Short" and curr_m5['close'] <= setup['tp']:
                                    print(f"🗑️ {coin}: TP kena tanpa MSS."); del pending[coin]
                            continue

                        # MSS confirmed — cek apakah candle MSS di satuan harga FVG H1 mana
                        entry_fvg = None
                        entry_price = None
                        for fvg in fvg_list:
                            if price_in_fvg(mss_candle['high'], mss_candle['low'], fvg):
                                entry_fvg   = fvg
                                # Long → entry top FVG | Short → entry bottom FVG
                                entry_price = fvg['top'] if stype == "Long" else fvg['bottom']
                                break

                        if entry_fvg is None:
                            print(f"⏳ {coin}: MSS terjadi tapi tidak di zona FVG H1. Cari IDM lagi.")
                            pending[coin]['phase']          = "WAIT_IDM_TOUCH"
                            pending[coin]['m5_freeze_high'] = None
                            pending[coin]['m5_freeze_low']  = None
                            pending[coin]['m5_freeze_ts']   = None
                            continue

                        # SL di ujung candle MSS
                        sl_price  = mss_candle['low'] if stype == "Long" else mss_candle['high']
                        side_order = "Buy" if stype == "Long" else "Sell"

                        print(f"🎯 {coin}: {side_order} @ {entry_price} | SL {sl_price} | TP {setup['tp']}")

                        if place_limit_order(coin, side_order, entry_price, sl_price, setup['tp']):
                            print(f"✅ {coin}: ORDER TERPASANG!")
                            active_positions[coin] = {
                                'side': side_order, 'entry': entry_price,
                                'sl': sl_price, 'tp': setup['tp'], 'sl_moved': False
                            }
                            del pending[coin]
                        else:
                            print(f"⚠️ {coin}: Gagal pasang order.")
                    continue

                # ── SCAN BOS H1 BARU ──────────────────────────────────
                is_long  = closed_h1['close'] > sh_h1[-1]['val']
                is_short = closed_h1['close'] < sl_h1[-1]['val']
                if not (is_long or is_short): continue

                stype   = "Long" if is_long else "Short"
                ref_idx = sl_h1[-1]['idx'] if is_long else sh_h1[-1]['idx']

                df_h1_snap = df_h1_live.copy()
                gaps       = get_internal_gaps(df_h1_snap, stype, ref_idx)
                if not gaps:
                    print(f"⚠️ {coin}: BOS {stype} tapi tidak ada FVG.")
                    continue

                since_bos = df_h1_snap.iloc[ref_idx:]
                tp_val    = since_bos['high'].max() if stype == "Long" else since_bos['low'].min()

                pending[coin] = {
                    'type': stype, 'df_h1': df_h1_snap,
                    'fvg_list': gaps, 'fvg_idx': 0,
                    'tp': tp_val, 'bos_ts': df_h1_snap['ts'].iloc[ref_idx],
                    'phase': "WAIT_FVG_TOUCH", 'fvg_touch_ts': 0,
                    'm5_freeze_high': None, 'm5_freeze_low': None, 'm5_freeze_ts': None,
                    'idm_list': [], 'idm_touched_val': None,
                }
                print(f"\n📊 {coin} | H:{sh_h1[-1]['val']} C:{curr_h1['close']} L:{sl_h1[-1]['val']}")
                print(f"🎯 {coin}: BOS {stype} | {len(gaps)} FVG | TP:{tp_val}")
                for i, g in enumerate(gaps):
                    print(f"   FVG {i+1}: bottom:{g['bottom']} top:{g['top']}")

            except Exception as e:
                print(f"⚠️ Error {coin}: {e}"); continue

        time.sleep(10)


if __name__ == "__main__":
    run_bot()
