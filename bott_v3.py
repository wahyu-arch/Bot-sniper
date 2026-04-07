import pandas as pd
import numpy as np
from pybit.unified_trading import HTTP
import os
import time

# ============================================================
# CONFIG — ambil dari environment variable (Railway)
# ============================================================
API_KEY    = os.environ.get('API_KEY', '')
API_SECRET = os.environ.get('API_SECRET', '')
CATEGORY   = "linear"
TESTNET    = os.environ.get('TESTNET', 'false').lower() == 'true'

if not API_KEY or not API_SECRET:
    raise ValueError("❌ API_KEY dan API_SECRET belum diset! Cek Environment Variables di Railway.")

session = HTTP(testnet=TESTNET, api_key=API_KEY, api_secret=API_SECRET)

# Test koneksi ke Bybit saat startup
def test_connection():
    try:
        res = session.get_server_time()
        if res['retCode'] == 0:
            print(f"✅ Koneksi Bybit OK | Server time: {res['result']['timeSecond']}")
            return True
        else:
            print(f"❌ Bybit response error: {res}")
            return False
    except Exception as e:
        print(f"❌ Gagal konek ke Bybit: {e}")
        print("   Kemungkinan IP Railway diblokir Bybit.")
        print("   Solusi: gunakan Bybit API dengan whitelist IP dimatikan,")
        print("   atau pakai proxy/VPN di Railway via environment variable.")
        return False

SYMBOLS = [
    'XVGUSDT', 'BELUSDT', 'TAOUSDT', '1000BONKUSDT', 'BTCUSDT', 'BERAUSDT',
    'APTUSDT', 'DASHUSDT', 'DOGEUSDT', 'JUPUSDT', 'USUALUSDT',
    'UNIUSDT', 'HANAUSDT', 'FARTCOINUSDT', '1000PEPEUSDT',
]

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
        print(f"⚠️ get_data {symbol} error: {res.get('retMsg','')}")
        return None
    except Exception as e:
        print(f"⚠️ get_data {symbol} exception: {e}")
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


def find_idm_swept(df, setup_type):
    """
    IDM = BOS single move, pola A → [B..I] → J:
      A        = candle yang bikin high/low
      B sampai I = berapapun candle konsolidasi, TIDAK ADA yang melewati high/low A
      J        = candle pertama yang melewati high/low A (IDM diambil)

    H1 BOS Bullish = Short setup → IDM BEARISH
      A bikin low, B..I tidak ada yang lebih rendah dari low A, J tembus low A

    H1 BOS Bearish = Long setup → IDM BULLISH
      A bikin high, B..I tidak ada yang lebih tinggi dari high A, J tembus high A
    """
    swept_list = []

    if setup_type == "Short":
        for i in range(0, len(df) - 2):
            low_a  = df['low'].iloc[i]
            high_a = df['high'].iloc[i]

            # Scan candle setelah A: cari konsolidasi (tidak melewati low A)
            # lalu temukan J yang melewati low A
            valid_consolidation = False
            for j in range(i + 1, len(df)):
                if df['low'].iloc[j] >= low_a:
                    # Candle ini masih konsolidasi (tidak melewati low A)
                    valid_consolidation = True
                else:
                    # Candle j melewati low A
                    if valid_consolidation:
                        # Ada minimal 1 candle konsolidasi sebelumnya → IDM valid
                        swept_list.append({
                            'val'      : low_a,
                            'high_a'   : high_a,
                            'idx'      : i,
                            'swept_idx': j,
                            'ts'       : df['ts'].iloc[i]
                        })
                    break  # Stop, sudah ketemu J (atau langsung tembus tanpa konsolidasi)

    else:  # Long setup
        for i in range(0, len(df) - 2):
            high_a = df['high'].iloc[i]
            low_a  = df['low'].iloc[i]

            valid_consolidation = False
            for j in range(i + 1, len(df)):
                if df['high'].iloc[j] <= high_a:
                    # Candle ini masih konsolidasi (tidak melewati high A)
                    valid_consolidation = True
                else:
                    # Candle j melewati high A
                    if valid_consolidation:
                        swept_list.append({
                            'val'      : high_a,
                            'low_a'    : low_a,
                            'idx'      : i,
                            'swept_idx': j,
                            'ts'       : df['ts'].iloc[i]
                        })
                    break

    return swept_list


# ============================================================
# FUNGSI FVG
# ============================================================

def get_internal_gaps(df, setup_type, start_idx):
    gaps    = []
    end_idx = len(df) - 2
    for i in range(end_idx, start_idx + 2, -1):
        gap = None
        if setup_type == "Short" and df['low'].iloc[i-2] > df['high'].iloc[i]:
            gap = {"top": df['low'].iloc[i-2], "bottom": df['high'].iloc[i]}
        elif setup_type == "Long" and df['high'].iloc[i-2] < df['low'].iloc[i]:
            gap = {"top": df['low'].iloc[i], "bottom": df['high'].iloc[i-2]}
        if gap:
            is_fresh = True
            for j in range(i + 1, len(df)):
                if (setup_type == "Short" and df['close'].iloc[j] > gap['bottom']) or \
                   (setup_type == "Long"  and df['close'].iloc[j] < gap['top']):
                    is_fresh = False; break
            if is_fresh:
                gaps.append(gap)
    return gaps


def price_in_fvg(price_high, price_low, fvg):
    return price_low <= fvg['top'] and price_high >= fvg['bottom']


def body_breaks_fvg(candle, fvg, setup_type):
    body_top    = max(candle['open'], candle['close'])
    body_bottom = min(candle['open'], candle['close'])
    if setup_type == "Short":
        return body_bottom < fvg['top'] and body_top > fvg['bottom']
    else:
        return body_top > fvg['bottom'] and body_bottom < fvg['top']


def wick_only_touch(candle, fvg, setup_type):
    body_top    = max(candle['open'], candle['close'])
    body_bottom = min(candle['open'], candle['close'])
    if setup_type == "Short":
        return candle['low'] <= fvg['top'] and body_bottom >= fvg['top']
    else:
        return candle['high'] >= fvg['bottom'] and body_top <= fvg['bottom']


# ============================================================
# FUNGSI ORDER
# ============================================================

def place_precision_limit(symbol, side, entry, sl, tp):
    try:
        res_bal  = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        balance  = float(res_bal['result']['list'][0]['totalEquity'])
        risk_usd = balance * 0.01
        dist     = abs(entry - sl)
        if dist == 0:
            print(f"⚠️ {symbol}: dist entry-SL = 0, skip.")
            return False
        qty = round(risk_usd / dist, 2)
        print(f"   Balance: {balance:.2f} | Risk: {risk_usd:.2f} | Dist: {dist} | Qty: {qty}")
        res = session.place_order(
            category=CATEGORY, symbol=symbol, side=side,
            orderType="Limit", qty=str(qty), price=str(entry),
            stopLoss=str(sl), takeProfit=str(tp),
            timeInForce="GTC"
        )
        if res['retCode'] == 0:
            return True
        else:
            print(f"⚠️ {symbol}: Order ditolak → {res.get('retMsg', '')} (code: {res['retCode']})")
            return False
    except Exception as e:
        print(f"⚠️ {symbol}: place_order exception → {e}")
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
# TRAILING SL
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
    if setup['type'] == "Short" and sh_h1 and curr_h1['close'] > sh_h1[-1]['val']:
        return True
    if setup['type'] == "Long"  and sl_h1 and curr_h1['close'] < sl_h1[-1]['val']:
        return True
    return False



# ============================================================
# REPLAY H1 — reconstruct state saat startup/restart
# ============================================================

def replay_h1(coin, df_h1):
    """
    Baca candle H1 dari kiri ke kanan sejak BOS terbentuk.
    Tujuannya: tahu phase mana yang benar sekarang tanpa perlu
    menunggu candle baru — seperti membaca chart manual.

    Return: dict state pending, atau None kalau tidak ada setup aktif.
    """
    sh_h1, sl_h1 = find_swings(df_h1, left=8, right=8)
    if not sh_h1 or not sl_h1:
        return None

    # Cari BOS terakhir yang valid
    last_close = df_h1.iloc[-2]['close']  # candle terakhir yang sudah close
    is_short   = last_close < sl_h1[-1]['val']
    is_long    = last_close > sh_h1[-1]['val']

    if not (is_short or is_long):
        return None

    stype   = "Short" if is_short else "Long"
    ref_idx = sh_h1[-1]['idx'] if is_short else sl_h1[-1]['idx']

    # Ambil semua FVG internal BOS (dari snapshot saat BOS)
    df_snap = df_h1.copy()
    gaps    = get_internal_gaps(df_snap, stype, ref_idx)
    if not gaps:
        return None

    since_bos = df_snap.iloc[ref_idx:]
    tp_val    = since_bos['low'].min() if is_short else since_bos['high'].max()
    bos_ts    = df_snap['ts'].iloc[ref_idx]

    # State awal
    state = {
        'type': stype, 'df_h1': df_snap,
        'fvg_list': gaps, 'fvg_idx': 0,
        'tp': tp_val, 'bos_ts': bos_ts,
        'phase': "WAIT_FVG_TOUCH", 'fvg_touch_ts': 0,
        'df_m5_frozen': None, 'm5_bos_high': None,
        'm5_bos_low': None, 'm5_idm_val': None,
        'mss_wick_ts': None, 'mss_struct_val': None,
        'mss_sl_candidate': None,
    }

    # ── REPLAY candle H1 dari kiri ke kanan sejak BOS ──
    # Gunakan candle yang sudah close saja (index ref_idx+1 sampai -2)
    candles_after_bos = df_h1.iloc[ref_idx + 1 : -1]  # exclude candle live terakhir

    fvg_idx        = 0
    fvg_touch_ts   = 0
    phase          = "WAIT_FVG_TOUCH"

    for _, candle in candles_after_bos.iterrows():
        if fvg_idx >= len(gaps):
            # Semua FVG habis → setup invalid
            return None

        active_fvg = gaps[fvg_idx]

        if phase == "WAIT_FVG_TOUCH":
            # Cek apakah TP sudah kena sebelum FVG → setup batal
            if stype == "Short" and candle['close'] <= tp_val:
                return None
            if stype == "Long"  and candle['close'] >= tp_val:
                return None

            if not price_in_fvg(candle['high'], candle['low'], active_fvg):
                continue

            # FVG disentuh — cek tipe sentuhan
            if body_breaks_fvg(candle, active_fvg, stype):
                # Body masuk → FVG gagal, coba berikutnya
                fvg_idx += 1
                continue

            if wick_only_touch(candle, active_fvg, stype):
                # Wick valid → langsung ke WAIT_IDM_SWEPT
                phase        = "WAIT_IDM_SWEPT"
                fvg_touch_ts = candle['ts']
                continue

        elif phase == "WAIT_IDM_SWEPT":
            # Di M5 phase — cek apakah TP sudah kena
            if stype == "Short" and candle['close'] <= tp_val:
                return None
            if stype == "Long"  and candle['close'] >= tp_val:
                return None
            # Tetap di phase ini, nanti M5 yang putuskan
            continue

    # Update state dengan hasil replay
    state['fvg_idx']      = fvg_idx
    state['phase']        = phase
    state['fvg_touch_ts'] = fvg_touch_ts

    
    return state


def reconstruct_state():
    """Jalankan replay untuk semua coin saat startup."""
    
    for coin in SYMBOLS:
        try:
            time.sleep(0.3)
            df_h1 = get_data(coin, "60", limit=150)
            if df_h1 is None:
                continue
            state = replay_h1(coin, df_h1)
            if state:
                pending[coin] = state
                
        except Exception as e:
            print(f"⚠️ Replay {coin}: {e}")

# ============================================================
# CORE LOOP
# ============================================================

def run_bot():
    print("🚀 SNIPER V3 | SMC FULL LOGIC | ACTIVE")
    if not test_connection():
        print("⛔ Bot berhenti karena tidak bisa konek ke Bybit.")
        return
    reconstruct_state()
    while True:

        for coin in list(active_positions.keys()):
            try:
                check_trailing_sl(coin)
            except Exception as e:
                print(f"⚠️ Trailing SL {coin}: {e}")

        for coin in SYMBOLS:
            try:
                time.sleep(0.5)

                df_h1_live = get_data(coin, "60", limit=150)
                if df_h1_live is None: continue

                sh_h1, sl_h1 = find_swings(df_h1_live, left=8, right=8)
                if not sh_h1 or not sl_h1: continue

                curr_h1   = df_h1_live.iloc[-1]
                closed_h1 = df_h1_live.iloc[-2]

                

                # ── PROSES SETUP PENDING ─────────────────────────────────
                if coin in pending:
                    setup    = pending[coin]
                    fvg_list = setup['fvg_list']
                    fvg_idx  = setup['fvg_idx']
                    stype    = setup['type']

                    if h1_trend_broken(curr_h1, setup, sh_h1, sl_h1):
                        print(f"🔄 {coin}: Tren H1 berubah. Setup dibatalkan.")
                        del pending[coin]; continue

                    if fvg_idx >= len(fvg_list):
                        print(f"🗑️ {coin}: Semua FVG habis.")
                        del pending[coin]; continue

                    active_fvg = fvg_list[fvg_idx]

                    # PHASE 1 — TUNGGU FVG H1 DISENTUH
                    if setup['phase'] == "WAIT_FVG_TOUCH":
                        if not price_in_fvg(closed_h1['high'], closed_h1['low'], active_fvg):
                            if stype == "Short" and curr_h1['close'] <= setup['tp']:
                                print(f"🗑️ {coin}: TP kena sebelum FVG."); del pending[coin]
                            elif stype == "Long" and curr_h1['close'] >= setup['tp']:
                                print(f"🗑️ {coin}: TP kena sebelum FVG."); del pending[coin]
                            continue

                        if body_breaks_fvg(closed_h1, active_fvg, stype):
                            print(f"❌ {coin}: FVG {fvg_idx+1} ditembus body. Coba berikutnya.")
                            pending[coin]['fvg_idx'] += 1
                            continue

                        if wick_only_touch(closed_h1, active_fvg, stype):
                            print(f"✅ {coin}: FVG {fvg_idx+1} valid (wick). Masuk M5.")
                            # Langsung ke WAIT_IDM_SWEPT — tidak ada phase BOS M5 terpisah
                            # karena BOS hanya valid setelah IDM di-sweep
                            pending[coin]['phase']        = "WAIT_IDM_SWEPT"
                            pending[coin]['fvg_touch_ts'] = closed_h1['ts']
                            pending[coin]['df_m5_frozen'] = None
                        continue

                    # AMBIL DATA M5
                    df_m5_live = get_data(coin, "5", limit=200)
                    if df_m5_live is None: continue

                    touch_ts = setup.get('fvg_touch_ts', setup['bos_ts'])
                    df_m5    = df_m5_live[df_m5_live['ts'] >= touch_ts].reset_index(drop=True)
                    if len(df_m5) < 5:
                        df_m5 = df_m5_live.tail(60).reset_index(drop=True)

                    curr_m5   = df_m5.iloc[-1]
                    closed_m5 = df_m5.iloc[-2] if len(df_m5) >= 2 else curr_m5

                    # PHASE 2 — TUNGGU IDM DI-SWEPT
                    # IDM harus di-sweep dulu sebelum BOS apapun bisa terjadi
                    # Short (H1 Bullish) → IDM bearish (low di-sweep)
                    # Long  (H1 Bearish) → IDM bullish (high di-sweep)
                    if setup['phase'] == "WAIT_IDM_SWEPT":
                        idm_list = find_idm_swept(df_m5, stype)
                        if idm_list:
                            latest_idm = idm_list[-1]
                            pending[coin]['m5_idm_val']     = latest_idm['val']
                            pending[coin]['m5_idm_swept_ts'] = df_m5['ts'].iloc[latest_idm['swept_idx']]
                            print(f"💧 {coin}: IDM swept @ {latest_idm['val']}. Tunggu MSS.")
                            pending[coin]['phase'] = "WAIT_MSS"
                        # Cek TP kena sebelum IDM
                        if coin in pending:
                            if stype == "Short" and curr_m5['close'] <= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa IDM."); del pending[coin]
                            elif stype == "Long" and curr_m5['close'] >= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa IDM."); del pending[coin]
                        continue

                    # PHASE 4 — TUNGGU MSS M5
                    # Short (H1 Bullish) → MSS = break ATAS (bullish) patahkan BOS bearish M5
                    # Long  (H1 Bearish) → MSS = break BAWAH (bearish) patahkan BOS bullish M5
                    if setup['phase'] == "WAIT_MSS":
                        sh_m5_mss, sl_m5_mss = find_swings(df_m5, left=3, right=3)
                        mss_confirmed = False
                        wick_detected = False
                        sl_order      = None

                        if stype == "Short" and sh_m5_mss:
                            # MSS Short = close di ATAS swing high M5 (patahkan BOS bearish)
                            struct_val = sh_m5_mss[-1]['val']
                            if closed_m5['close'] > struct_val:
                                mss_confirmed = True
                                sl_order      = closed_m5['low']  # SL di low candle MSS
                                print(f"🔺 {coin}: MSS Short (bullish break) @ {closed_m5['close']}")
                            elif closed_m5['high'] > struct_val and closed_m5['close'] <= struct_val:
                                # Wick tembus ke atas tapi close masih di bawah
                                wick_detected = True
                                pending[coin]['mss_wick_ts']      = closed_m5['ts']
                                pending[coin]['mss_struct_val']   = struct_val
                                pending[coin]['mss_sl_candidate'] = closed_m5['low']
                                print(f"👀 {coin}: MSS Short wick detected @ {closed_m5['high']}. Tunggu body.")

                        elif stype == "Long" and sl_m5_mss:
                            # MSS Long = close di BAWAH swing low M5 (patahkan BOS bullish)
                            struct_val = sl_m5_mss[-1]['val']
                            if closed_m5['close'] < struct_val:
                                mss_confirmed = True
                                sl_order      = closed_m5['high']  # SL di high candle MSS
                                print(f"🔻 {coin}: MSS Long (bearish break) @ {closed_m5['close']}")
                            elif closed_m5['low'] < struct_val and closed_m5['close'] >= struct_val:
                                wick_detected = True
                                pending[coin]['mss_wick_ts']      = closed_m5['ts']
                                pending[coin]['mss_struct_val']   = struct_val
                                pending[coin]['mss_sl_candidate'] = closed_m5['high']
                                print(f"👀 {coin}: MSS Long wick detected @ {closed_m5['low']}. Tunggu body.")

                        # Cek wick sebelumnya yang menunggu konfirmasi body
                        if not mss_confirmed and not wick_detected and setup.get('mss_wick_ts'):
                            struct_val   = setup['mss_struct_val']
                            sl_candidate = setup['mss_sl_candidate']

                            if stype == "Short":
                                if closed_m5['close'] > struct_val and closed_m5['ts'] > setup['mss_wick_ts']:
                                    mss_confirmed = True
                                    sl_order      = sl_candidate
                                    print(f"🔺 {coin}: MSS Short confirmed (body setelah wick) @ {closed_m5['close']}")
                                elif closed_m5['close'] < struct_val and closed_m5['ts'] > setup['mss_wick_ts']:
                                    # Setelah wick malah lanjut turun → MSS gagal
                                    print(f"❌ {coin}: Setelah wick MSS malah lanjut turun. Reset IDM.")
                                    pending[coin]['mss_wick_ts'] = pending[coin]['mss_struct_val'] = pending[coin]['mss_sl_candidate'] = None
                                    pending[coin]['phase'] = "WAIT_IDM_SWEPT"
                                    continue
                            else:
                                if closed_m5['close'] < struct_val and closed_m5['ts'] > setup['mss_wick_ts']:
                                    mss_confirmed = True
                                    sl_order      = sl_candidate
                                    print(f"🔻 {coin}: MSS Long confirmed (body setelah wick) @ {closed_m5['close']}")
                                elif closed_m5['close'] > struct_val and closed_m5['ts'] > setup['mss_wick_ts']:
                                    print(f"❌ {coin}: Setelah wick MSS malah lanjut naik. Reset IDM.")
                                    pending[coin]['mss_wick_ts'] = pending[coin]['mss_struct_val'] = pending[coin]['mss_sl_candidate'] = None
                                    pending[coin]['phase'] = "WAIT_IDM_SWEPT"
                                    continue

                        if not mss_confirmed:
                            if stype == "Short" and curr_m5['close'] <= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa MSS."); del pending[coin]
                            elif stype == "Long" and curr_m5['close'] >= setup['tp']:
                                print(f"🗑️ {coin}: TP kena tanpa MSS."); del pending[coin]
                            continue

                        entry_fvg = None
                        for fvg in fvg_list:
                            if price_in_fvg(closed_m5['high'], closed_m5['low'], fvg):
                                entry_fvg = fvg; break

                        if entry_fvg is None:
                            print(f"⏳ {coin}: MSS di luar FVG H1. Tunggu lagi.")
                            pending[coin]['phase'] = "WAIT_IDM_SWEPT"
                            pending[coin]['mss_wick_ts'] = None
                            continue

                        # Short (H1 Bullish) → entry top FVG | Long (H1 Bearish) → entry bottom FVG
                        entry_price = entry_fvg['top'] if stype == "Short" else entry_fvg['bottom']
                        side_order  = "Sell" if stype == "Short" else "Buy"

                        print(f"🎯 {coin}: {side_order} @ {entry_price} | SL {sl_order} | TP {setup['tp']}")

                        if place_precision_limit(coin, side_order, entry_price, sl_order, setup['tp']):
                            print(f"✅ {coin}: ORDER TERPASANG!")
                            active_positions[coin] = {
                                'side': side_order, 'entry': entry_price,
                                'sl': sl_order, 'tp': setup['tp'], 'sl_moved': False
                            }
                            del pending[coin]
                        else:
                            print(f"⚠️ {coin}: Gagal pasang order.")

                    continue

                # ── SCAN BOS H1 BARU ─────────────────────────────────────
                is_short = closed_h1['close'] < sl_h1[-1]['val']
                is_long  = closed_h1['close'] > sh_h1[-1]['val']
                if not (is_short or is_long): continue

                stype   = "Short" if is_short else "Long"
                ref_idx = sh_h1[-1]['idx'] if is_short else sl_h1[-1]['idx']

                df_h1_snap = df_h1_live.copy()
                gaps       = get_internal_gaps(df_h1_snap, stype, ref_idx)
                if not gaps:
                    print(f"⚠️ {coin}: BOS {stype} tapi tidak ada FVG.")
                    continue

                since_bos = df_h1_snap.iloc[ref_idx:]
                tp_val    = since_bos['low'].min() if is_short else since_bos['high'].max()

                pending[coin] = {
                    'type': stype, 'df_h1': df_h1_snap,
                    'fvg_list': gaps, 'fvg_idx': 0,
                    'tp': tp_val, 'bos_ts': df_h1_snap['ts'].iloc[ref_idx],
                    'phase': "WAIT_FVG_TOUCH", 'fvg_touch_ts': 0,
                    'df_m5_frozen': None,
                    'm5_idm_val': None, 'm5_idm_swept_ts': None,
                    'mss_wick_ts': None, 'mss_struct_val': None,
                    'mss_sl_candidate': None,
                }
                print(f"\n📊 {coin} | H:{sh_h1[-1]['val']} C:{curr_h1['close']} L:{sl_h1[-1]['val']}")
                print(f"🎯 {coin}: BOS {stype} | {len(gaps)} FVG | TP: {tp_val}")
                for i, g in enumerate(gaps):
                    print(f"   FVG {i+1}: {g['bottom']} – {g['top']}")

            except Exception as e:
                print(f"⚠️ Error {coin}: {e}"); continue

        time.sleep(5)


if __name__ == "__main__":
    run_bot()
