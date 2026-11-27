import os
import json
import asyncio
import aiohttp
from datetime import datetime, timezone

BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
CHAT_ID = os.environ["TG_CHAT_ID"]
INTERVALS = ["5m", "15m", "1h", "4h"]
QUOTE = "USDT"
TOP_N = 100
SPOT_ROOTS = ["https://data-api.binance.vision", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]
FUTURES_ROOTS = ["https://fapi1.binance.com", "https://fapi2.binance.com", "https://fapi3.binance.com", "https://fapi4.binance.com", "https://fapi.binance.com"]
TIMEOUT = aiohttp.ClientTimeout(total=20)
STATE_FILE = "sent_signals.json"

def is_pattern(c):
    # c contains five completed klines, oldest first.
    first_three_green = all(float(x[4]) > float(x[1]) for x in c[:3])
    fourth_red = float(c[3][4]) < float(c[3][1])
    fifth_green = float(c[4][4]) > float(c[4][1])
    red_inside_third = (
        float(c[3][2]) < float(c[2][2]) and
        float(c[3][3]) > float(c[2][3])
    )
    return first_three_green and fourth_red and red_inside_third and fifth_green

async def get_json(session, url, params=None):
    async with session.get(url, params=params) as r:
        r.raise_for_status()
        return await r.json()

async def get_json_any(session, roots, path, params=None):
    errors = []
    for root in roots:
        try:
            return await get_json(session, root + path, params)
        except Exception as e:
            errors.append(f"{root}: {e}")
    raise RuntimeError("All Binance API endpoints failed: " + " | ".join(errors))

async def get_symbols(session, futures=False):
    roots = FUTURES_ROOTS if futures else SPOT_ROOTS
    info_path = "/fapi/v1/exchangeInfo" if futures else "/api/v3/exchangeInfo"
    ticker_path = "/fapi/v1/ticker/24hr" if futures else "/api/v3/ticker/24hr"
    info = await get_json_any(session, roots, info_path)
    tickers = await get_json_any(session, roots, ticker_path)
    allowed = {
        s["symbol"] for s in info["symbols"]
        if s.get("quoteAsset") == QUOTE
        and s.get("status") == "TRADING"
        and (s.get("contractType") == "PERPETUAL" if futures else s.get("isSpotTradingAllowed", False))
    }
    ranked = [t for t in tickers if t["symbol"] in allowed]
    ranked.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
    return [x["symbol"] for x in ranked[:TOP_N]]

async def scan_one(session, symbol, interval, futures=False):
    roots = FUTURES_ROOTS if futures else SPOT_ROOTS
    path = "/fapi/v1/klines" if futures else "/api/v3/klines"
    try:
        rows = await get_json_any(session, roots, path, {"symbol": symbol, "interval": interval, "limit": 6})
        # Ignore the currently forming candle; evaluate the latest five closed candles.
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        closed = [k for k in rows if int(k[6]) < now_ms]
        if len(closed) < 5:
            return None
        candles = closed[-5:]
        if is_pattern(candles):
            close_time = datetime.fromtimestamp(int(candles[-1][6]) / 1000, timezone.utc)
            market = "Futures" if futures else "Spot"
            return (market, symbol, interval, close_time.strftime("%Y-%m-%d %H:%M UTC"))
    except Exception as e:
        print(f"Error {symbol} {interval} futures={futures}: {e}")
    return None

async def telegram(session, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with session.post(url, json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}) as r:
        body = await r.text()
        if r.status >= 400:
            raise RuntimeError(f"Telegram error {r.status}: {body}")

def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

async def main():
    state = load_state()
    connector = aiohttp.TCPConnector(limit=12)
    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=connector) as session:
        tasks = []
        for futures in (False, True):
            symbols = await get_symbols(session, futures)
            market = "Futures" if futures else "Spot"
            print(f"{market}: scanning {len(symbols)} USDT pairs")
            for symbol in symbols:
                for interval in INTERVALS:
                    tasks.append(scan_one(session, symbol, interval, futures))
        results = await asyncio.gather(*tasks)
        found = [x for x in results if x]
        new_count = 0
        for market, symbol, interval, closed_at in found:
            key = market + ":" + symbol + ":" + interval + ":" + closed_at
            if key in state:
                continue
            await telegram(session, f"🟢 Свічковий сигнал\nРинок: Binance {market}\nПара: {symbol}\nТаймфрейм: {interval}\nП'ята свічка закрилась: {closed_at}\nПеревір графік перед угодою.")
            state[key] = True
            new_count += 1
        if len(state) > 2000:
            state = dict(list(state.items())[-2000:])
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        print(f"Finished. New signals: {new_count}")

if __name__ == "__main__":
    asyncio.run(main())
