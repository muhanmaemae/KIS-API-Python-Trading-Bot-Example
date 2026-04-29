# ==========================================================
# [main.py] - 🌟 100% 통합 무결점 완성본 (V44.04) 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# MODIFIED: [V42.16 핫픽스] post_init 함수 내부 asyncio.lock() typo AI 환각 영구 소각 및 교정
# MODIFIED: [V43.10 핫픽스] /avwap 관제탑 및 큐 관리 명령어 라우팅 누락 팩트 교정 완료
# NEW: [V44.04 핫픽스] yfinance 패키지 내부의 불필요한 스팸 로그(possibly delisted 등)가 로그를 오염시키는 현상 원천 차단(침묵 락온)
# ==========================================================

import os
import logging
import datetime
import asyncio
import math 
from zoneinfo import ZoneInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController

from queue_ledger import QueueLedger
from strategy_reversion import ReversionStrategy
from volatility_engine import VolatilityEngine, determine_market_regime

from scheduler_core import (
    scheduled_token_check,
    scheduled_auto_sync_summer,
    scheduled_auto_sync_winter,
    scheduled_force_reset,
    scheduled_self_cleaning,
    perform_self_cleaning
)

from scheduler_sniper import scheduled_sniper_monitor
from scheduler_vwap import scheduled_vwap_trade, scheduled_vwap_init_and_cancel
from scheduler_regular import scheduled_regular_trade
from scheduler_aftermarket import scheduled_after_market_lottery

TICKER_BASE_MAP = {
    "SOXL": "SOXX",
    "TQQQ": "QQQ",
    "TSLL": "TSLA",
    "FNGU": "FNGS",
    "BULZ": "FNGS"
}

if not os.path.exists('data'): os.makedirs('data')
if not os.path.exists('logs'): os.makedirs('logs')

load_dotenv() 

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
try:
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
except ValueError:
    ADMIN_CHAT_ID = None

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

if not all([TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO, ADMIN_CHAT_ID]):
    print("❌ [치명적 오류] .env 파일에 봇 구동 필수 키가 누락되었습니다. 봇을 종료합니다.")
    exit(1)

est_zone = ZoneInfo('America/New_York')
log_filename = f"logs/bot_app_{datetime.datetime.now(est_zone).strftime('%Y%m%d')}.log"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# NEW: [V44.04 핫픽스] yfinance 패키지 내부의 불필요한 스팸 로그(possibly delisted 등)가 로그를 오염시키는 현상 원천 차단(침묵 락온)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

async def scheduled_volatility_scan(context):
    app_data = context.job.data
    cfg = app_data['cfg']
    broker = app_data['broker']
    base_map = app_data.get('base_map', TICKER_BASE_MAP)
    
    print("\n" + "=" * 60)
    print("📈 [자율주행 변동성 & 시장 국면 스캔 완료] (10:20 EST 스냅샷)")
    
    regime_data = await determine_market_regime(broker)
    app_data['regime_data'] = regime_data
    
    if regime_data.get("status") == "success":
        regime = regime_data.get("regime")
        target_ticker = regime_data.get("target_ticker")
        close_p = regime_data.get("close", 0.0)
        prev_vwap = regime_data.get("prev_vwap", 0.0)
        curr_vwap = regime_data.get("curr_vwap", 0.0)
        desc = regime_data.get("desc", "")
        print(f"🏛️ 옴니 매트릭스: [{regime}] 타겟: {target_ticker} ({desc}) | 종가: {close_p:.2f}, 당일VWAP: {curr_vwap:.2f}, 전일VWAP: {prev_vwap:.2f}")
    else:
        print(f"⚠️ 옴니 매트릭스 판별 실패: {regime_data.get('msg')}")

    active_tickers = cfg.get_active_tickers()
    if not active_tickers:
        print("📊 현재 운용 중인 종목이 없습니다.")
    else:
        briefing_lines = []
        vol_engine = VolatilityEngine()
        for ticker in active_tickers:
            target_base = base_map.get(ticker, ticker)
            try:
                weight_data = await asyncio.to_thread(vol_engine.calculate_weight, target_base)
                raw_weight = weight_data.get('weight', 1.0) if isinstance(weight_data, dict) else weight_data
                real_weight = float(raw_weight)
                if not math.isfinite(real_weight):
                    raise ValueError(f"비정상 수학 수치 산출: {real_weight}")
            except Exception as e:
                logging.warning(f"[{ticker}] 변동성 지표 산출 실패. 중립 안전마진(1.0) 강제 적용: {e}")
                real_weight = 1.0 
                
            status_text = "OFF 권장" if real_weight <= 1.0 else "ON 권장"
            briefing_lines.append(f"{ticker}({target_base}): {real_weight:.2f} ({status_text})")
            
        print(f"📊 [자율주행 지표] {' | '.join(briefing_lines)} (상세 게이지: /mode)")
    print("=" * 60 + "\n")

async def post_init(application: Application):
    tx_lock = asyncio.Lock()
    application.bot_data['app_data']['tx_lock'] = tx_lock
    application.bot_data['bot_controller'].tx_lock = tx_lock

def main():
    est_zone = ZoneInfo('America/New_York')
    kst_zone = ZoneInfo('Asia/Seoul')
    now_est = datetime.datetime.now(est_zone)
    is_dst = bool(now_est.dst())
    season_msg = "☀️ 서머타임 (EDT) 적용 중" if is_dst else "❄️ 표준시간 (EST) 적용 중"
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 60)
    print(f"🚀 옴니 매트릭스 퀀트 엔진 {latest_version} (V40.00 락온)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 10:00(KST) 확정 스캔 가동")
    print("=" * 60)
    
    perform_self_cleaning()
    cfg.set_chat_id(ADMIN_CHAT_ID)
    
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    queue_ledger = QueueLedger()
    strategy_rev = ReversionStrategy()
    
    bot = TelegramController(
        cfg, broker, strategy, tx_lock=None, 
        queue_ledger=queue_ledger, strategy_rev=strategy_rev
    )
    
    app_data = {
        'cfg': cfg, 'broker': broker, 'strategy': strategy, 
        'queue_ledger': queue_ledger, 'strategy_rev': strategy_rev,  
        'bot': bot, 'tx_lock': None, 'base_map': TICKER_BASE_MAP,
        'tz_kst': kst_zone, 'tz_est': est_zone,
        'regime_data': None 
    }

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .connection_pool_size(8)
        .post_init(post_init) 
        .build()
    )
    
    app.bot_data['app_data'] = app_data
    app.bot_data['bot_controller'] = bot
    
    for cmd, handler in [
        ("start", bot.cmd_start), ("record", bot.cmd_record), ("history", bot.cmd_history), 
        ("sync", bot.cmd_sync), ("settlement", bot.cmd_settlement), ("seed", bot.cmd_seed), 
        ("ticker", bot.cmd_ticker), ("mode", bot.cmd_mode), ("reset", bot.cmd_reset), 
        ("version", bot.cmd_version), ("update", bot.cmd_update),
        ("avwap", bot.cmd_avwap), ("queue", bot.cmd_queue), ("add_q", bot.cmd_add_q), ("clear_q", bot.cmd_clear_q)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
        
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    jq = app.job_queue
    
    for tt in [datetime.time(7,0,tzinfo=kst_zone), datetime.time(11,0,tzinfo=kst_zone), datetime.time(16,30,tzinfo=kst_zone), datetime.time(22,0,tzinfo=kst_zone)]:
        jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    SYNC_FUNC = scheduled_auto_sync_summer if is_dst else scheduled_auto_sync_winter
    jq.run_daily(SYNC_FUNC, time=datetime.time(10, 0, 5, tzinfo=kst_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    jq.run_daily(scheduled_force_reset, time=datetime.time(4, 0, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    jq.run_daily(scheduled_volatility_scan, time=datetime.time(10, 20, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    jq.run_daily(scheduled_regular_trade, time=datetime.time(4, 5, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_daily(scheduled_vwap_init_and_cancel, time=datetime.time(15, 30, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)

    jq.run_repeating(scheduled_sniper_monitor, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_repeating(scheduled_vwap_trade, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    
    jq.run_daily(scheduled_after_market_lottery, time=datetime.time(16, 5, tzinfo=est_zone), days=(0,1,2,3,4), chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_daily(scheduled_self_cleaning, time=datetime.time(6, 0, tzinfo=kst_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()
