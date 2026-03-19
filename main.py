import os
import logging
import datetime
import pytz
import time
import math
import asyncio
import pandas_market_calendars as mcal
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController

if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('logs'):
    os.makedirs('logs')

load_dotenv() 

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

log_filename = f"logs/bot_app_{datetime.datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def is_dst_active():
    est = pytz.timezone('US/Eastern')
    return datetime.datetime.now(est).dst() != datetime.timedelta(0)

def get_target_hour():
    return (17, "🌞 서머타임 적용(여름)") if is_dst_active() else (18, "❄️ 서머타임 해제(겨울)")

def is_market_open():
    est = pytz.timezone('US/Eastern')
    today = datetime.datetime.now(est).date()
    nyse = mcal.get_calendar('NYSE')
    return not nyse.schedule(start_date=today, end_date=today).empty

def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    total_req, portions = 0, {}
    
    for tx in sorted_tickers:
        split = cfg.get_split_count(tx)
        portion = cfg.get_seed(tx) / split if split > 0 else 0
        portions[tx] = portion
        total_req += portion
        
    force_turbo_off = cash < total_req
    rem_cash, allocated = cash, {}
    
    for tx in sorted_tickers:
        if rem_cash >= portions[tx]:
            allocated[tx] = rem_cash 
            rem_cash -= portions[tx] 
        else: allocated[tx] = 0 
            
    return sorted_tickers, allocated, force_turbo_off

async def scheduled_token_check(context):
    context.job.data['broker']._get_access_token(force=True)

async def scheduled_force_reset(context):
    if not is_market_open(): return
    app_data = context.job.data
    app_data['cfg'].reset_locks()
    
    # 🦇 [V18.9 패치] 매일 초기화 시 볼린저 밴드 하한선을 1회만 갱신하여 저장 (API 트래픽 최적화)
    for t in app_data['cfg'].get_active_tickers():
        bb_lower = await asyncio.to_thread(app_data['broker'].get_bb_lower, t)
        app_data['cfg'].set_daily_bb_lower(t, bb_lower) # config에 저장 기능 필요 (다음 지시에서 수정 예정)
        
    await context.bot.send_message(chat_id=context.job.chat_id, text=f"🔓 <b>[{app_data.get('target_hour')}:00] 시스템 초기화 완료 (매매 잠금 해제 & 하단 스나이퍼 장전)</b>", parse_mode='HTML')

async def scheduled_premarket_monitor(context):
    if not is_market_open(): return
    app_data = context.job.data
    
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour = app_data.get('target_hour', 18)
    
    if not (now.hour == target_hour and 0 <= now.minute < 30):
        return

    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']

    async with tx_lock:
        _, holdings = broker.get_account_balance()
        if holdings is None: return 

        for t in cfg.get_active_tickers():
            h = holdings.get(t, {'qty': 0, 'avg': 0})
            if int(h['qty']) == 0: continue 

            curr_p = await asyncio.to_thread(broker.get_current_price, t)
            prev_c = await asyncio.to_thread(broker.get_previous_close, t)
            
            plan = strategy.get_plan(t, curr_p, float(h['avg']), int(h['qty']), prev_c, market_type="PRE_CHECK")
            
            if plan['orders']:
                msg = f"🌅 <b>[{t}] 대박! 프리마켓 목표 달성 🎉</b>\n⚡ 전량 익절 주문을 실행합니다!"
                broker.cancel_all_orders_safe(t)
                for o in plan['orders']:
                    res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    msg += f"\n└ {o['desc']}: {'✅' if res.get('rt_cd') == '0' else f'❌({res.get('msg1')})'}"
                    await asyncio.sleep(0.2) 
                await context.bot.send_message(chat_id=context.job.chat_id, text=msg, parse_mode='HTML')

async def scheduled_sniper_monitor(context):
    if not is_market_open(): return
    
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
    if schedule.empty: return
    
    market_open = schedule.iloc[0]['market_open'].astimezone(est)
    market_close = schedule.iloc[0]['market_close'].astimezone(est)
    
    pre_start = market_open.replace(hour=4, minute=0)
    start_monitor = pre_start + datetime.timedelta(minutes=1)
    end_monitor = market_close - datetime.timedelta(minutes=15)
    
    if not (start_monitor <= now_est <= end_monitor):
        return

    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    async with tx_lock:
        cash, holdings = broker.get_account_balance()
        if holdings is None: return
        
        sorted_tickers, allocated_cash, force_turbo_off = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
        
        for t in cfg.get_active_tickers():
            if cfg.get_version(t) != "V17": continue
            if cfg.check_lock(t, "SNIPER"): continue 
            
            h = holdings.get(t, {'qty': 0, 'avg': 0})
            qty = int(h['qty'])
            avg_price = float(h['avg'])
            if qty == 0: continue
            
            curr_p = await asyncio.to_thread(broker.get_current_price, t)
            prev_c = await asyncio.to_thread(broker.get_previous_close, t)
            if curr_p <= 0: continue
            
            # --- 🦇 양방향 암살자 엔진 (하단 스나이퍼 로직 추가) ---
            # 미리 계산해둔 일일 볼린저 밴드 하한선 가져오기
            bb_lower = cfg.get_daily_bb_lower(t) 
            
            # 🚨 1. 하단 돌파 잭팟 (볼린저 밴드 이탈 기습 매수)
            if bb_lower > 0 and curr_p <= bb_lower and curr_p < avg_price:
                # 하단 돌파를 잡았으니, 기존 얌전한 매수(BUY) 주문들을 모조리 취소!
                await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="BUY")
                await asyncio.sleep(1.0)
                
                # 오늘 할당된 1회분 전체 예산을 끌어와 기습 지정가(LIMIT)로 때린다!
                split = cfg.get_split_count(t)
                base_portion = cfg.get_seed(t) / split if split > 0 else 0
                buy_qty = math.floor(base_portion / curr_p) if curr_p > 0 else 0
                
                if buy_qty > 0:
                    res = broker.send_order(t, "BUY", buy_qty, curr_p, "LIMIT")
                    if res.get('rt_cd') == '0':
                        # 명중 시 매매 잠금 (오늘치 1회분은 다 샀으므로)
                        cfg.set_lock(t, "SNIPER") 
                        msg = f"💥 <b>[{t}] 스나이퍼 하단 기습 명중! (볼린저 밴드 이탈)</b>\n"
                        msg += f"📉 실시간 현재가(${curr_p:.2f})가 볼린저 하한선(${bb_lower:.2f})을 무참히 뚫고 내려갔습니다.\n"
                        msg += f"🦇 즉시 방어용(LOC) 매수를 취소하고, <b>{buy_qty}주를 지정가(LIMIT) 기습 매수</b> 때렸습니다!\n"
                        msg += "🔫 당일 스나이퍼 활동을 짜릿하게 종료합니다."
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                        continue # 처리 끝났으니 상단 감시 스킵

            # --- 기존 상단 익절 스나이퍼 로직 유지 ---
            target_pct_val = cfg.get_target_profit(t)
            target_price = math.ceil(avg_price * (1 + target_pct_val / 100.0) * 100) / 100.0
            
            split = cfg.get_split_count(t)
            t_val, _ = cfg.get_absolute_t_val(t, qty, avg_price)
            
            depreciation_factor = 2.0 / split if split > 0 else 0.1
            star_ratio = (target_pct_val / 100.0) - ((target_pct_val / 100.0) * depreciation_factor * t_val)
            star_price = math.ceil(avg_price * (1 + star_ratio) * 100) / 100.0
            
            # 2. 잭팟 (전량 익절)
            if curr_p >= target_price:
                await asyncio.to_thread(broker.cancel_all_orders_safe, t)
                res = broker.send_order(t, "SELL", qty, curr_p, "LIMIT")
                if res.get('rt_cd') == '0':
                    cfg.set_lock(t, "SNIPER")
                    msg = f"🔥 <b>[{t}] 스나이퍼 잭팟 터짐! (목표가 돌파)</b>\n"
                    msg += f"🎯 실시간 현재가(${curr_p:.2f})가 목표가(${target_price:.2f})를 돌파하여 <b>전량 강제 익절</b> 처리했습니다.\n"
                    msg += "🔫 당일 스나이퍼 활동을 완전 종료합니다."
                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                continue
            
            # 3. 쿼터 익절 (스나이퍼)
            is_first_half = t_val < (split / 2)
            trigger_price = star_price if is_first_half else math.ceil(avg_price * 1.0025 * 100) / 100.0
            
            if curr_p >= trigger_price:
                unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                target_odno = None
                
                for o in unfilled:
                    if o.get('sll_buy_dvsn_cd') == '01': 
                        target_odno = o.get('odno')
                        break
                        
                if target_odno:
                    await asyncio.to_thread(broker.cancel_order, t, target_odno)
                    await asyncio.sleep(1.0) 
                    
                    q_qty = math.ceil(qty / 4)
                    res = broker.send_order(t, "SELL", q_qty, curr_p, "LIMIT")
                    
                    if res.get('rt_cd') == '0':
                        cfg.set_lock(t, "SNIPER")
                        phase = "전반전(별값 돌파)" if is_first_half else "후반전(본전+수수료 돌파)"
                        
                        msg = f"🔫 <b>[{t}] V17 시크릿 쿼터 익절 발동! ({phase})</b>\n"
                        msg += f"🎯 실시간 현재가: ${curr_p:.2f} (트리거: ${trigger_price:.2f})\n"
                        msg += f"🛡️ 기존 방어선을 해제하고 {q_qty}주를 <b>지정가(LIMIT)</b>로 즉시 낚아챘습니다!\n"
                        
                        await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="BUY")
                        await asyncio.sleep(1.0)
                        
                        ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                        plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
                        
                        smart_cores = plan.get('smart_core_orders', [])
                        smart_bonus = plan.get('smart_bonus_orders', [])
                        
                        if len(smart_cores) == 0:
                            msg += "\n🛑 <b>[스마트 밸런싱 발동]</b>\n└ 전반전 종가 관망 모드로 전환 (오늘 추가 매수 안 함)"
                        else:
                            msg += "\n🦇 <b>[스마트 방어 매수 장전] (플랜 B 전환)</b>\n"
                            for o in smart_cores:
                                buy_res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                                msg += f"└ {o['desc']} {o['qty']}주: {'✅' if buy_res.get('rt_cd') == '0' else f'❌({buy_res.get('msg1')})'}\n"
                                await asyncio.sleep(0.2)
                            for o in smart_bonus:
                                buy_res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                                msg += f"└ {o['desc']} {o['qty']}주: {'✅' if buy_res.get('rt_cd') == '0' else '❌'}\n"
                                await asyncio.sleep(0.2)
                        
                        msg += "\n🔒 당일 스나이퍼 감시를 안전하게 종료합니다."
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

async def scheduled_regular_trade(context):
    if not is_market_open(): return
    chat_id = context.job.chat_id
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    latest_version = cfg.get_latest_version()
    await context.bot.send_message(chat_id=chat_id, text=f"🌃 <b>[{app_data.get('target_hour')}:30] 다이내믹 스노우볼 {latest_version} 정규장 주문을 준비합니다.</b>", parse_mode='HTML')
    
    async with tx_lock:
        cash, holdings = broker.get_account_balance()
        if holdings is None:
            await context.bot.send_message(chat_id=chat_id, text="❌ 계좌 정보를 불러오지 못해 정규장 주문을 취소합니다.", parse_mode='HTML')
            return

        sorted_tickers, allocated_cash, force_turbo_off = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
        
        plans = {}
        msgs = {t: "" for t in sorted_tickers}
        all_success = {t: True for t in sorted_tickers}

        for t in sorted_tickers:
            if cfg.check_lock(t, "REG"): continue
            h = holdings.get(t, {'qty': 0, 'avg': 0})
            
            curr_p = await asyncio.to_thread(broker.get_current_price, t)
            prev_c = await asyncio.to_thread(broker.get_previous_close, t)
            ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
            
            plan = strategy.get_plan(t, curr_p, float(h['avg']), int(h['qty']), prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], force_turbo_off=force_turbo_off)
            plans[t] = plan
            
            if plan['orders']:
                is_rev = plan.get('is_reverse', False)
                title = f"🔄 <b>[{t}] 리버스 주문 실행 (교차 전송)</b>\n" if is_rev else f"💎 <b>[{t}] 정규장 주문 실행 (교차 전송)</b>\n"
                msgs[t] += title

        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            for o in plans[t].get('core_orders', []):
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                if not is_success: all_success[t] = False
                msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주: {'✅' if is_success else f'❌({res.get('msg1')})'}\n"
                await asyncio.sleep(0.2) 

        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            for o in plans[t].get('bonus_orders', []):
                res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {'✅' if is_success else '❌(잔금패스)'}\n"
                await asyncio.sleep(0.2) 

        for t in sorted_tickers:
            if t not in plans or not plans[t]['orders']: continue
            
            if all_success[t] and len(plans[t].get('core_orders', [])) > 0:
                cfg.set_lock(t, "REG")
                msgs[t] += "\n🔒 <b>필수 주문 정상 전송 완료 (잠금 설정됨)</b>"
            elif not all_success[t]:
                msgs[t] += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류 - 다음 스케줄 재시도)</b>"
            else:
                cfg.set_lock(t, "REG")
                msgs[t] += "\n🔒 <b>보너스 줍줍 주문만 전송 완료 (잠금 설정됨)</b>"
                
            await context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML')

async def scheduled_auto_sync_summer(context):
    if not is_dst_active(): return 
    await run_auto_sync(context, "08:30")

async def scheduled_auto_sync_winter(context):
    if is_dst_active(): return 
    await run_auto_sync(context, "09:30")

async def run_auto_sync(context, time_str):
    chat_id = context.job.chat_id
    bot = context.job.data['bot']
    status_msg = await context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[{time_str}] 장부 자동 동기화(무결성 검증)를 시작합니다.</b>", parse_mode='HTML')
    
    success_tickers = []
    for t in context.job.data['cfg'].get_active_tickers():
        res = await bot.process_auto_sync(t, chat_id, context, silent_ledger=True)
        if res == "SUCCESS":
            success_tickers.append(t)
            
    if success_tickers:
        await bot._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg)
    else:
        await status_msg.edit_text(f"📝 <b>[{time_str}] 장부 동기화 완료</b> (표시할 진행 중인 장부가 없습니다)", parse_mode='HTML')

def main():
    TARGET_HOUR, season_msg = get_target_hour()
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 50)
    print(f"🚀 방치형 자동매매 {latest_version} (방탄 아키텍처 적용 완료)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 08:30(여름) / 09:30(겨울) 자동 변경")
    print(f"⏰ 정규장 주문: {TARGET_HOUR}:30")
    print("=" * 50)
    
    if ADMIN_CHAT_ID: cfg.set_chat_id(ADMIN_CHAT_ID)
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    tx_lock = asyncio.Lock()
    bot = TelegramController(cfg, broker, strategy, tx_lock)
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    for cmd, handler in [
        ("start", bot.cmd_start), 
        ("record", bot.cmd_record), 
        ("history", bot.cmd_history), 
        ("sync", bot.cmd_sync), 
        ("settlement", bot.cmd_settlement), 
        ("seed", bot.cmd_seed), 
        ("ticker", bot.cmd_ticker), 
        ("mode", bot.cmd_mode), 
        ("reset", bot.cmd_reset), 
        ("version", bot.cmd_version),
        ("v17", bot.cmd_v17),
        ("v4", bot.cmd_v4)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    if cfg.get_chat_id():
        jq = app.job_queue
        app_data = {'cfg': cfg, 'broker': broker, 'strategy': strategy, 'target_hour': TARGET_HOUR, 'bot': bot, 'tx_lock': tx_lock}
        kst = pytz.timezone('Asia/Seoul')
        
        for tt in [datetime.time(7,0,tzinfo=kst), datetime.time(11,0,tzinfo=kst), datetime.time(16,30,tzinfo=kst), datetime.time(22,0,tzinfo=kst)]:
            jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), data=app_data)
        
        jq.run_daily(scheduled_auto_sync_summer, time=datetime.time(8, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_auto_sync_winter, time=datetime.time(9, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_force_reset, time=datetime.time(TARGET_HOUR, 0, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_repeating(scheduled_premarket_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_regular_trade, time=datetime.time(TARGET_HOUR, 30, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_repeating(scheduled_sniper_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()
