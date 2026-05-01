# ==========================================================
# FILE: scheduler_regular.py
# ==========================================================
# MODIFIED: [V44.43 이벤트 루프 교착 방어 및 타임 쉴드 이식] 동기 함수 is_market_open을 asyncio.to_thread로 래핑하고 10초 타임아웃 족쇄를 채워 스케줄러 증발(Deadlock) 원천 차단. 타임아웃 시 평일 강제 개장(Fail-Open) 방어막 이식 완료.
# MODIFIED: [V44.08 예방 덫 소각] V-REV 예방 덫 가상 에스크로 락온 로직 전면 소각. 자전거래 의심 회피 및 AVWAP 암살자의 가용 예산을 100% 개방하기 위해 04:05 EST의 모든 매수/매도 덫 장전을 0주 상태든 기보유 상태든 전면 백지화 완료.
# MODIFIED: [V44.12 UX 팩트 교정] AVWAP 암살자가 1회분 예산을 사용한다는 시각적 환각(텍스트 오기) 맹점 전면 소각. V-REV 가상 에스크로 100% 격리 팩트를 지시서에 정확히 렌더링하도록 텍스트 디커플링 수술 완료.
# NEW: [V44.46 중복 스케줄러 원천 소각] 레거시 스케줄러 코드(KST 기반 동적 연산 및 pytz 잔재)와의 충돌로 인한 17시 05분 런타임 붕괴 맹점 전면 수술. 단일 무결성 스케줄러로 압축 락온 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import random

from scheduler_core import is_market_open, get_budget_allocation

# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# pandas_market_calendars 등 무거운 I/O 동기 함수를 asyncio 루프 내에서 직접 호출하면 전체 스케줄러가 교착(Deadlock)에 빠져 스케줄이 증발합니다. 반드시 await asyncio.wait_for(asyncio.to_thread(...)) 패턴으로 래핑하세요.
async def scheduled_regular_trade(context):
    try:
        is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
    except asyncio.TimeoutError:
        logging.error("⚠️ is_market_open 달력 API 타임아웃. 평일이므로 강제 개장 처리합니다.")
        est = ZoneInfo('America/New_York')
        is_open = datetime.datetime.now(est).weekday() < 5

    if not is_open:
        return
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    strategy_rev = app_data.get('strategy_rev')
    queue_ledger = app_data.get('queue_ledger')
    
    if tx_lock is None:
        logging.warning("⚠️ [regular_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        await context.bot.send_message(chat_id=context.job.chat_id, text="⚠️ <b>[시스템 경고]</b> tx_lock 미초기화로 정규장 주문을 1회 스킵합니다.", parse_mode='HTML')
        return
    
    jitter_seconds = random.randint(0, 180)

    await context.bot.send_message(
        chat_id=context.job.chat_id, 
        text=f"🌃 <b>[04:05 EST] 통합 주문 장전 및 스냅샷 박제!</b>\n"
             f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 안전하게 주문 전송을 시도합니다.", 
        parse_mode='HTML'
    )

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60

    async def _do_regular_trade():
        est = ZoneInfo('America/New_York')
        _now_est = datetime.datetime.now(est)
        today_str_est = _now_est.strftime("%Y-%m-%d")
        
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}

            sorted_tickers, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            
            all_success_map = {t: True for t in sorted_tickers}
            v_rev_tickers = [] 

            for t in sorted_tickers:
                if cfg.check_lock(t, "REG"):
                    skip_msg = (
                        f"⚠️ <b>[{t}] REG 잠금 미해제 — 주문 스킵</b>\n"
                        f"▫️ 전날 REG 잠금이 자정 초기화 시 해제되지 않아 오늘 04:05 EST 주문 루프에서 제외되었습니다.\n"
                        f"▫️ 수동으로 잠금 해제 후 상태를 확인하십시오."
                    )
                    await context.bot.send_message(context.job.chat_id, skip_msg, parse_mode='HTML')
                    continue
                
                h = safe_holdings.get(t) or {}
                safe_avg = float(h.get('avg') or 0.0)
                safe_qty = int(float(h.get('qty') or 0))

                curr_p = 0.0
                prev_c = 0.0
                for _api_retry in range(3):
                    curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                    prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                    if curr_p > 0 and prev_c > 0:
                        break
                    await asyncio.sleep(2.0)

                if curr_p <= 0 or prev_c <= 0:
                    msgs[t] += (
                        f"🚨 <b>[{t}] 전일 종가/현재가 API 3회 결측 감지!</b>\n"
                        f"▫️ 매수 방어선을 장전하지 못하고 다음 종목으로 넘어갑니다(continue 바이패스).\n"
                    )
                    all_success_map[t] = False
                    await context.bot.send_message(context.job.chat_id, msgs[t], parse_mode='HTML')
                    continue
                
                version = cfg.get_version(t)
                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)

                if version == "V_REV" and is_manual_vwap:
                    msgs[t] += f"🛡️ <b>[{t}] V-REV 수동 시그널 모드 가동 중</b>\n"
                    msgs[t] += "▫️ 봇 자동 주문이 락다운되었습니다. V앱에서 장 마감 30분 전 세팅으로 수동 장전하십시오.\n"
                    await context.bot.send_message(context.job.chat_id, msgs[t], parse_mode='HTML')
                    continue

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    loc_orders = []
                    
                    if version == "V_REV":
                        q_data = queue_ledger.get_queue(t)
                        v_rev_q_qty = sum(item.get("qty", 0) for item in q_data)
                        
                        msgs[t] += f"🛡️ <b>[{t}] V-REV 예방적 덫 장전 기능 전면 소각</b>\n"
                        msgs[t] += "▫️ 자전거래(FDS) 의심을 회피하고 AVWAP 암살자가 자유롭게 타격하도록 예방 덫 기능을 영구 소각했습니다.\n"
                        
                        plan_result = {"orders": [], "trigger_loc": False, "total_q": v_rev_q_qty}
                        if hasattr(strategy_rev, 'save_daily_snapshot'):
                            strategy_rev.save_daily_snapshot(t, plan_result)

                        cfg.set_lock(t, "REG")
                        all_success_map[t] = True
                        v_rev_tickers.append((t, version))
                        continue 

                    elif version == "V14":
                        ma_5day = float(await asyncio.to_thread(broker.get_5day_ma, t) or 0.0)
                        v14_vwap_plugin = strategy.v14_vwap_plugin
                        
                        v14_plan = v14_vwap_plugin.get_plan(
                            ticker=t, current_price=curr_p, avg_price=safe_avg, qty=safe_qty, 
                            prev_close=prev_c, ma_5day=ma_5day, market_type="REG", 
                            available_cash=allocated_cash.get(t, 0.0), is_simulation=False,
                            is_snapshot_mode=True
                        )
                        loc_orders = v14_plan.get('core_orders', [])
                        msgs[t] += f"🛡️ <b>[{t}] 무매4(VWAP) 예방적 LOC 덫 장전 완료</b>\n"

                    sell_success_count = 0
                    for o in loc_orders:
                        res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                        is_success = res.get('rt_cd') == '0'
                        if not is_success: all_success_map[t] = False
                        if is_success and o['side'] == 'SELL':
                            sell_success_count += 1
                            
                        err_msg = res.get('msg1', '오류')
                        status_icon = '✅' if is_success else f'❌({err_msg})'
                        msgs[t] += f"└ {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                        await asyncio.sleep(0.2)
                        
                    if all_success_map[t] and len(loc_orders) > 0:
                        cfg.set_lock(t, "REG")
                        msgs[t] += "\n🔒 <b>방어선 전송 완료 (매매 잠금 설정됨)</b>"
                    elif sell_success_count > 0:
                        cfg.set_lock(t, "REG")
                        msgs[t] += "\n⚠️ <b>일부 방어선 구축 실패 (반쪽짜리 잠금 설정됨)</b>"
                    elif len(loc_orders) == 0:
                        msgs[t] += "\n⚠️ <b>전송할 방어선(예산/수량)이 없습니다.</b>"
                    else:
                        msgs[t] += "\n⚠️ <b>일부 방어선 구축 실패 (잠금 보류)</b>"
                        
                    await context.bot.send_message(context.job.chat_id, msgs[t], parse_mode='HTML')
                    v_rev_tickers.append((t, version))
                    continue
                
                ma_5day = float(await asyncio.to_thread(broker.get_5day_ma, t) or 0.0)
                plan = strategy.get_plan(t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash.get(t, 0.0), is_snapshot_mode=True)
                
                if hasattr(strategy, 'v14_plugin') and hasattr(strategy.v14_plugin, 'save_daily_snapshot'):
                    strategy.v14_plugin.save_daily_snapshot(t, plan)
                    
                plans[t] = plan
                if plan.get('core_orders', []) or plan.get('orders', []):
                    is_rev = plan.get('is_reverse', False)
                    msgs[t] += f"🔄 <b>[{t}] 리버스 주문 실행</b>\n" if is_rev else f"💎 <b>[{t}] 정규장 주문 실행</b>\n"

            for t, ver in v_rev_tickers:
                mod_name = "V-REV" if ver == "V_REV" else "무매4(VWAP)"
                if ver == "V_REV":
                    msg = f"🎺 <b>[{t}] {mod_name} 예방적 방어망 전면 철거 완료</b>\n"
                    msg += f"▫️ 프리장이 개장했습니다! 자전거래(FDS) 의심을 회피하고 <b>AVWAP 암살자</b>의 기동력을 극대화하기 위해 예방적 덫(물리/가상)을 전면 소각했습니다.\n"
                    msg += f"▫️ <b>1회분 예산은 가상 에스크로에 100% 안전하게 격리 보존되며</b>, 암살자는 오직 잉여 현금만으로 자유롭게 타격합니다.\n"
                    msg += f"▫️ 장 후반(15:27 EST) VWAP 엔진이 깨어나 보존된 1회분 예산으로 정밀 타격을 개시합니다! 편안한 밤 보내십시오! 🌙💤\n"
                else:
                    msg = f"🎺 <b>[{t}] {mod_name} 예방적 방어망 장전 완료</b>\n"
                    msg += f"▫️ 프리장이 개장했습니다! 시스템 다운 등 최악의 블랙스완을 대비하여 <b>지층별 분리 종가(LOC) 덫</b>을 KIS 서버에 선제 전송했습니다.\n"
                    msg += f"▫️ 서버가 무사하다면 장 후반(15:27 EST)에 스스로 깨어나 이 덫을 거두고 추세(60% 허들)를 스캔하여 새로운 최적 전술로 교체합니다! 편안한 밤 보내십시오! 🌙💤\n"
                await context.bot.send_message(chat_id=context.job.chat_id, text=msg, parse_mode='HTML')

            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                for o in target_orders:
                    res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                    if res.get('rt_cd') != '0': all_success_map[t] = False
                    
                    err_msg = res.get('msg1', '오류')
                    status_icon = '✅' if res.get('rt_cd') == '0' else f'❌({err_msg})'
                    msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans: continue
                target_bonus = plans[t].get('bonus_orders', [])
                for o in target_bonus:
                    res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                    msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {'✅' if res.get('rt_cd')=='0' else '❌(잔금패스)'}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                target_bonus = plans[t].get('bonus_orders', [])
                if not target_orders and not target_bonus: continue
                
                if all_success_map[t] and len(target_orders) > 0:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>필수 주문 정상 전송 완료 (잠금 설정됨)</b>"
                elif not all_success_map[t] and len(target_orders) > 0:
                    msgs[t] += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"
                elif len(target_bonus) > 0:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>보너스 주문만 전송 완료 (잠금 설정됨)</b>"
                    
                if not any(tx[0] == t for tx in v_rev_tickers): 
                    await context.bot.send_message(chat_id=context.job.chat_id, text=msgs[t], parse_mode='HTML')

            return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_regular_trade(), timeout=300.0)
            if success:
                if attempt > 1:
                    await context.bot.send_message(chat_id=context.job.chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 전송을 완수했습니다!</b>", parse_mode='HTML')
                return 
        except Exception as e:
            logging.error(f"정규장 전송 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1:
                await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️\n<code>사유: {type(e).__name__}: {e}</code>", 
                    parse_mode='HTML'
                )
        else:
            logging.warning(f"정규장 조건 미충족 ({attempt}/{MAX_RETRIES}): {fail_reason}")
            if attempt == 1:
                await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️\n<code>사유: {fail_reason}</code>", 
                    parse_mode='HTML'
                )

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0:
                await context.bot.send_message(chat_id=context.job.chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️", parse_mode='HTML')
            await asyncio.sleep(RETRY_DELAY)

    await context.bot.send_message(chat_id=context.job.chat_id, text="🚨 <b>[긴급 에러] 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML')
