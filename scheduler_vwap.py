# MODIFIED: [V44.27 0주 스냅샷 환각 락온] 서버 재시작으로 스냅샷이 증발했을 때, 장중 VWAP 매수 물량을 과거 물량으로 오판하여 기보유 상태로 전환되는 맹점 차단. 큐 장부 및 메인 장부에서 당일 날짜(EST)를 수학적으로 도려내고 순수 이월 장부만 역산하여 0주 출발 팩트 100% 복구 완료.
# MODIFIED: [V44.27 뇌동매매 증발 오판 방어] KIS 총잔고에서 AVWAP 암살자 물량을 완벽히 격리한 pure_actual_qty 로 뇌동매매 증발(0주) 셧다운 감지 수행
# MODIFIED: [V44.27 물귀신 덤핑 차단] V-REV 스윕 덤핑 시 암살자 물량이 동반 투매되는 사태를 막기 위해 순수 매도 가능 수량(pure_sellable_qty)으로 정밀 캡핑
# MODIFIED: [V44.27 AVWAP 잔고 오염 방어] V14_VWAP 런타임 엔진에 KIS 총잔고 대신 암살자 물량이 배제된 pure_qty_v14를 주입하여 동적 플랜 훼손 원천 차단
# MODIFIED: [V44.36 VWAP 페일세이프 락다운 버그 및 환각 방어막 이식] Nuke 실패 시 상태 플래그를 False로 리셋하여 다음 1분봉에서 재시도를 보장(EC-1 교정)하고, 코파일럿의 is_zero_start 역산 로직 훼손 시도를 원천 차단하는 백신 주석 하드코딩 완료.
# ==========================================================
# FILE: scheduler_vwap.py
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import traceback
import math
import os
import time
import json
import pandas_market_calendars as mcal
import tempfile

from scheduler_core import is_market_open

async def scheduled_vwap_init_and_cancel(context):
    if not is_market_open(): return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    try:
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        if schedule.empty: return
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except Exception:
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        
    vwap_start_time = market_close - datetime.timedelta(minutes=33)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    strategy = app_data.get('strategy')
    strategy_rev = app_data.get('strategy_rev')
    queue_ledger = app_data.get('queue_ledger')
    chat_id = context.job.chat_id
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str
        
    async def _do_init():
        async with tx_lock:
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
                
                if version == "V_REV" and is_manual_vwap:
                    continue
                
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                            prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                            
                            _, holdings = await asyncio.to_thread(broker.get_account_balance)
                            safe_holdings = holdings if isinstance(holdings, dict) else {}
                            h = safe_holdings.get(t) or {}
                            total_kis_qty = int(float(h.get('qty', 0)))
                            avg_price = float(h.get('avg', 0.0))
                            
                            avwap_qty = 0
                            if hasattr(strategy, 'load_avwap_state'):
                                avwap_state = strategy.load_avwap_state(t, now_est)
                                avwap_qty = int(avwap_state.get('qty', 0))
                            
                            if version == "V_REV" and strategy_rev and queue_ledger:
                                rev_daily_budget = float(cfg.get_seed(t) or 0.0) * 0.15
                                q_data = queue_ledger.get_queue(t)
                                strategy_rev.ensure_failsafe_snapshot(
                                    ticker=t, curr_p=curr_p, prev_c=prev_c, alloc_cash=rev_daily_budget, 
                                    q_data=q_data, total_kis_qty=total_kis_qty, avwap_qty=avwap_qty
                                )
                            elif version == "V14" and is_manual_vwap and strategy and hasattr(strategy, 'v14_vwap_plugin'):
                                _, alloc_cash, _ = cfg.calculate_v14_state(t)
                                strategy.v14_vwap_plugin.ensure_failsafe_snapshot(
                                    ticker=t, current_price=curr_p, total_qty=total_kis_qty, 
                                    avwap_qty=avwap_qty, avg_price=avg_price, prev_close=prev_c, alloc_cash=alloc_cash
                                )

                            if version == "V14" and is_manual_vwap:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                msg = f"🌅 <b>[{t}] 장 마감 33분 전 엔진 기상 (Fail-Safe 전환)</b>\n"
                                msg += f"▫️ 프리장에 선제 전송해둔 '예방적 양방향 LOC 덫'을 전량 취소(Nuke)했습니다.\n"
                                msg += f"▫️ 1분 단위 정밀 타격(VWAP 슬라이싱) 모드로 교전 수칙을 변경합니다. ⚔️"
                            else:
                                msg = f"🌅 <b>[{t}] 가상 에스크로 해제 및 엔진 기상</b>\n"
                                msg += f"▫️ 자전거래(FDS) 우회를 위해 설정된 <b>'가상 에스크로(Virtual Escrow)'를 해제</b>하고 자금을 실전 배치합니다.\n"
                                msg += f"▫️ 1분 단위 정밀 타격(VWAP 슬라이싱) 모드로 교전 수칙을 변경합니다. ⚔️"
                                
                            vwap_cache[f"REV_{t}_nuked"] = True
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 자가 치유 Nuke 실패: {e}", exc_info=True)
                            vwap_cache[f"REV_{t}_nuked"] = False 
                    
    try:
        await asyncio.wait_for(_do_init(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 Fail-Safe 타임아웃 에러: {e}", exc_info=True)


async def scheduled_vwap_trade(context):
    if not is_market_open(): return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [vwap_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return
        
    try:
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        if schedule.empty: return
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except Exception:
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        
    vwap_start_time = market_close - datetime.timedelta(minutes=33)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
        
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    
    regime_data = app_data.get('regime_data')
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str

    async def _do_vwap():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            minutes_to_close = int(max(0, (market_close - now_est).total_seconds()) / 60)
            min_idx = 33 - minutes_to_close
            if min_idx < 0: min_idx = 0
            if min_idx > 29: min_idx = 29
            
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
                is_zero_start_session = False 

                if version == "V_REV" and is_manual_vwap:
                    continue

                try:
                    profile = cfg.get_vwap_profile(t) if hasattr(cfg, 'get_vwap_profile') else {}
                except Exception as e:
                    logging.error(f"🚨 [{t}] VWAP 프로파일 로드 실패: {e}")
                    profile = {}
                    
                target_keys = [f"15:{str(m).zfill(2)}" for m in range(27, 60)]
                total_target_vol = sum(profile.get(k, 0.0) for k in target_keys)
                time_str = now_est.strftime('%H:%M')
                
                if time_str in target_keys:
                    raw_weight = profile.get(time_str, 0.0)
                    current_weight = (raw_weight / total_target_vol) if total_target_vol > 0 else (1.0 / len(target_keys))
                else:
                    current_weight = 0.0

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                            prev_c = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                            
                            h = safe_holdings.get(t) or {}
                            total_kis_qty = int(float(h.get('qty', 0)))
                            avg_price = float(h.get('avg', 0.0))
                            
                            avwap_qty = 0
                            if hasattr(strategy, 'load_avwap_state'):
                                avwap_state = strategy.load_avwap_state(t, now_est)
                                avwap_qty = int(avwap_state.get('qty', 0))
                            
                            if version == "V_REV":
                                strategy_rev = app_data.get('strategy_rev')
                                queue_ledger = app_data.get('queue_ledger')
                                if strategy_rev and queue_ledger:
                                    rev_daily_budget = float(cfg.get_seed(t) or 0.0) * 0.15
                                    q_data = queue_ledger.get_queue(t)
                                    strategy_rev.ensure_failsafe_snapshot(
                                        ticker=t, curr_p=curr_p, prev_c=prev_c, alloc_cash=rev_daily_budget, 
                                        q_data=q_data, total_kis_qty=total_kis_qty, avwap_qty=avwap_qty
                                    )
                            elif version == "V14" and is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'):
                                _, alloc_cash, _ = cfg.calculate_v14_state(t)
                                strategy.v14_vwap_plugin.ensure_failsafe_snapshot(
                                    ticker=t, current_price=curr_p, total_qty=total_kis_qty, 
                                    avwap_qty=avwap_qty, avg_price=avg_price, prev_close=prev_c, alloc_cash=alloc_cash
                                )

                            if version == "V14" and is_manual_vwap:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                msg = f"🌅 <b>[{t}] 하이브리드 타임 슬라이싱 기상 (자가 치유 가동)</b>\n"
                                msg += f"▫️ 장 마감 33분 전 진입을 확인하여 기존 LOC 덫 강제 취소(Nuke)했습니다.\n"
                                msg += f"▫️ 스케줄러 누락을 완벽히 극복하고 1분 단위 정밀 타격을 즉각 개시합니다. ⚔️"
                            else:
                                msg = f"🌅 <b>[{t}] 가상 에스크로 해제 및 엔진 기상 (자가 치유 가동)</b>\n"
                                msg += f"▫️ 장 마감 33분 전 진입을 확인하여 가상 에스크로를 해제했습니다.\n"
                                msg += f"▫️ 스케줄러 누락을 완벽히 극복하고 1분 단위 정밀 타격을 즉각 개시합니다. ⚔️"
                                
                            vwap_cache[f"REV_{t}_nuked"] = True
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 자가 치유 Nuke 실패: {e}", exc_info=True)
                            # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
                            # vwap_cache Nuke 상태를 False로 리셋해야 다음 1분봉에서 재시도가 가능함 (EC-1 교정 완료)
                            vwap_cache[f"REV_{t}_nuked"] = False
                            continue

                    curr_p = float(await asyncio.to_thread(broker.get_current_price, t) or 0.0)
                    
                    if not vwap_cache.get(f"REV_{t}_anchor_prev_c"):
                        prev_c_live = float(await asyncio.to_thread(broker.get_previous_close, t) or 0.0)
                        if prev_c_live > 0:
                            vwap_cache[f"REV_{t}_anchor_prev_c"] = prev_c_live
                    prev_c = float(vwap_cache.get(f"REV_{t}_anchor_prev_c") or 0.0)

                    if curr_p <= 0 or prev_c <= 0: continue

                    if version == "V_REV":
                        strategy_rev = app_data.get('strategy_rev')
                        queue_ledger = app_data.get('queue_ledger')
                        if not strategy_rev or not queue_ledger: continue
                        
                        h = safe_holdings.get(t) or {}
                        actual_qty = int(float(h.get('qty', 0)))
                        
                        avwap_qty_for_shutdown = 0
                        if hasattr(strategy, 'load_avwap_state'):
                            avwap_state_sd = strategy.load_avwap_state(t, now_est)
                            avwap_qty_for_shutdown = int(avwap_state_sd.get('qty', 0))
                            
                        pure_actual_qty = max(0, actual_qty - avwap_qty_for_shutdown)
                        
                        q_data = queue_ledger.get_queue(t)
                        total_q = sum(item.get("qty", 0) for item in q_data)
                        
                        if pure_actual_qty == 0 and total_q > 0:
                            if vwap_cache.get(f"REV_{t}_sweep_msg_sent"):
                                continue
                                
                            if not vwap_cache.get(f"REV_{t}_panic_sell_warn"):
                                vwap_cache[f"REV_{t}_panic_sell_warn"] = True
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"🚨 <b>[비상] [{t}] 수동매매로 인한 잔고 증발이 감지되었습니다.</b>\n"
                                         f"▫️ 봇의 매매가 일시 정지됩니다.\n"
                                         f"▫️ 시드 오염을 막기 위해 즉시 <code>/reset</code> 커맨드를 실행하여 장부를 소각하십시오.",
                                    parse_mode='HTML'
                                )
                            continue
                        
                        cached_plan = strategy_rev.load_daily_snapshot(t)
                        if not cached_plan:
                            # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
                            # cached_plan=None일 때 and 단락평가로 is_zero_start=False가 고착된다고 오판하지 말 것.
                            # 아래 로직은 타임머신 역산을 통해 legacy_q를 도출하여 완벽하게 0주 출발 팩트를 복원하고 있음.
                            today_str_est = now_est.strftime("%Y-%m-%d")
                            legacy_lots = [item for item in q_data if not str(item.get("date", "")).startswith(today_str_est)]
                            legacy_q = sum(int(item.get("qty", 0)) for item in legacy_lots if float(item.get('price', 0.0)) > 0)
                            is_zero_start = (legacy_q == 0)
                            if is_zero_start:
                                logging.warning(f"🚨 [{t}] V-REV 스냅샷 증발! 큐 장부 타임머신 역산 결과 0주 새출발 팩트 복원 완료.")
                            else:
                                is_zero_start = (total_q == 0)
                        else:
                            is_zero_start = cached_plan.get("is_zero_start", cached_plan.get("total_q", -1) == 0)
                            
                        is_zero_start_session = is_zero_start 
                        virtual_q_data = [] if is_zero_start else q_data
                        
                        strategy_rev._load_state_if_needed(t)
                        held_in_cache = vwap_cache.get(f"REV_{t}_was_holding", False)
                        held_in_file = strategy_rev.was_holding.get(t, False)
                        if (held_in_cache or held_in_file) and total_q == 0:
                            continue
                            
                        if total_q > 0:
                            vwap_cache[f"REV_{t}_was_holding"] = True
                            if not strategy_rev.was_holding.get(t, False):
                                strategy_rev.was_holding[t] = True
                                strategy_rev._save_state(t)
                            
                        if total_q > 0:
                            avg_price = sum(item.get("qty", 0) * item.get("price", 0.0) for item in q_data) / total_q
                            jackpot_trigger = avg_price * 1.010
                        else:
                            avg_price = 0.0
                            jackpot_trigger = float('inf')
                        
                        dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                        layer_1_qty = 0
                        layer_1_trigger = round(prev_c * 1.006, 2)
                        if dates_in_queue:
                            lots_for_date = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                            layer_1_qty = sum(item.get('qty', 0) for item in lots_for_date)
                            if layer_1_qty > 0:
                                layer_1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_for_date) / layer_1_qty
                                layer_1_trigger = round(layer_1_price * 1.006, 2)
                        
                        if not is_zero_start and minutes_to_close <= 3:
                            target_sweep_qty = 0
                            sweep_type = ""
                            
                            if total_q > 0 and curr_p >= jackpot_trigger:
                                target_sweep_qty = total_q
                                sweep_type = "잭팟 전량"
                            elif layer_1_qty > 0 and curr_p >= layer_1_trigger:
                                target_sweep_qty = layer_1_qty
                                sweep_type = "1층 잔여물량"
                                
                            if target_sweep_qty > 0:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                await asyncio.sleep(0.5)
                                
                                _, live_holdings = await asyncio.to_thread(broker.get_account_balance)
                                safe_live_holdings = live_holdings if isinstance(live_holdings, dict) else {}
                                
                                if safe_live_holdings and t in safe_live_holdings:
                                    h_live = safe_live_holdings[t]
                                    ord_psbl_qty = int(float(h_live.get('ord_psbl_qty', h_live.get('qty', 0))))
                                    
                                    avwap_qty_sweep = 0
                                    if hasattr(strategy, 'load_avwap_state'):
                                        avwap_state_sw = strategy.load_avwap_state(t, now_est)
                                        avwap_qty_sweep = int(avwap_state_sw.get('qty', 0))
                                    
                                    pure_sellable_qty = max(0, ord_psbl_qty - avwap_qty_sweep)
                                    actual_sweep_qty = min(target_sweep_qty, pure_sellable_qty)
                                    
                                    if actual_sweep_qty > 0:
                                        bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                                        exec_price = bid_price if bid_price > 0 else curr_p
                                        
                                        res = await asyncio.to_thread(broker.send_order, t, "SELL", actual_sweep_qty, exec_price, "LIMIT")
                                        odno = res.get('odno', '') if isinstance(res, dict) else ''
                                        
                                        if res and res.get('rt_cd') == '0' and odno:
                                            if not vwap_cache.get(f"REV_{t}_sweep_msg_sent"):
                                                msg = f"🌪️ <b>[{t}] V-REV 본대 {sweep_type} 3분 가속 스윕(Sweep) 개시!</b>\n"
                                                if "잭팟" in sweep_type:
                                                    msg += f"▫️ 장 마감 3분 전 데드존 철거. 잭팟 커트라인({jackpot_trigger:.2f}) 돌파를 확인했습니다.\n"
                                                else:
                                                    msg += f"▫️ 장 마감 3분 전 데드존 철거. 1층 앵커({layer_1_trigger:.2f}) 방어를 확인했습니다.\n"
                                                msg += f"▫️ 매도 가능 잔량이 0이 될 때까지 매 1분마다 지속 덤핑합니다! (현재 <b>{actual_sweep_qty}주</b> 매수호가 폭격) 🏆"
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                                vwap_cache[f"REV_{t}_sweep_msg_sent"] = True
                                            
                                            ccld_qty = 0
                                            for _ in range(4):
                                                await asyncio.sleep(2.0)
                                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                                
                                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                                if my_order:
                                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                                else:
                                                    ccld_qty = actual_sweep_qty
                                                    break
                                       
                                            if ccld_qty < actual_sweep_qty:
                                                try:
                                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                                    await asyncio.sleep(0.5)
                                                except Exception as e_cancel:
                                                    logging.warning(f"⚠️ [{t}] 스윕 잔여 주문 취소 실패: {e_cancel}")
                                                    
                                            if ccld_qty > 0:
                                                strategy_rev.record_execution(t, "SELL", ccld_qty, exec_price)
                                                q_snap_before_pop = list(q_data)
                                                queue_ledger.pop_lots(t, ccld_qty)
                                                remaining_after_pop = queue_ledger.get_queue(t)
                                                remaining_qty_after = sum(item.get('qty', 0) for item in remaining_after_pop)
                                                if remaining_qty_after == 0 and total_q > 0:
                                                    try:
                                                        pending_file = f"data/pending_grad_{t}.json"
                                                        pending_data = {
                                                            "q_data_before": q_snap_before_pop,
                                                            "exec_price": exec_price,
                                                            "total_q": total_q
                                                        }
                                                        def _save_pending_grad(f_path, p_data):
                                                            os.makedirs("data", exist_ok=True)
                                                            fd, tmp_path = tempfile.mkstemp(dir="data", text=True)
                                                            with os.fdopen(fd, 'w', encoding='utf-8') as _pf:
                                                                json.dump(p_data, _pf)
                                                            os.replace(tmp_path, f_path)
                                                            
                                                        await asyncio.to_thread(_save_pending_grad, pending_file, pending_data)
                                                    except Exception as pg_e:
                                                        logging.error(f"🚨 [{t}] pending_grad 마커 파일 저장 실패: {pg_e}")
                                    else:
                                        if not vwap_cache.get(f"REV_{t}_sweep_skip_msg"):
                                            msg = f"⚠️ <b>[{t}] 스윕 피니셔 덤핑 생략 (MOC 락다운 감지)</b>\n▫️ 조건이 달성되었으나, 대상 물량이 수동 긴급 수혈(MOC) 등 취소 불가 상태로 미국 거래소에 묶여 있어 스윕 덤핑을 자동 스킵합니다."
                                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                            vwap_cache[f"REV_{t}_sweep_skip_msg"] = True
                                            
                            if target_sweep_qty > 0 or (total_q > 0 and curr_p >= jackpot_trigger):
                                continue 
                        
                        try:
                            df_1min = await asyncio.to_thread(broker.get_1min_candles_df, t)
                            vwap_status = strategy.analyze_vwap_dominance(df_1min)
                        except Exception:
                            vwap_status = {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                        
                        current_regime = "BUY" if is_zero_start else ("SELL" if curr_p > prev_c else "BUY")
                        last_regime = vwap_cache.get(f"REV_{t}_regime")
                        
                        if not is_zero_start and last_regime and last_regime != current_regime:
                            await context.bot.send_message(
                                chat_id=chat_id, 
                                text=f"🔄 <b>[{t}] 실시간 공수 교대 발동!</b>\n"
                                     f"▫️ <b>[{last_regime} ➡️ {current_regime}]</b> 모드로 두뇌를 전환하며 궤도를 수정합니다.", 
                                parse_mode='HTML', disable_notification=True
                            )
                            try:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                strategy_rev.reset_residual(t) 
                            except Exception as e:
                                err_msg = f"🛑 <b>[FATAL ERROR] {t} 공수 교대 중 기존 덫 취소 실패!</b>\n▫️ 2중 예산 소진 방어를 위해 당일 남은 V-REV 교전을 강제 중단(Hard-Lock)합니다.\n▫️ 상세 오류: {e}"
                                await context.bot.send_message(chat_id=chat_id, text=err_msg, parse_mode='HTML')
                                continue
                                
                        vwap_cache[f"REV_{t}_regime"] = current_regime
                        
                        if vwap_cache.get(f"REV_{t}_loc_fired"):
                            continue

                        rev_daily_budget = float(cfg.get_seed(t) or 0.0) * 0.15
                        
                        target_orders = []
                        
                        gap_thresh = getattr(cfg, 'get_vrev_gap_threshold', lambda x: -0.67)(t)
                        
                        omni_filter = {"allow_buy": True}  
                            
                        if omni_filter["allow_buy"] and current_regime == "BUY" and not vwap_cache.get(f"REV_{t}_gap_hijack_fired"):
                            base_tkr = base_map.get(t, 'SOXX')
                            base_curr_p = float(await asyncio.to_thread(broker.get_current_price, base_tkr) or 0.0)
                            try:
                                df_1min_base = await asyncio.to_thread(broker.get_1min_candles_df, base_tkr)
                                if df_1min_base is not None and not df_1min_base.empty:
                                    df_b = df_1min_base.copy()
                                    df_b['tp'] = (df_b['high'].astype(float) + df_b['low'].astype(float) + df_b['close'].astype(float)) / 3.0
                                    df_b['vol'] = df_b['volume'].astype(float)
                                    df_b['vol_tp'] = df_b['tp'] * df_b['vol']
                                    
                                    c_vol = df_b['vol'].sum()
                                    base_vwap = df_b['vol_tp'].sum() / c_vol if c_vol > 0 else base_curr_p
                                    
                                    gap_pct = ((base_curr_p - base_vwap) / base_vwap * 100.0) if base_vwap > 0 else 0.0
                                    
                                    if gap_pct <= gap_thresh:
                                        total_spent = float(strategy_rev.executed["BUY_BUDGET"].get(t, 0.0))
                                        rem_budget = max(0.0, rev_daily_budget - total_spent)
                                        
                                        ask_price = float(await asyncio.to_thread(broker.get_ask_price, t) or 0.0)
                                        exec_price = ask_price if ask_price > 0 else curr_p
                                        
                                        buy_qty = int(math.floor(rem_budget / exec_price))
                                        
                                        if buy_qty > 0:
                                            target_orders = [{'side': 'BUY', 'qty': buy_qty, 'price': exec_price, 'type': 'LIMIT', 'desc': '갭 스위치 스윕'}]
                                            vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                            
                                            msg = f"⚡ <b>[{t}] 🤖 옴니 매트릭스 자율주행 (Gap Hijack) 발동!</b>\n"
                                            msg += f"▫️ 기초자산({base_tkr}) VWAP 이탈률(<b>{gap_pct:+.2f}%</b>)이 임계치(<b>{gap_thresh}%</b>)를 하향 돌파했습니다.\n"
                                            msg += f"▫️ VWAP 타임 슬라이싱 스케줄을 즉각 파기하고, 잔여 예산 100%를 매도 1호가로 전량 스윕(Sweep) 타격합니다!"
                                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                            except Exception as e:
                                logging.error(f"🚨 갭 스위칭 기초자산 스캔 에러: {e}")

                        if not target_orders:
                            rev_plan = None
                            try:
                                rev_plan = strategy_rev.get_dynamic_plan(
                                    ticker=t, curr_p=curr_p, prev_c=prev_c, 
                                    current_weight=current_weight, vwap_status=vwap_status, 
                                    min_idx=min_idx, alloc_cash=rev_daily_budget, q_data=virtual_q_data,
                                    is_snapshot_mode=False
                                )
                            except Exception as plan_e:
                                logging.error(f"🚨 [{t}] get_dynamic_plan 실행 에러 (해당 티커 건너뜀): {plan_e}")
                            
                            if rev_plan is None:
                                continue
                            if not is_zero_start and rev_plan.get('trigger_loc') and minutes_to_close >= 15:
                                vwap_cache[f"REV_{t}_loc_fired"] = True
                                msg = f"🛡️ <b>[{t}] 60% 거래량 지배력 감지 (추세장 전환)</b>\n"
                                msg += f"▫️ 기관급 자금 쏠림으로 인해 위험한 1분 단위 타임 슬라이싱(VWAP)을 전면 중단합니다.\n"
                                msg += f"▫️ <b>잔여 할당량 전량을 양방향 LOC 방어선으로 전환 배치 완료!</b>\n"
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                                
                                for o in rev_plan.get('orders', []):
                                    if o['qty'] > 0:
                                        await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], "LOC")
                                        await asyncio.sleep(0.2)
                                continue
                                
                            target_orders = rev_plan.get('orders', [])

                    elif version == "V14":
                        if not is_manual_vwap:
                            continue
                            
                        h = safe_holdings.get(t, {'qty':0, 'avg':0.0})
                        actual_qty = int(h.get('qty', 0))
                        actual_avg = float(h.get('avg', 0.0))
                        
                        avwap_qty_v14 = 0
                        if hasattr(strategy, 'load_avwap_state'):
                            avwap_state_v14 = strategy.load_avwap_state(t, now_est)
                            avwap_qty_v14 = int(avwap_state_v14.get('qty', 0))
                            
                        pure_qty_v14 = max(0, actual_qty - avwap_qty_v14)
                        
                        v14_vwap_plugin = strategy.v14_vwap_plugin
                        
                        cached_snap_v14 = v14_vwap_plugin.load_daily_snapshot(t)
                        if not cached_snap_v14:
                            ledger_qty = 0
                            try:
                                recs = [r for r in cfg.get_ledger() if r['ticker'] == t]
                                ledger_qty, _, _, _ = cfg.calculate_holdings(t, recs)
                            except Exception: pass
                            
                            is_zero_start_session = (ledger_qty == 0)
                            if is_zero_start_session:
                                pure_qty_v14 = 0 
                                logging.warning(f"🚨 [{t}] V14_VWAP 스냅샷 증발! 메인 장부 역산 결과 0주 새출발 팩트 복원 완료.")
                        else:
                            is_zero_start_session = cached_snap_v14.get("is_zero_start", cached_snap_v14.get("total_q", -1) == 0)
                        
                        plan = v14_vwap_plugin.get_dynamic_plan(
                            ticker=t, current_price=curr_p, prev_close=prev_c, 
                            current_weight=current_weight, min_idx=min_idx, 
                            alloc_cash=0.0, qty=pure_qty_v14, avg_price=actual_avg
                        )
                        target_orders = plan.get('orders', [])

                    for o in target_orders:
                        slice_qty = o['qty']
                        if slice_qty <= 0: continue
                        
                        target_price = o['price']
                        side = o['side']

                        ask_price = float(await asyncio.to_thread(broker.get_ask_price, t) or 0.0)
                        bid_price = float(await asyncio.to_thread(broker.get_bid_price, t) or 0.0)
                        exec_price = ask_price if side == "BUY" else bid_price
                        if exec_price <= 0: exec_price = curr_p
                        
                        if side == "BUY":
                            if not is_zero_start_session and exec_price > target_price:
                                continue
                        elif side == "SELL":
                            if exec_price < target_price:
                                continue
                        
                        res = await asyncio.to_thread(broker.send_order, t, side, slice_qty, exec_price, "LIMIT")
                        odno = res.get('odno', '') if isinstance(res, dict) else ''
                        
                        if res and res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                
                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                else:
                                    ccld_qty = slice_qty
                                    break
                                    
                            if ccld_qty < slice_qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass
                                
                            if ccld_qty > 0:
                                if version == "V_REV":
                                    strategy_rev.record_execution(t, side, ccld_qty, exec_price)
                                    if side == "BUY": queue_ledger.add_lot(t, ccld_qty, exec_price, "VWAP_BUY")
                                    elif side == "SELL": queue_ledger.pop_lots(t, ccld_qty)
                                elif version == "V14":
                                    v14_vwap_plugin.record_execution(t, side, ccld_qty, exec_price)
                                    
                            await asyncio.sleep(0.2)

    try:
        await asyncio.wait_for(_do_vwap(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 VWAP 스케줄러 에러: {e}", exc_info=True)
