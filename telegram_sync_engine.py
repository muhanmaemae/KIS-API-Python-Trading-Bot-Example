# ==========================================================
# FILE: telegram_sync_engine.py
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: queue_ledger.get_queue 등 모든 파일 I/O 및 락 점유 메서드는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# MODIFIED: [V44.47 이벤트 루프 데드락 영구 소각] 동기식 블로킹 호출(장부 I/O, JSON 파싱) 전면 비동기 래핑 및 Atomic Write 적용 완료.
# MODIFIED: [V44.48 데드코드 소각] 클래스 내부에 잔존하는 _verify_and_update_queue 메서드 전체 100% 영구 소각.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import time
import os
import asyncio
import json
import tempfile
import traceback
import yfinance as yf
import pandas_market_calendars as mcal
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class TelegramSyncEngine:
    def __init__(self, config, broker, strategy, queue_ledger, view, tx_lock, sync_locks):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.view = view
        self.tx_lock = tx_lock
        self.sync_locks = sync_locks

    # 🚨 [비동기 래핑] 파일 I/O 데드락 방어를 위한 async 전환
    async def _sync_escrow_cash(self, ticker):
        rev_state = await asyncio.to_thread(self.cfg.get_reverse_state, ticker)
        is_rev = rev_state.get("is_active", False)
        if not is_rev:
            await asyncio.to_thread(self.cfg.clear_escrow_cash, ticker)
            return

        ledger = await asyncio.to_thread(self.cfg.get_ledger)
        
        target_recs = []
        for r in reversed(ledger):
            if r.get('ticker') == ticker:
                if r.get('is_reverse', False):
                    target_recs.append(r)
                else:
                    break
        
        escrow = 0.0
        for r in target_recs:
            amt = r['qty'] * r['price']
            if r['side'] == 'SELL':
                escrow += amt
            elif r['side'] == 'BUY':
                escrow -= amt
                
        await asyncio.to_thread(self.cfg.set_escrow_cash, ticker, max(0.0, escrow))

    async def process_auto_sync(self, ticker, chat_id, context, silent_ledger=False):
        if ticker not in self.sync_locks:
            self.sync_locks[ticker] = asyncio.Lock()
            
        if self.sync_locks[ticker].locked(): 
            return "LOCKED"
            
        async with self.sync_locks[ticker]:
            async with self.tx_lock:
                
                last_split_date = await asyncio.to_thread(self.cfg.get_last_split_date, ticker)
                
                try:
                    split_ratio, split_date = await asyncio.wait_for(
                        asyncio.to_thread(self.broker.get_recent_stock_split, ticker, last_split_date),
                        timeout=10.0
                    )
                except asyncio.TimeoutError:
                    split_ratio, split_date = 0.0, ""
                    logging.warning(f"⚠️ [{ticker}] 야후 파이낸스 액면분할 조회 타임아웃 (10초 초과), 이번 싱크에서 스킵")
                
                if split_ratio > 0.0 and split_date != "":
                    await asyncio.to_thread(self.cfg.apply_stock_split, ticker, split_ratio)
                    await asyncio.to_thread(self.cfg.set_last_split_date, ticker, split_date)
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await context.bot.send_message(chat_id, f"✂️ <b>[{ticker}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {split_date})\n▫️ 봇이 기존 장부의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
                
                kst = ZoneInfo('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)
                
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                
                def _get_last_trade_date():
                    nyse = mcal.get_calendar('NYSE')
                    schedule = nyse.schedule(start_date=(now_est - datetime.timedelta(days=10)).date(), end_date=now_est.date())
                    return schedule

                try:
                    schedule = await asyncio.wait_for(asyncio.to_thread(_get_last_trade_date), timeout=10.0)
                    if not schedule.empty:
                        last_trade_date = schedule.index[-1]
                        target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                    else:
                        target_ledger_str = now_est.strftime('%Y-%m-%d')
                except Exception as e:
                    logging.error(f"⚠️ [{ticker}] 달력 API 에러/타임아웃. Fallback으로 현재 날짜 세팅: {e}")
                    target_ledger_str = now_est.strftime('%Y-%m-%d')

                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                if holdings is None:
                    await context.bot.send_message(chat_id, f"❌ <b>[{ticker}] API 오류</b>\n잔고를 불러오지 못했습니다.", parse_mode='HTML')
                    return "ERROR"

                actual_qty = int(float(holdings.get(ticker, {'qty': 0}).get('qty') or 0))
                actual_avg = float(holdings.get(ticker, {'avg': 0}).get('avg') or 0.0)

                # 🚨 [비동기 래핑] 장부 파일 I/O 데드락 방어
                full_ledger = await asyncio.to_thread(self.cfg.get_ledger)
                recs_for_check = [r for r in full_ledger if r['ticker'] == ticker]
                ledger_qty_for_check, _, _, _ = await asyncio.to_thread(self.cfg.calculate_holdings, ticker, recs_for_check)
                
                vrev_ledger_qty_for_check = 0
                is_rev = (await asyncio.to_thread(self.cfg.get_version, ticker) == "V_REV")
                
                if is_rev:
                    if not getattr(self, 'queue_ledger', None):
                        from queue_ledger import QueueLedger
                        self.queue_ledger = QueueLedger()
                    
                    q_data_check = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                    vrev_ledger_qty_for_check = sum(int(float(item.get("qty") or 0)) for item in q_data_check)
                
                max_check_qty = max(ledger_qty_for_check, vrev_ledger_qty_for_check)

                kis_search_start = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                query_end_dt = now_kst.strftime('%Y%m%d')

                def filter_to_est(execs_raw):
                    filtered = []
                    if not execs_raw: return filtered
                    for ex in execs_raw:
                        ord_dt = ex.get('ord_dt') or ex.get('ord_strt_dt')
                        if not ord_dt: continue
                        ord_tmd = ex.get('ord_tmd')
                        if not ord_tmd or len(str(ord_tmd)) != 6: 
                            ord_tmd = '000000'
                        try:
                            k_dt = datetime.datetime.strptime(f"{ord_dt}{ord_tmd}", "%Y%m%d%H%M%S").replace(tzinfo=kst)
                            e_dt = k_dt.astimezone(est)
                            if e_dt.strftime('%Y-%m-%d') == target_ledger_str:
                                filtered.append(ex)
                        except Exception as e:
                            logging.error(f"🚨 타임존 파싱 에러: {e}")
                    return filtered

                raw_execs = []
                target_execs = []
                
                if actual_qty == 0 and max_check_qty > 0:
                    max_retries = 6
                    prev_sold_today = -1
                    stable_cnt = 0
                    for attempt in range(max_retries):
                        raw_execs = await asyncio.to_thread(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt)
                        target_execs = filter_to_est(raw_execs)
                        sold_today = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                        
                        if sold_today >= max_check_qty:
                            if sold_today == prev_sold_today:
                                stable_cnt += 1
                                if stable_cnt >= 1: 
                                    break
                            else:
                                stable_cnt = 0
                        prev_sold_today = sold_today
                        
                        if attempt < max_retries - 1:
                            logging.info(f"⏳ [{ticker}] 체결 원장 지연(Lag) 감지. 데이터 안정화 및 EST 매핑 검증 중... ({attempt+1}/{max_retries})")
                            await asyncio.sleep(2.0)
                else:
                    raw_execs = await asyncio.to_thread(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt)
                    target_execs = filter_to_est(raw_execs)

                if target_execs:
                    calibrated_count = await asyncio.to_thread(self.cfg.calibrate_ledger_prices, ticker, target_ledger_str, target_execs)
                    if calibrated_count > 0:
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                full_ledger = await asyncio.to_thread(self.cfg.get_ledger)
                recs = [r for r in full_ledger if r['ticker'] == ticker]
                ledger_qty, avg_price, _, _ = await asyncio.to_thread(self.cfg.calculate_holdings, ticker, recs)
                
                diff = actual_qty - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                today_recs = [r for r in recs if r['date'] == target_ledger_str and 'INIT' not in str(r.get('exec_id', '')) and 'CALIB' not in str(r.get('exec_id', ''))]
                ledger_today_buy = sum(r['qty'] for r in today_recs if r['side'] == 'BUY')
                ledger_today_sell = sum(r['qty'] for r in today_recs if r['side'] == 'SELL')
                
                exec_today_buy = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "02")
                exec_today_sell = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                
                needs_reconstruction = (diff != 0) or (ledger_today_buy != exec_today_buy) or (ledger_today_sell != exec_today_sell)

                if not needs_reconstruction and price_diff < 0.01:
                    pass 
                elif not needs_reconstruction and price_diff >= 0.01:
                    await asyncio.to_thread(self.cfg.calibrate_avg_price, ticker, actual_avg)
                    await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                elif needs_reconstruction:
                    temp_recs = [r for r in recs if r['date'] != target_ledger_str or 'INIT' in str(r.get('exec_id', ''))]
                    temp_qty, temp_avg, _, _ = await asyncio.to_thread(self.cfg.calculate_holdings, ticker, temp_recs)
                    
                    temp_sim_qty = temp_qty
                    temp_sim_avg = temp_avg
                    new_target_records = []
                    
                    if target_execs:
                        target_execs.sort(key=lambda x: str(x.get('ord_dt', '00000000')) + str(x.get('ord_tmd', '000000'))) 
                        for ex in target_execs:
                            side_cd = ex.get('sll_buy_dvsn_cd')
                            exec_qty = int(float(ex.get('ft_ccld_qty') or '0'))
                            exec_price = float(ex.get('ft_ccld_unpr3') or '0')
                            
                            if side_cd == "02": 
                                new_avg = ((temp_sim_qty * temp_sim_avg) + (exec_qty * exec_price)) / (temp_sim_qty + exec_qty) if (temp_sim_qty + exec_qty) > 0 else exec_price
                                temp_sim_qty += exec_qty
                                temp_sim_avg = new_avg
                            else:
                                temp_sim_qty -= exec_qty
                                
                            rec_item = {
                                'date': target_ledger_str, 'side': "BUY" if side_cd == "02" else "SELL",
                                'qty': exec_qty, 'price': exec_price, 'avg_price': temp_sim_avg
                            }
                            if is_rev:
                                rec_item['is_reverse'] = True
                            new_target_records.append(rec_item)
                            
                    gap_qty = actual_qty - temp_sim_qty
                    if gap_qty != 0:
                        calib_side = "BUY" if gap_qty > 0 else "SELL"
                        
                        calib_price = actual_avg
                        if calib_side == "SELL" and actual_avg <= 0.0:
                            actual_clear_price_calib = 0.0
                            if target_execs:
                                sell_execs_calib = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs_calib:
                                    tot_amt_calib = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in sell_execs_calib)
                                    tot_q_calib = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in sell_execs_calib)
                                    if tot_q_calib > 0:
                                        actual_clear_price_calib = round(tot_amt_calib / tot_q_calib, 4)
                            
                            if actual_clear_price_calib == 0.0 and raw_execs:
                                recent_sells = [ex for ex in raw_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if recent_sells:
                                    recent_sells.sort(key=lambda x: f"{x.get('ord_dt', '')}{x.get('ord_tmd', '')}", reverse=True)
                                    last_sell_dt = recent_sells[0].get('ord_dt')
                                    same_day_sells = [ex for ex in recent_sells if ex.get('ord_dt') == last_sell_dt]
                                    tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in same_day_sells)
                                    tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in same_day_sells)
                                    if tot_q > 0:
                                        actual_clear_price_calib = round(tot_amt / tot_q, 4)
                            
                            if actual_clear_price_calib > 0.0:
                                calib_price = actual_clear_price_calib
                                logging.info(f"🛡️ [{ticker}] CALIB_SELL 0달러 역산 방어: 당일/최근 체결 원장의 실제 매도 평균가(${calib_price:.4f})를 팩트 주입했습니다.")
                            else:
                                calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                                logging.info(f"🛡️ [{ticker}] CALIB_SELL 0달러 폴백 방어: 원장 결측으로 기존 장부 평단가(${calib_price:.4f})를 강제 주입했습니다.")
                                
                            calib_avg = temp_sim_avg
                        elif calib_side == "BUY" and actual_avg <= 0.0:
                            calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                            calib_avg = temp_sim_avg
                            logging.info(f"🛡️ [{ticker}] CALIB_BUY 0달러 폴백 방어: 기존 장부 평단가(${calib_price:.4f})를 강제 주입했습니다.")
                        else:
                            calib_price = actual_avg if actual_avg > 0 else temp_sim_avg
                            calib_avg = actual_avg if actual_avg > 0 else temp_sim_avg
                            
                        calib_item = {
                            'date': target_ledger_str, 
                            'side': calib_side,
                            'qty': abs(gap_qty), 
                            'price': calib_price, 
                            'avg_price': calib_avg,
                            'exec_id': f"CALIB_{int(time.time())}",
                            'desc': "비파괴 보정"
                        }
                        
                        if is_rev:
                            calib_item['is_reverse'] = True
                        new_target_records.append(calib_item)
                        
                    if new_target_records:
                        if actual_qty > 0:
                            for r in new_target_records:
                                r['avg_price'] = actual_avg
                    elif temp_recs: 
                        if actual_qty > 0:
                            temp_recs[-1]['avg_price'] = actual_avg
                        
                    await asyncio.to_thread(self.cfg.overwrite_incremental_ledger, ticker, temp_recs, new_target_records)
                    
                    if gap_qty != 0:
                        await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 통합 메인 장부(MAIN LEDGER) 비파괴 보정 완료!</b>\n▫️ KIS 실잔고 오차 수량({gap_qty}주)을 역사 보존 상태로 안전하게 교정했습니다.", parse_mode='HTML')
                    elif exec_today_buy > 0 or exec_today_sell > 0:
                        logging.info(f"📜 [{ticker}] 당일 데이트레이딩 체결 원장(제로섬 회귀)이 메인 장부에 완벽히 복원 기입되었습니다.")

                # ==========================================================
                # V-REV 큐 관리 및 0주 졸업 판별 로직 시작
                # ==========================================================
                if is_rev:
                    q_data_before = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                    vrev_ledger_qty = sum(int(float(item.get("qty") or 0)) for item in q_data_before)
                    
                    sold_today_vrev = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    
                    avwap_qty_global = 0
                    tracking_cache_global = None
                    try:
                        jobs = context.job_queue.jobs() if context.job_queue else []
                        job_data = jobs[0].data if jobs and len(jobs) > 0 and jobs[0].data is not None else {}
                        tracking_cache_global = job_data.get('sniper_tracking', {})
                        avwap_qty_global = tracking_cache_global.get(f"AVWAP_QTY_{ticker}", 0)
                        
                        if avwap_qty_global == 0:
                            strategy = job_data.get('strategy')
                            if strategy and hasattr(strategy, 'v_avwap_plugin'):
                                avwap_state = await asyncio.to_thread(strategy.v_avwap_plugin.load_state, ticker, now_est)
                                avwap_qty_global = int(avwap_state.get('qty', 0))
                    except Exception:
                        pass
                    
                    adjusted_actual_qty = max(0, actual_qty - avwap_qty_global)
                    
                    if adjusted_actual_qty == 0 and (vrev_ledger_qty > 0 or sold_today_vrev > 0):
                        if actual_qty == 0 and avwap_qty_global > 0:
                            logging.warning(f"🚨 [{ticker}] V-REV 0주 졸업 판별 확정! 그러나 AVWAP 유령 물량({avwap_qty_global}주) 감지. 즉각 100% 영구 소각(Format)합니다.")
                            try:
                                if tracking_cache_global is not None:
                                    tracking_cache_global[f"AVWAP_QTY_{ticker}"] = 0
                                    tracking_cache_global[f"AVWAP_AVG_{ticker}"] = 0.0
                                    tracking_cache_global[f"AVWAP_BOUGHT_{ticker}"] = False
                                    tracking_cache_global[f"AVWAP_SHUTDOWN_{ticker}"] = True
                                
                                strategy = job_data.get('strategy')
                                if strategy and hasattr(strategy, 'v_avwap_plugin'):
                                    state_data = {
                                        'bought': False,
                                        'shutdown': True,
                                        'qty': 0,
                                        'avg_price': 0.0,
                                        'strikes': tracking_cache_global.get(f"AVWAP_STRIKES_{ticker}", 0) if tracking_cache_global is not None else 0
                                    }
                                    await asyncio.to_thread(strategy.v_avwap_plugin.save_state, ticker, now_est, state_data)
                            except Exception as e:
                                logging.error(f"🚨 [{ticker}] AVWAP 환각 소각 중 예외 발생: {e}")

                        if now_kst.hour < 10:
                            await context.bot.send_message(chat_id, "⏳ <b>증권사 확정 정산(10:00 KST) 대기 중입니다.</b> 가결제 오차 방지를 위해 졸업 카드 발급 및 장부 초기화가 보류됩니다.", parse_mode='HTML')
                            await self._sync_escrow_cash(ticker)
                            return "SUCCESS"

                        added_seed = 0.0
                        _vrev_snap_ok = False
                        snapshot = None
                        try:
                            actual_clear_price = 0.0
                            tot_q = 0
                            
                            if target_execs:
                                sell_execs = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs:
                                    tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in sell_execs)
                                    tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in sell_execs)
                                    if tot_q > 0:
                                        actual_clear_price = round(tot_amt / tot_q, 4)
                            
                            if actual_clear_price == 0.0:
                                if raw_execs:
                                    recent_sells = [ex for ex in raw_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                    if recent_sells:
                                        recent_sells.sort(key=lambda x: f"{x.get('ord_dt', '')}{x.get('ord_tmd', '')}", reverse=True)
                                        last_sell_dt = recent_sells[0].get('ord_dt')
                                        same_day_sells = [ex for ex in recent_sells if ex.get('ord_dt') == last_sell_dt]
                                        tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in same_day_sells)
                                        tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in same_day_sells)
                                        if tot_q > 0:
                                            actual_clear_price = round(tot_amt / tot_q, 4)
                        
                            logging.info(f"🔍 [{ticker}] 과거 4일치 광역 스캔 및 최근일({last_sell_dt}) 추출 폴백으로 매도 단가(${actual_clear_price})를 복원했습니다.")

                            if tot_q > vrev_ledger_qty:
                                missing_qty = tot_q - vrev_ledger_qty
                                buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                                
                                temp_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                                temp_avg = temp_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0
                                missing_price = temp_avg
                            
                                if buy_execs:
                                    b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in buy_execs)
                                    b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in buy_execs)
                                    
                                    if b_tot_q > 0:
                                        q_today_amt = 0.0
                                        q_today_qty = 0
                                        for item in q_data_before:
                                            if str(item.get("date", "")).startswith(target_ledger_str):
                                                iq = int(float(item.get("qty", 0)))
                                                q_today_qty += iq
                                                q_today_amt += iq * float(item.get("price", 0))
                                                
                                        pure_manual_q = b_tot_q - q_today_qty
                                        pure_manual_amt = b_tot_amt - q_today_amt
                                        
                                        if pure_manual_q >= missing_qty and pure_manual_q > 0 and pure_manual_amt > 0:
                                            derived_price = pure_manual_amt / pure_manual_q
                                            missing_price = round(derived_price, 4)
                                        else:
                                            missing_price = round(b_tot_amt / b_tot_q, 4)
                                            
                                q_data_before.append({
                                    "date": now_est.strftime('%Y-%m-%d %H:%M:%S'),
                                    "qty": missing_qty,
                                    "price": missing_price,
                                    "exec_id": "MANUAL_SYNC"
                                })
                                vrev_ledger_qty = tot_q
                                
                                q_file = "data/queue_ledger.json"
                                try:
                                    def _read_all_q(f_path):
                                        if os.path.exists(f_path):
                                            with open(f_path, 'r', encoding='utf-8') as f:
                                                return json.load(f)
                                        return {}
                                    
                                    all_q = await asyncio.to_thread(_read_all_q, q_file)
                                    all_q[ticker] = q_data_before
                                    
                                    def _write_q_file(q_data_dict, file_path):
                                        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
                                        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(file_path) if os.path.dirname(file_path) else '.')
                                        with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                            json.dump(q_data_dict, f_out, indent=4, ensure_ascii=False)
                                            f_out.flush()
                                            os.fsync(f_out.fileno())
                                        os.replace(tmp_path, file_path)
                                        
                                    await asyncio.to_thread(_write_q_file, all_q, q_file)
                                    
                                    if hasattr(self.queue_ledger, 'data'):
                                        self.queue_ledger.data = all_q
                                    if hasattr(self.queue_ledger, 'queues'):
                                        self.queue_ledger.queues = all_q
                                    if hasattr(self.queue_ledger, 'load'):
                                        await asyncio.to_thread(self.queue_ledger.load)
                                         
                                    logging.info(f"🔧 [{ticker}] 미동기화 수동 매수 물량({missing_qty}주, 진성단가 ${missing_price})을 졸업 큐에 다이렉트 영속화하여 PnL 오차 교정 및 스냅샷 충돌 방어 완료.")
                                except Exception as e:
                                    logging.error(f"🚨 MANUAL_SYNC LIFO 큐 파일 I/O 영속화 실패: {e}")

                            total_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                            q_avg_price = total_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0

                            try:
                                curr_p = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, ticker), timeout=10.0)
                            except asyncio.TimeoutError:
                                curr_p = 0.0
                                logging.warning(f"⚠️ [{ticker}] 현재가 조회 타임아웃 (10초), 스냅샷 보정용 가격에서 제외")
                            
                            clear_price = actual_clear_price if actual_clear_price > 0.0 else (curr_p if curr_p and curr_p > 0 else q_avg_price * 1.006)
                            
                            snapshot = await asyncio.to_thread(self.strategy.capture_vrev_snapshot, ticker, clear_price, q_avg_price, vrev_ledger_qty)
                            
                            if snapshot:
                                realized_pnl = snapshot['realized_pnl']
                                yield_pct = snapshot['realized_pnl_pct']
                                
                                compound_rate = float(await asyncio.to_thread(self.cfg.get_compound_rate, ticker)) / 100.0
                                if realized_pnl > 0 and compound_rate > 0:
                                    added_seed = realized_pnl * compound_rate
                                    current_seed = await asyncio.to_thread(self.cfg.get_seed, ticker)
                                    await asyncio.to_thread(self.cfg.set_seed, ticker, current_seed + added_seed)
                                
                                cap_dt = snapshot['captured_at']
                                cap_dt_str = cap_dt if isinstance(cap_dt, str) else cap_dt.strftime('%Y-%m-%d')
                                start_dt_str = q_data_before[0]['date'][:10] if q_data_before else cap_dt_str[:10]
                                
                                hist_data = await asyncio.to_thread(self.cfg._load_json, self.cfg.FILES["HISTORY"], [])
                                new_hist = {
                                    "id": int(time.time()),
                                    "ticker": ticker,
                                    "start_date": start_dt_str,
                                    "end_date": cap_dt_str[:10],
                                    "invested": total_invested,
                                    "revenue": total_invested + realized_pnl,
                                    "profit": realized_pnl,
                                    "yield": yield_pct,
                                    "trades": q_data_before 
                                }
                                hist_data.append(new_hist)
                                
                                await asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["HISTORY"], hist_data)
                                _vrev_snap_ok = True
                                
                        except Exception as e:
                            logging.error(f"🚨 스냅샷 캡처 및 복리 정산 중 치명적 오류 감지: {e}\n{traceback.format_exc()}")
                            snapshot = None
                            
                        await asyncio.to_thread(self.queue_ledger.sync_with_broker, ticker, 0)
                        
                        if _vrev_snap_ok:
                            msg = f"🎉 <b>[{ticker} V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                            if added_seed > 0:
                                msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                            
                            if snapshot:
                                try:
                                    img_path = await asyncio.to_thread(
                                        self.view.create_profit_image,
                                        ticker=ticker, profit=snapshot['realized_pnl'], 
                                        yield_pct=snapshot['realized_pnl_pct'],
                                        invested=snapshot['avg_price'] * snapshot['cleared_qty'], 
                                        revenue=snapshot['clear_price'] * snapshot['cleared_qty'], 
                                        end_date=cap_dt_str[:10]
                                    )
                                    if img_path and os.path.exists(img_path):
                                        with open(img_path, 'rb') as f_out:
                                            if img_path.lower().endswith('.gif'):
                                                await context.bot.send_animation(chat_id=chat_id, animation=f_out)
                                            else:
                                                await context.bot.send_photo(chat_id=chat_id, photo=f_out)
                                except Exception as e:
                                    logging.error(f"📸 V-REV 스냅샷 이미지 렌더링/발송 실패: {e}")
                        else:
                            await context.bot.send_message(chat_id, f"⚠️ <b>[{ticker} V-REV 0주 강제 정산 완료]</b>\n▫️ 0주를 확인하여 큐를 안전하게 비웠으나 통신 지연으로 졸업 카드는 생략되었습니다.", parse_mode='HTML')
                                    
                        await self._sync_escrow_cash(ticker)
                        return "SUCCESS"
                        
                    if adjusted_actual_qty == vrev_ledger_qty:
                        pass
                    else:
                        if adjusted_actual_qty > 0 and adjusted_actual_qty < vrev_ledger_qty:
                            gap_qty = vrev_ledger_qty - adjusted_actual_qty
                            
                            vwap_state_file = f"data/vwap_state_REV_{ticker}.json"
                            if os.path.exists(vwap_state_file):
                                try:
                                    def _read_v_state(f_path):
                                        with open(f_path, 'r', encoding='utf-8') as vf:
                                            return json.load(vf)
                                    
                                    v_state = await asyncio.to_thread(_read_v_state, vwap_state_file)
                                    if "executed" in v_state and "SELL_QTY" in v_state["executed"]:
                                        old_sell_qty = v_state["executed"]["SELL_QTY"]
                                        v_state["executed"]["SELL_QTY"] = max(0, old_sell_qty - gap_qty)
                                        
                                        def _write_v_state(state_dict, f_path):
                                            fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(f_path) or '.')
                                            with os.fdopen(fd, 'w', encoding='utf-8') as _vf_out:
                                                json.dump(state_dict, _vf_out, ensure_ascii=False, indent=4)
                                                _vf_out.flush()
                                                os.fsync(_vf_out.fileno())
                                            os.replace(tmp_path, f_path)
                                                
                                        await asyncio.to_thread(_write_v_state, v_state, vwap_state_file)
                                        logging.info(f"🔧 [{ticker}] VWAP 잔차 수학적 보정 완료: {old_sell_qty} -> {v_state['executed']['SELL_QTY']}")
                                except Exception as e:
                                    logging.error(f"🚨 VWAP 상태 교정 에러: {e}")

                            calibrated = await asyncio.to_thread(self.queue_ledger.sync_with_broker, ticker, adjusted_actual_qty, actual_avg)
                            if calibrated:
                                await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] V-REV 큐(Queue) 비파괴 보정 완료!</b>\n▫️ 수동 매도 물량(<b>{gap_qty}주</b>)을 LIFO 큐에서 안전하게 차감했습니다.", parse_mode='HTML')
                            
                        elif adjusted_actual_qty > 0 and adjusted_actual_qty > vrev_ledger_qty:
                            gap_qty = adjusted_actual_qty - vrev_ledger_qty
                            
                            real_buy_price = actual_avg
                            try:
                                buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                                if buy_execs:
                                    b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in buy_execs)
                                    b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in buy_execs)
                                    if b_tot_q > 0:
                                        real_buy_price = round(b_tot_amt / b_tot_q, 4)
                                
                                if real_buy_price == actual_avg:
                                    search_start_dt = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                                    past_raw = await asyncio.to_thread(self.broker.get_execution_history, ticker, search_start_dt, query_end_dt)
                                    past_execs = filter_to_est(past_raw)
                                    if past_execs:
                                        p_buy_execs = [ex for ex in past_execs if ex.get('sll_buy_dvsn_cd') == "02"]
                                        if p_buy_execs:
                                            b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in p_buy_execs)
                                            b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in p_buy_execs)
                                            if b_tot_q > 0:
                                                real_buy_price = round(b_tot_amt / b_tot_q, 4)
                            except Exception as e:
                                logging.error(f"🚨 수동매수 실제 체결단가 역산 중 예외 발생 (기존 평단가 fallback): {e}")

                            if real_buy_price == actual_avg:
                                old_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                                new_invested = adjusted_actual_qty * actual_avg
                                if new_invested > old_invested:
                                    derived_price = (new_invested - old_invested) / gap_qty
                                    real_buy_price = round(derived_price, 4) if derived_price > 0 else actual_avg
                            
                            q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                            q_data.append({
                                "date": now_est.strftime('%Y-%m-%d %H:%M:%S'),
                                "qty": gap_qty,
                                "price": real_buy_price,
                                "exec_id": f"MANUAL_BUY_{int(time.time())}"
                            })
                            
                            q_file = "data/queue_ledger.json"
                            try:
                                def _read_all_q_manual(f_path):
                                    if os.path.exists(f_path):
                                        with open(f_path, 'r', encoding='utf-8') as f:
                                            return json.load(f)
                                    return {}
                                
                                all_q = await asyncio.to_thread(_read_all_q_manual, q_file)
                                all_q[ticker] = q_data
                                
                                def _write_q_manual(q_dict, file_path):
                                    os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
                                    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(file_path) if os.path.dirname(file_path) else '.')
                                    with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                        json.dump(q_dict, f_out, indent=4, ensure_ascii=False)
                                        f_out.flush()
                                        os.fsync(f_out.fileno())
                                    os.replace(tmp_path, file_path)
                                    
                                await asyncio.to_thread(_write_q_manual, all_q, q_file)
                                
                                if hasattr(self.queue_ledger, 'data'):
                                    self.queue_ledger.data = all_q
                                     
                                logging.info(f"🔧 [{ticker}] 수동 매수 감지! KIS 실잔고에 맞춰 LIFO 큐에 신규 지층({gap_qty}주, 진성단가 ${real_buy_price}) 다이렉트 편입 및 파일 영속화 완료.")
                                await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] V-REV 큐(Queue) 수동 매수 편입 완료!</b>\n▫️ KIS 실잔고에 맞춰 신규 지층(<b>{gap_qty}주</b>, 추정단가 ${real_buy_price})을 정밀 추가했습니다.", parse_mode='HTML')
                            except Exception as e:
                                logging.error(f"🚨 LIFO 큐 다이렉트 파일 I/O 쓰기 에러: {e}")
                    
                    await self._sync_escrow_cash(ticker)
                    return "SUCCESS"

                # ==========================================================
                # V14 0주 졸업 판별 로직 
                # ==========================================================
                if not is_rev:
                    sold_today_v14 = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    if actual_qty == 0 and (ledger_qty > 0 or sold_today_v14 > 0):
                        if now_kst.hour < 10:
                            await context.bot.send_message(chat_id, "⏳ <b>증권사 확정 정산(10:00 KST) 대기 중입니다.</b> 가결제 오차 방지를 위해 졸업 카드 발급 및 장부 초기화가 보류됩니다.", parse_mode='HTML')
                        else:
                            today_est_str = now_est.strftime('%Y-%m-%d')
                            
                            try:
                                prev_c = await asyncio.wait_for(
                                    asyncio.to_thread(self.broker.get_previous_close, ticker),
                                    timeout=10.0
                                )
                            except asyncio.TimeoutError:
                                prev_c = 0.0
                                logging.warning(f"⚠️ [{ticker}] 야후 파이낸스 전일 종가 조회 타임아웃 (10초). 0.0으로 대체")
                            
                            try:
                                new_hist, added_seed = await asyncio.to_thread(self.cfg.archive_graduation, ticker, today_est_str, prev_c)

                                if new_hist:
                                    msg = f"🎉 <b>[{ticker} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                                    if added_seed > 0:
                                        msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                                    await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                                    
                                    try:
                                        img_path = await asyncio.to_thread(
                                            self.view.create_profit_image,
                                            ticker=ticker, profit=new_hist['profit'], yield_pct=new_hist['yield'],
                                            invested=new_hist['invested'], revenue=new_hist['revenue'], end_date=new_hist['end_date']
                                        )
                                        if img_path and os.path.exists(img_path):
                                            with open(img_path, 'rb') as f_out:
                                                if img_path.lower().endswith('.gif'):
                                                    await context.bot.send_animation(chat_id=chat_id, animation=f_out)
                                                else:
                                                    await context.bot.send_photo(chat_id=chat_id, photo=f_out)
                                    except Exception as e:
                                        logging.error(f"📸 졸업 이미지 발송 실패: {e}")
                                else:
                                    full_ledger2 = await asyncio.to_thread(self.cfg.get_ledger)
                                    all_recs = [r for r in full_ledger2 if r['ticker'] != ticker]
                                    await asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["LEDGER"], all_recs)
                                    await context.bot.send_message(chat_id, f"⚠️ <b>[{ticker} 강제 정산 완료]</b>\n잔고가 0주이나 마이너스 수익 상태이므로 명예의 전당 박제 없이 장부를 비우고 새출발 타점을 장전합니다.", parse_mode='HTML')
                            except Exception as e:
                                logging.error(f"강제 졸업 처리 중 에러: {e}")

                    await self._sync_escrow_cash(ticker) 
                    return "SUCCESS"

                await self._sync_escrow_cash(ticker)
                return "SUCCESS"

    async def _display_ledger(self, ticker, chat_id, context, query=None, message_obj=None, pre_fetched_holdings=None):
        full_ledger = await asyncio.to_thread(self.cfg.get_ledger)
        recs = [r for r in full_ledger if r['ticker'] == ticker]
        
        if not recs:
            msg = f"📭 <b>[{ticker}]</b> 현재 진행 중인 사이클이 없습니다 (보유량 0주)."
        else:
            from collections import OrderedDict
            agg_dict = OrderedDict()
            total_buy = 0.0
            total_sell = 0.0
            
            for rec in recs:
                parts = rec['date'].split('-')
                if len(parts) == 3:
                    date_short = f"{parts[1]}.{parts[2]}"
                else:
                    date_short = rec['date']
                    
                side_str = "🔴매수" if rec['side'] == 'BUY' else "🔵매도"
                key = (date_short, side_str)
                
                if key not in agg_dict:
                    agg_dict[key] = {'qty': 0, 'amt': 0.0}
                    
                agg_dict[key]['qty'] += rec['qty']
                agg_dict[key]['amt'] += (rec['qty'] * rec['price'])
                
                if rec['side'] == 'BUY':
                    total_buy += (rec['qty'] * rec['price'])
                elif rec['side'] == 'SELL':
                    total_sell += (rec['qty'] * rec['price'])
            
            report = f"📜 <b>[ {ticker} 일자별 매매 (통합 변동분) (총 {len(agg_dict)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n"
            report += "-"*30 + "\n"
            
            idx = 1
            for (date, side), data in agg_dict.items():
                tot_qty = data['qty']
                avg_prc = data['amt'] / tot_qty if tot_qty > 0 else 0.0
                report += f"{idx:<3} {date} {side} ${avg_prc:<6.2f} {tot_qty}주\n"
                idx += 1
                
            report += "-"*30 + "</code>\n"
            
            actual_qty = int(float(pre_fetched_holdings.get(ticker, {'qty': 0})['qty'] or 0)) if pre_fetched_holdings else 0
            actual_avg = float(pre_fetched_holdings.get(ticker, {'avg': 0})['avg'] or 0.0) if pre_fetched_holdings else 0.0
            
            split = await asyncio.to_thread(self.cfg.get_split_count, ticker)
            t_val, _ = await asyncio.to_thread(self.cfg.get_absolute_t_val, ticker, actual_qty, actual_avg)
            
            report += "📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
            report += f"▪️ 현재 T값 : {t_val:.4f} T ({int(split)}분할)\n"
            report += f"▪️ 보유 수량 : {actual_qty} 주 (평단 ${actual_avg:,.2f})\n"
            report += f"▪️ 총 매수액 : ${total_buy:,.2f}\n"
            report += f"▪️ 총 매도액 : ${total_sell:,.2f}"
            
            msg = report

        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        keyboard = []
        
        v_mode = await asyncio.to_thread(self.cfg.get_version, ticker)
        if v_mode == "V_REV":
            keyboard.append([InlineKeyboardButton(f"🗄️ {ticker} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
            
        row = [InlineKeyboardButton(f"🔄 {t} 장부 업데이트", callback_data=f"REC:SYNC:{t}") for t in active_tickers]
        keyboard.append(row)
        markup = InlineKeyboardMarkup(keyboard)

        if query:
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
        elif message_obj:
            await message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML')
        else:
            await context.bot.send_message(chat_id, msg, reply_markup=markup, parse_mode='HTML')
