# ==========================================================
# FILE: strategy_reversion.py
# ==========================================================
# MODIFIED: [V44.27 0주 스냅샷 환각 락온] 서버 재시작으로 인메모리 스냅샷이 소실되었을 때, VWAP이 장중 매수한 로트를 기보유 물량으로 오판하여 매도를 재개(하극상)하던 맹점 원천 차단. 큐 장부에서 당일 날짜(EST)의 로트를 100% 도려내고 오직 어제까지 이월된 순수 과거 물량만을 스캔하여 '0주 새출발' 상태를 완벽히 팩트 복구하는 타임머신 역산 엔진 이식 완료.
# MODIFIED: [V44.25 예산 탈취(Stealing) 런타임 붕괴 방어막 이식] Buy1이 Buy2의 미사용 예산을 훔쳐와 무한 타격(34주 체결 등)하는 차원 붕괴를 영구 소각.
# MODIFIED: [V44.25 AVWAP 디커플링] VWAP 기상 전 스냅샷 2중 교차 검증(Fail-Safe) 및 암살자 물량(AVWAP) 100% 격리(Decoupling) 파이프라인 이식 완료.
# MODIFIED: [V44.36 큐 장부 vs 브로커 실잔고 불일치 팩트 스캔] 페일세이프 스냅샷 복원 시 KIS 순수 본대 수량과 큐 장부 이월 수량 간의 팩트 불일치가 발생할 경우 명시적으로 경고를 타전하여 CALIB 보정을 유도하도록 감시망(EC-3) 이식 완료.
# MODIFIED: [V44.48 런타임 붕괴 방어] 들여쓰기 붕괴(IndentationError) 완벽 교정.
import math
import os
import json
import tempfile
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class ReversionStrategy:
    # MODIFIED: [V42 U-Curve 락온 무결성 복구] config 주입 배선 100% 복구
    # 🚨 [AI 에이전트 절대 주의 환각 방어막] V42 U-Curve 락온 무결성 유지를 위해 config 주입을 훼손하지 말 것
    def __init__(self, config):
        self.cfg = config
        self.residual = {
            "BUY1": {}, "BUY2": {}, 
            "SELL_L1": {}, "SELL_UPPER": {}, "SELL_JACKPOT": {}
        }
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        self.state_loaded = {}
        self.was_holding = {}

    def _get_logical_date_str(self):
        now_est = datetime.now(ZoneInfo('America/New_York'))
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 5):
            target_date = now_est - timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime("%Y-%m-%d")

    def _get_state_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/vwap_state_REV_{today_str}_{ticker}.json"

    def _get_snapshot_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/daily_snapshot_REV_{today_str}_{ticker}.json"

    def _load_state_if_needed(self, ticker):
        today_str = self._get_logical_date_str()
        if self.state_loaded.get(ticker) == today_str:
            return 
            
        state_file = self._get_state_file(ticker)
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for k in self.residual.keys():
                        self.residual[k][ticker] = float(data.get("residual", {}).get(k, 0.0))
                    for k in self.executed.keys():
                        raw_val = data.get("executed", {}).get(k, 0)
                        self.executed[k][ticker] = int(raw_val) if k == "SELL_QTY" else float(raw_val)
                    self.was_holding[ticker] = bool(data.get("was_holding", False))
                    self.state_loaded[ticker] = today_str
                    return
            except Exception:
                pass
                
        for k in self.residual.keys():
            self.residual[k][ticker] = 0.0
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0
        self.was_holding[ticker] = False
        self.state_loaded[ticker] = today_str

    def _save_state(self, ticker):
        today_str = self._get_logical_date_str()
        state_file = self._get_state_file(ticker)
        data = {
            "date": today_str,
            "residual": {k: float(self.residual[k].get(ticker, 0.0)) for k in self.residual.keys()},
            "executed": {
                "BUY_BUDGET": float(self.executed.get("BUY_BUDGET", {}).get(ticker, 0.0)),
                "SELL_QTY": int(self.executed.get("SELL_QTY", {}).get(ticker, 0))
            },
            "was_holding": bool(self.was_holding.get(ticker, False))
        }
        temp_path = None
        try:
            dir_name = os.path.dirname(state_file)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, state_file)
            temp_path = None
        except Exception:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    def save_daily_snapshot(self, ticker, plan_data):
        snap_file = self._get_snapshot_file(ticker)
        if os.path.exists(snap_file):
            return
            
        today_str = self._get_logical_date_str()
        data = {
            "date": today_str,
            "plan": plan_data
        }
        temp_path = None
        try:
            dir_name = os.path.dirname(snap_file)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, snap_file)
            temp_path = None
        except Exception:
            if temp_path and os.path.exists(temp_path):
                try: os.unlink(temp_path)
                except OSError: pass

    def load_daily_snapshot(self, ticker):
        snap_file = self._get_snapshot_file(ticker)
        if os.path.exists(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("plan")
            except Exception:
                pass
        return None

    def ensure_failsafe_snapshot(self, ticker, curr_p, prev_c, alloc_cash, q_data, total_kis_qty, avwap_qty):
        snap = self.load_daily_snapshot(ticker)
        if snap is not None:
            return snap
            
        pure_qty = max(0, total_kis_qty - avwap_qty)
        
        today_str_est = self._get_logical_date_str()
        legacy_lots = [item for item in q_data if not str(item.get("date", "")).startswith(today_str_est)]
        legacy_q = sum(int(item.get("qty", 0)) for item in legacy_lots if float(item.get('price', 0.0)) > 0)
        
        if pure_qty != legacy_q:
            logging.warning(f"⚠️ [{ticker}] V-REV 페일세이프 경고: KIS 순수 본대 수량({pure_qty}주)과 이월 큐 장부 수량({legacy_q}주) 불일치 감지. CALIB 비파괴 보정 또는 수동 동기화 요망.")
            
        logging.warning(f"🚨 [{ticker}] V_REV 스냅샷 증발 감지! 페일세이프 긴급 복원 가동 (KIS총잔고:{total_kis_qty} - 암살자:{avwap_qty} = 본대:{pure_qty}주 | 이월 큐 장부:{legacy_q}주)")
        
        return self.get_dynamic_plan(
            ticker=ticker,
            curr_p=curr_p,
            prev_c=prev_c,
            current_weight=0.0,
            vwap_status={},
            min_idx=-1,
            alloc_cash=alloc_cash,
            q_data=legacy_lots,
            is_snapshot_mode=True,
            market_type="REG"
        )

    def reset_residual(self, ticker):
        self._load_state_if_needed(ticker)
        self.residual["BUY1"][ticker] = 0.0
        self.residual["BUY2"][ticker] = 0.0
        self.residual["SELL_L1"][ticker] = 0.0
        self.residual["SELL_UPPER"][ticker] = 0.0
        self.residual["SELL_JACKPOT"][ticker] = 0.0
        self._save_state(ticker)

    def record_execution(self, ticker, side, qty, exec_price):
        self._load_state_if_needed(ticker)
        safe_qty = int(float(qty or 0))
        safe_price = float(exec_price or 0.0)
        
        if side == "BUY":
            spent = safe_qty * safe_price
            self.executed["BUY_BUDGET"][ticker] = float(self.executed.get("BUY_BUDGET", {}).get(ticker, 0.0)) + spent
        else:
            self.executed["SELL_QTY"][ticker] = int(self.executed.get("SELL_QTY", {}).get(ticker, 0)) + safe_qty
        self._save_state(ticker)

    def get_dynamic_plan(self, ticker, curr_p, prev_c, current_weight, vwap_status, min_idx, alloc_cash, q_data, is_snapshot_mode=False, market_type="REG"):
        self._load_state_if_needed(ticker)

        valid_q_data = [item for item in q_data if float(item.get('price', 0.0)) > 0]
        total_q = sum(int(item.get("qty", 0)) for item in valid_q_data)
        total_inv = sum(float(item.get('qty', 0)) * float(item.get('price', 0.0)) for item in valid_q_data)
        avg_price = (total_inv / total_q) if total_q > 0 else 0.0
        
        dates_in_queue = sorted(list(set(item.get('date') for item in valid_q_data if item.get('date'))), reverse=True)
        l1_qty, l1_price = 0, 0.0
        
        if dates_in_queue:
            lots_1 = [item for item in valid_q_data if item.get('date') == dates_in_queue[0]]
            l1_qty = sum(int(item.get('qty', 0)) for item in lots_1)
            l1_price = sum(float(item.get('qty', 0)) * float(item.get('price', 0.0)) for item in lots_1) / l1_qty if l1_qty > 0 else 0.0
            
        upper_qty = total_q - l1_qty
        upper_inv = total_inv - (l1_qty * l1_price)
        upper_avg = upper_inv / upper_qty if upper_qty > 0 else 0.0

        trigger_jackpot = round(avg_price * 1.010, 2)
        trigger_l1 = round(l1_price * 1.006, 2)
        trigger_upper = round(upper_avg * 1.005, 2) if upper_qty > 0 else 0.0

        cached_plan = self.load_daily_snapshot(ticker)
        
        if is_snapshot_mode:
            is_zero_start_session = (total_q == 0)
        else:
            if cached_plan:
                is_zero_start_session = cached_plan.get("is_zero_start", cached_plan.get("snapshot_total_q", cached_plan.get("total_q", -1)) == 0)
            else:
                today_str_est = self._get_logical_date_str()
                legacy_lots = [item for item in valid_q_data if not str(item.get("date", "")).startswith(today_str_est)]
                legacy_q = sum(int(item.get("qty", 0)) for item in legacy_lots)
                is_zero_start_session = (legacy_q == 0)

        try:
            profile = getattr(self, 'cfg').get_vwap_profile(ticker) if hasattr(self, 'cfg') and hasattr(self.cfg, 'get_vwap_profile') else {}
        except Exception as e:
            logging.error(f"🚨 [{ticker}] VWAP 프로파일 로드 실패: {e}")
            profile = {}
            
        target_keys = [f"15:{str(m).zfill(2)}" for m in range(27, 60)]
        total_target_vol = sum(profile.get(k, 0.0) for k in target_keys)
        
        now_est = datetime.now(ZoneInfo('America/New_York'))
        time_str = now_est.strftime('%H:%M')

        if not is_snapshot_mode and time_str not in target_keys:
            if cached_plan:
                if is_zero_start_session:
                    p1_trigger_fact = round(prev_c * 1.15, 2)
                    p2_trigger_fact = round(prev_c * 0.999, 2)
                    b1_budget = alloc_cash * 0.5
                    b2_budget = alloc_cash - b1_budget
                    
                    q1 = math.floor(b1_budget / p1_trigger_fact) if p1_trigger_fact > 0 else 0
                    q2 = math.floor(b2_budget / p2_trigger_fact) if p2_trigger_fact > 0 else 0
                    
                    new_buy_orders = []
                    if q1 > 0: new_buy_orders.append({"side": "BUY", "qty": q1, "price": p1_trigger_fact})
                    if q2 > 0: new_buy_orders.append({"side": "BUY", "qty": q2, "price": p2_trigger_fact})
                    
                    cached_plan["orders"] = new_buy_orders
                    cached_plan["total_q"] = 0
                else:
                    buy_orders = [o for o in cached_plan.get("orders", []) if o.get("side") == "BUY"]
                    sell_orders = []
                    
                    rem_qty_total = max(0, int(total_q) - int(self.executed.get("SELL_QTY", {}).get(ticker, 0)))
                    if rem_qty_total > 0:
                        sell_orders.append({"side": "SELL", "qty": rem_qty_total, "price": trigger_jackpot})
                        
                        available_l1 = min(l1_qty, rem_qty_total)
                        l1_queued = 0
                        if available_l1 > 0:
                            sell_orders.append({"side": "SELL", "qty": available_l1, "price": trigger_l1})
                            l1_queued = available_l1
                            
                        available_upper = min(upper_qty, rem_qty_total - l1_queued)
                        if available_upper > 0:
                            sell_orders.append({"side": "SELL", "qty": available_upper, "price": trigger_upper})
                    
                    cached_plan["orders"] = buy_orders + sell_orders
                    cached_plan["snapshot_total_q"] = cached_plan.get("snapshot_total_q", cached_plan.get("total_q", 0)) 
                    cached_plan["total_q"] = total_q
                
                if is_zero_start_session and market_type != "AFTER":
                    cached_plan["orders"] = [o for o in cached_plan.get("orders", []) if o.get("side") != "SELL"]
                    
                return cached_plan

        if time_str not in target_keys:
            if not vwap_status.get('is_strong_up') and not vwap_status.get('is_strong_down'):
                return {"orders": [], "trigger_loc": False, "total_q": total_q}

        if is_zero_start_session or total_q == 0:
            side = "BUY"
            p1_trigger = round(prev_c * 1.15, 2)
            p2_trigger = round(prev_c * 0.999, 2)
        else:
            side = "SELL" if curr_p > prev_c else "BUY"
            p1_trigger = round(prev_c * 0.995, 2)
            p2_trigger = round(prev_c * 0.9725, 2)

        if total_q > 0:
            active_sell_targets = [t for t in [trigger_jackpot, trigger_l1, trigger_upper] if t > 0]
            if active_sell_targets:
                min_sell = min(active_sell_targets)
                if p1_trigger >= min_sell:
                    p1_trigger = max(0.01, round(min_sell - 0.01, 2))
                if p2_trigger >= min_sell:
                    p2_trigger = max(0.01, round(min_sell - 0.01, 2))

        is_strong_up = vwap_status.get('is_strong_up', False)
        is_strong_down = vwap_status.get('is_strong_down', False)
        trigger_loc = is_strong_up or is_strong_down 

        orders = []

        if trigger_loc or is_snapshot_mode:
            total_spent = float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
            rem_budget = max(0.0, float(alloc_cash) - total_spent)
            if rem_budget > 0:
                b1_budget = rem_budget * 0.5
                b2_budget = rem_budget - b1_budget
                
                q1 = math.floor(b1_budget / p1_trigger) if p1_trigger > 0 else 0
                q2 = math.floor(b2_budget / p2_trigger) if p2_trigger > 0 else 0
                
                if q1 > 0: orders.append({"side": "BUY", "qty": q1, "price": p1_trigger})
                if q2 > 0: orders.append({"side": "BUY", "qty": q2, "price": p2_trigger})
                
                if total_q > 0:
                    max_n = 5
                    if curr_p > 0:
                        required_n = math.ceil(b2_budget / curr_p) - q2
                        if required_n > 5:
                            max_n = min(required_n, 50)
                    
                    for n in range(1, max_n + 1):
                        if (q2 + n) > 0:
                            grid_p2 = round(b2_budget / (q2 + n), 2)
                            if grid_p2 >= 0.01 and grid_p2 < p2_trigger:
                                orders.append({"side": "BUY", "qty": 1, "price": grid_p2})
                
            rem_qty_total = max(0, int(total_q) - int(self.executed["SELL_QTY"].get(ticker, 0)))
            if rem_qty_total > 0:
                if curr_p >= trigger_jackpot:
                    orders.append({"side": "SELL", "qty": rem_qty_total, "price": trigger_jackpot})
                else:
                    available_l1 = min(l1_qty, rem_qty_total)
                    l1_queued = 0
                    if available_l1 > 0 and curr_p >= trigger_l1:
                        orders.append({"side": "SELL", "qty": available_l1, "price": trigger_l1})
                        l1_queued = available_l1
                        
                    available_upper = min(upper_qty, rem_qty_total - l1_queued)
                    if available_upper > 0 and trigger_upper > 0 and curr_p >= trigger_upper:
                        orders.append({"side": "SELL", "qty": available_upper, "price": trigger_upper})
            
            plan_result = {
                "orders": orders, 
                "trigger_loc": True, 
                "total_q": total_q,
                "is_zero_start": is_zero_start_session
            }
            
            if is_zero_start_session and market_type != "AFTER":
                plan_result["orders"] = [o for o in plan_result.get("orders", []) if o.get("side") != "SELL"]
            
            if is_snapshot_mode:
                self.save_daily_snapshot(ticker, plan_result)
                
            return plan_result

        rem_weight = 0.0
        if time_str in target_keys:
            start_idx = target_keys.index(time_str)
            rem_vol = sum(profile.get(k, 0.0) for k in target_keys[start_idx:])
            rem_weight = (rem_vol / total_target_vol) if total_target_vol > 0 else (30 - start_idx) / 30.0
            
            slice_ratio_sell = current_weight / rem_weight if rem_weight > 0 else 1.0
            slice_ratio_buy = current_weight / rem_weight if rem_weight > 0 else 1.0
        else:
            slice_ratio_sell = 0.0
            slice_ratio_buy = 0.0

        if side == "BUY":
            total_spent = float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
            rem_budget = max(0.0, float(alloc_cash) - total_spent)
            
            if rem_budget <= 0:
                return {"orders": [], "trigger_loc": False, "total_q": total_q}
            
            # MODIFIED: [예산 탈취 디커플링 수술]
            # 🚨 [AI 에이전트 절대 주의 - 환각 방어막]
            # Buy1과 Buy2가 순차적 차감으로 서로의 예산을 훔치는 맹점을 소각.
            # 각각의 최대 할당량(alloc_cash * 0.5)을 한계치로 독립 캡핑한 후 초과 시 균등 축소 적용.
            half_alloc = float(alloc_cash) * 0.5
            raw_b1_slice = half_alloc * current_weight
            raw_b2_slice = half_alloc * current_weight
            
            b1_bucket = float(self.residual["BUY1"].get(ticker, 0.0)) + raw_b1_slice
            b2_bucket = float(self.residual["BUY2"].get(ticker, 0.0)) + raw_b2_slice

            b1_budget_slice = min(b1_bucket, half_alloc)
            b2_budget_slice = min(b2_bucket, half_alloc)
            
            total_slice = b1_budget_slice + b2_budget_slice
            if total_slice > rem_budget and total_slice > 0:
                ratio = rem_budget / total_slice
                b1_budget_slice *= ratio
                b2_budget_slice *= ratio

            if curr_p > 0:
                # MODIFIED: [NameError 런타임 붕괴 수술] 선언되지 않은 환각 변수(buy_star_price) 참조 전면 소각 및 0.5회분 무조건 매수 팩트 락온
                if is_zero_start_session or curr_p <= p1_trigger:
                    alloc_q1 = int(math.floor(b1_budget_slice / curr_p))
                    self.residual["BUY1"][ticker] = b1_bucket - (alloc_q1 * curr_p)
                    if alloc_q1 > 0:
                        orders.append({"side": "BUY", "qty": alloc_q1, "price": p1_trigger})
                else:
                    self.residual["BUY1"][ticker] = b1_bucket

                if curr_p <= p2_trigger:
                    alloc_q2 = int(math.floor(b2_budget_slice / curr_p))
                    self.residual["BUY2"][ticker] = b2_bucket - (alloc_q2 * curr_p)
                    if alloc_q2 > 0:
                        orders.append({"side": "BUY", "qty": alloc_q2, "price": p2_trigger})
                else:
                    self.residual["BUY2"][ticker] = b2_bucket
            else:
                self.residual["BUY1"][ticker] = b1_bucket
                self.residual["BUY2"][ticker] = b2_bucket

        else: # SELL
            rem_qty_total = total_q
            
            if rem_qty_total <= 0:
                return {"orders": [], "trigger_loc": False, "total_q": total_q}

            if slice_ratio_sell > 0:
                if curr_p >= trigger_jackpot:
                    exact_qs = float(rem_qty_total * slice_ratio_sell) + float(self.residual["SELL_JACKPOT"].get(ticker, 0.0))
                    alloc_qs = int(min(math.floor(exact_qs), rem_qty_total))
                    self.residual["SELL_JACKPOT"][ticker] = float(exact_qs - alloc_qs)
                    if alloc_qs > 0:
                        orders.append({"side": "SELL", "qty": alloc_qs, "price": trigger_jackpot})
                else:
                    if l1_qty > 0 and curr_p >= trigger_l1:
                        sold_so_far = int(total_q) - rem_qty_total
                        rem_l1_qty = max(0, l1_qty - sold_so_far)
                        if rem_l1_qty > 0:
                            exact_l1 = float(rem_l1_qty * slice_ratio_sell) + float(self.residual["SELL_L1"].get(ticker, 0.0))
                            alloc_l1 = int(min(math.floor(exact_l1), rem_l1_qty))
                            self.residual["SELL_L1"][ticker] = float(exact_l1 - alloc_l1)
                            if alloc_l1 > 0:
                                orders.append({"side": "SELL", "qty": alloc_l1, "price": trigger_l1})
                            rem_qty_total -= alloc_l1

                    if upper_qty > 0 and trigger_upper > 0 and curr_p >= trigger_upper and rem_qty_total > 0:
                        exact_upper = float(rem_qty_total * slice_ratio_sell) + float(self.residual["SELL_UPPER"].get(ticker, 0.0))
                        alloc_upper = int(min(math.floor(exact_upper), rem_qty_total))
                        self.residual["SELL_UPPER"][ticker] = float(exact_upper - alloc_upper)
                        if alloc_upper > 0:
                            orders.append({"side": "SELL", "qty": alloc_upper, "price": trigger_upper})

        if is_zero_start_session and market_type != "AFTER":
            orders = [o for o in orders if o.get("side") != "SELL"]

        self._save_state(ticker)
        return {"orders": orders, "trigger_loc": False, "total_q": total_q}
