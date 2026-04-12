# ==========================================================
# [strategy_reversion.py]
# ⚠️ V-REV 하이브리드 엔진 전용 수학적 타격 모듈
# 💡 5년 백테스트 기반 VWAP 유동성 정밀 가중치(U_CURVE_WEIGHTS) 적용 완료
# 💡 [V24.16 팩트 동기화] 0주 새출발 디커플링 타점 (Buy1: 0.999, Buy2: /0.935)
# 💡 [V24.16 팩트 동기화] 하락장 방어 매수 Buy2 타점 (0.9725) 교정
# 💡 [V24.16 팩트 동기화] 1층 전량 익절 타점 고유 매수가 기반(layer_price * 1.006) 원복
# 🚨 [V25.13 디커플링 스왑 패치] UI와 동일하게 Buy1과 Buy2의 타점을 고가->저가 순으로 스왑 연동
# 🚨 [V25.14 팩트 동기화] 1층 물귀신 덤핑 차단 및 지층별 평단가 완벽 분리 개별 탈출(Decoupling) 이식
# ==========================================================
import math

class ReversionStrategy:
    def __init__(self):
        self.residual = {"BUY1": {}, "BUY2": {}, "SELL": {}}
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        
        # 5년치 실데이터 기반 장마감 30분 비중 정규화 적용 (합산 1.0)
        self.U_CURVE_WEIGHTS = [
            0.0252, 0.0213, 0.0192, 0.0210, 0.0189, 0.0187, 0.0228, 0.0203, 0.0200, 0.0209,
            0.0254, 0.0217, 0.0225, 0.0211, 0.0228, 0.0281, 0.0262, 0.0240, 0.0236, 0.0256,
            0.0434, 0.0294, 0.0327, 0.0362, 0.0549, 0.0566, 0.0407, 0.0470, 0.0582, 0.1515
        ]

    def reset_residual(self, ticker):
        self.residual["BUY1"][ticker] = 0.0
        self.residual["BUY2"][ticker] = 0.0
        self.residual["SELL"][ticker] = 0.0
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0

    def record_execution(self, ticker, side, qty, exec_price):
        if side == "BUY":
            spent = qty * exec_price
            self.executed["BUY_BUDGET"][ticker] = self.executed["BUY_BUDGET"].get(ticker, 0.0) + spent
        else:
            self.executed["SELL_QTY"][ticker] = self.executed["SELL_QTY"].get(ticker, 0) + qty

    def get_dynamic_plan(self, ticker, curr_p, prev_c, current_weight, vwap_status, min_idx, alloc_cash, q_data):
        if min_idx < 0 or min_idx >= 30:
            if not vwap_status.get('is_strong_up') and not vwap_status.get('is_strong_down'):
                return {"orders": [], "trigger_loc": False}

        total_q = sum(item.get("qty", 0) for item in q_data)
        avg_price = (sum(item.get("qty", 0) * item.get("price", 0.0) for item in q_data) / total_q) if total_q > 0 else 0.0
        
        if total_q == 0:
            side = "BUY"
            # 🚨 MODIFIED: [V25.13 디커플링 스왑 패치] 무조건 Buy1(p1)이 고가, Buy2(p2)가 저가가 되도록 변수 스왑
            p1_trigger = round(prev_c / 0.935, 2)
            p2_trigger = round(prev_c * 0.999, 2)
        else:
            side = "SELL" if curr_p > prev_c else "BUY"
            p1_trigger = round(prev_c * 0.995, 2)
            p2_trigger = round(prev_c * 0.9725, 2)

        is_strong_up = vwap_status.get('is_strong_up', False)
        is_strong_down = vwap_status.get('is_strong_down', False)
        
        trigger_loc = is_strong_up or is_strong_down 

        orders = []

        if trigger_loc:
            total_spent = self.executed["BUY_BUDGET"].get(ticker, 0.0)
            rem_budget = max(0.0, alloc_cash - total_spent)
            if rem_budget > 0:
                b1_budget = rem_budget * 0.5
                b2_budget = rem_budget - b1_budget
                
                q1 = math.floor(b1_budget / p1_trigger) if p1_trigger > 0 else 0
                q2 = math.floor(b2_budget / p2_trigger) if p2_trigger > 0 else 0
                
                if q1 > 0: orders.append({"side": "BUY", "qty": q1, "price": p1_trigger})
                if q2 > 0: orders.append({"side": "BUY", "qty": q2, "price": p2_trigger})
                
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
                
            rem_qty = max(0, total_q - self.executed["SELL_QTY"].get(ticker, 0))
            if rem_qty > 0:
                jackpot_trigger = avg_price * 1.010
                
                if curr_p >= jackpot_trigger:
                    target_sell_qty = rem_qty 
                    target_p = round(jackpot_trigger, 2)
                    orders.append({"side": "SELL", "qty": target_sell_qty, "price": target_p})
                else:
                    # MODIFIED: [V25.14 팩트 동기화] 1층과 상위 층(2~N층) 평단가 및 익절 타점 완벽 분리
                    dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                    
                    layer_1_qty = 0
                    layer_1_price = 0.0
                    
                    if dates_in_queue:
                        lots_1 = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                        layer_1_qty = sum(item.get('qty', 0) for item in lots_1)
                        if layer_1_qty > 0:
                            layer_1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_1) / layer_1_qty
                            
                    upper_qty = total_q - layer_1_qty
                    total_inv = sum(item.get('qty', 0) * item.get('price', 0.0) for item in q_data)
                    upper_inv = total_inv - (layer_1_qty * layer_1_price)
                    upper_avg = upper_inv / upper_qty if upper_qty > 0 else 0.0
                    
                    trigger_1 = round(layer_1_price * 1.006, 2)
                    trigger_upper = round(upper_avg * 1.005, 2) if upper_qty > 0 else 0.0
                    
                    available_l1 = min(layer_1_qty, rem_qty)
                    available_upper = min(upper_qty, rem_qty - available_l1)
                    
                    if available_l1 > 0 and curr_p >= trigger_1:
                        orders.append({"side": "SELL", "qty": available_l1, "price": trigger_1})
                        
                    if available_upper > 0 and trigger_upper > 0 and curr_p >= trigger_upper:
                        orders.append({"side": "SELL", "qty": available_upper, "price": trigger_upper})
            
            return {"orders": orders, "trigger_loc": True}

        rem_weight = sum(self.U_CURVE_WEIGHTS[min_idx:])
        slice_ratio_sell = current_weight / rem_weight if rem_weight > 0 else 1.0
        
        total_weight = sum(self.U_CURVE_WEIGHTS)
        slice_ratio_buy = current_weight / total_weight if total_weight > 0 else 1.0

        if side == "BUY":
            total_spent = self.executed["BUY_BUDGET"].get(ticker, 0.0)
            if total_spent >= alloc_cash:
                return {"orders": [], "trigger_loc": False}
            
            b1_budget_slice = (alloc_cash * 0.5) * slice_ratio_buy
            b2_budget_slice = (alloc_cash * 0.5) * slice_ratio_buy

            if curr_p <= p1_trigger:
                exact_q1 = (b1_budget_slice / curr_p) + self.residual["BUY1"].get(ticker, 0.0)
                alloc_q1 = math.floor(exact_q1)
                self.residual["BUY1"][ticker] = exact_q1 - alloc_q1
                if alloc_q1 > 0:
                    orders.append({"side": "BUY", "qty": alloc_q1, "price": p1_trigger})
                    
            if curr_p <= p2_trigger:
                exact_q2 = (b2_budget_slice / curr_p) + self.residual["BUY2"].get(ticker, 0.0)
                alloc_q2 = math.floor(exact_q2)
                self.residual["BUY2"][ticker] = exact_q2 - alloc_q2
                if alloc_q2 > 0:
                    orders.append({"side": "BUY", "qty": alloc_q2, "price": p2_trigger})

        else: # SELL
            if total_q > 0:
                jackpot_trigger = avg_price * 1.010
                
                if curr_p >= jackpot_trigger:
                    target_sell_qty = total_q
                    sell_price_target = round(jackpot_trigger, 2)
                    
                    rem_qty_to_sell = max(0, target_sell_qty - self.executed["SELL_QTY"].get(ticker, 0))
                    
                    if rem_qty_to_sell > 0:
                        exact_qs = (target_sell_qty * slice_ratio_sell) + self.residual["SELL"].get(ticker, 0.0)
                        alloc_qs = math.floor(exact_qs)
                        
                        alloc_qs = min(alloc_qs, rem_qty_to_sell)
                        self.residual["SELL"][ticker] = exact_qs - alloc_qs
                        
                        if alloc_qs > 0:
                            orders.append({"side": "SELL", "qty": alloc_qs, "price": sell_price_target})
                else:
                    # MODIFIED: [V25.14 팩트 동기화] 1층과 상위 층(2~N층) 평단가 및 익절 타점 완벽 분리
                    dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                    
                    layer_1_qty = 0
                    layer_1_price = 0.0
                    
                    if dates_in_queue:
                        lots_1 = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                        layer_1_qty = sum(item.get('qty', 0) for item in lots_1)
                        if layer_1_qty > 0:
                            layer_1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_1) / layer_1_qty
                            
                    upper_qty = total_q - layer_1_qty
                    total_inv = sum(item.get('qty', 0) * item.get('price', 0.0) for item in q_data)
                    upper_inv = total_inv - (layer_1_qty * layer_1_price)
                    upper_avg = upper_inv / upper_qty if upper_qty > 0 else 0.0
                    
                    trigger_1 = round(layer_1_price * 1.006, 2)
                    trigger_upper = round(upper_avg * 1.005, 2) if upper_qty > 0 else 0.0
                    
                    target_sell_qty = 0
                    sell_price_target = 0.0
                    
                    is_l1_hit = (layer_1_qty > 0 and curr_p >= trigger_1)
                    is_upper_hit = (upper_qty > 0 and trigger_upper > 0 and curr_p >= trigger_upper)
                    
                    if is_l1_hit and is_upper_hit:
                        target_sell_qty = layer_1_qty + upper_qty
                        sell_price_target = min(trigger_1, trigger_upper)
                    elif is_l1_hit:
                        target_sell_qty = layer_1_qty
                        sell_price_target = trigger_1
                    elif is_upper_hit:
                        target_sell_qty = upper_qty
                        sell_price_target = trigger_upper
                        
                    rem_qty_to_sell = max(0, target_sell_qty - self.executed["SELL_QTY"].get(ticker, 0))
                    
                    if rem_qty_to_sell > 0:
                        exact_qs = (target_sell_qty * slice_ratio_sell) + self.residual["SELL"].get(ticker, 0.0)
                        alloc_qs = math.floor(exact_qs)
                        
                        alloc_qs = min(alloc_qs, rem_qty_to_sell)
                        self.residual["SELL"][ticker] = exact_qs - alloc_qs
                        
                        if alloc_qs > 0:
                            orders.append({"side": "SELL", "qty": alloc_qs, "price": sell_price_target})

        return {"orders": orders, "trigger_loc": False}

    def get_emergency_liquidation_qty(self, alloc_cash, available_cash, q_data):
        total_q = sum(item.get("qty", 0) for item in q_data)
        
        if total_q > 0 and available_cash < (alloc_cash / 2.0):
            if q_data:
                return q_data[-1].get('qty', 0)
        return 0
