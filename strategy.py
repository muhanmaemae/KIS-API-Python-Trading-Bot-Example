import math
from datetime import datetime, timedelta

class InfiniteStrategy:
    def __init__(self, config):
        self.cfg = config

    def _ceil(self, val): return math.ceil(val * 100) / 100.0
    def _floor(self, val): return math.floor(val * 100) / 100.0

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, force_turbo_off=False):
        core_orders = []
        bonus_orders = []
        smart_core_orders = []   
        smart_bonus_orders = []  
        process_status = "" 
        
        # ==========================================================
        # 🛡️ [V18.13 패치] KIS 자전거래(Wash-Trade) 원천 차단 방어벽 엔진
        # ==========================================================
        def apply_wash_trade_shield(c_orders, b_orders, sc_orders, sb_orders):
            all_o = c_orders + b_orders + sc_orders + sb_orders
            
            has_sell_moc = any(o['type'] in ['MOC', 'MOO'] and o['side'] == 'SELL' for o in all_o)
            
            s_prices = [o['price'] for o in all_o if o['side'] == 'SELL' and o['price'] > 0]
            min_s = min(s_prices) if s_prices else 0.0

            def _clean(lst):
                res = []
                for o in lst:
                    if o['side'] == 'BUY':
                        if has_sell_moc and o['type'] in ['LOC', 'MOC']: 
                            continue 
                        
                        if min_s > 0 and o['price'] >= min_s:
                            o['price'] = round(min_s - 0.01, 2)
                            if "🛡️" not in o['desc']: 
                                o['desc'] = f"🛡️교정_{o['desc'].replace('🦇', '').replace('🧹', '')}"
                    res.append(o)
                return res

            return _clean(c_orders), _clean(b_orders), _clean(sc_orders), _clean(sb_orders)
        # ==========================================================

        other_locked_cash = self.cfg.get_total_locked_cash(exclude_ticker=ticker)
        real_available_cash = max(0, available_cash - other_locked_cash)
        
        split = self.cfg.get_split_count(ticker)      
        target_pct_val = self.cfg.get_target_profit(ticker) 
        target_ratio = target_pct_val / 100.0
        version = self.cfg.get_version(ticker)
        
        rev_state = self.cfg.get_reverse_state(ticker)
        is_reverse = rev_state.get("is_active", False)
        rev_day = rev_state.get("day_count", 0)
        exit_target = rev_state.get("exit_target", 0.0)

        t_val, base_portion = self.cfg.get_absolute_t_val(ticker, qty, avg_price)

        if version in ["V14", "V17"]:
            _, dynamic_budget, rem_cash = self.cfg.calculate_v14_state(ticker)
            one_portion_amt = dynamic_budget
            
            is_money_short_check = False if (is_simulation or market_type == "PRE_CHECK") else (real_available_cash < one_portion_amt)
            
            if not is_reverse and (t_val > (split - 1) or (qty > 0 and is_money_short_check)):
                is_reverse = True 
                rev_day = 1 
                
                current_return = (current_price - avg_price) / avg_price * 100.0 if avg_price > 0 else 0.0
                default_exit = -15.0 if ticker == "TQQQ" else -20.0
                
                if current_return >= default_exit:
                    exit_target = 0.0
                else:
                    exit_target = default_exit

                if market_type == "REG":
                    self.cfg.set_reverse_state(ticker, True, rev_day, exit_target)
        else:
            one_portion_amt = base_portion

        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        
        if is_reverse:
            if ma_5day > 0: star_price = round(ma_5day, 2)
            else: star_price = round(avg_price, 2)

            escrow_cash = self.cfg.get_escrow_cash(ticker)
            one_portion_amt = (escrow_cash / 4.0) if escrow_cash > 0 else 0.0
        else:
            star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0
            
        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0
        is_last_lap = (split - 1) < t_val < split
        
        if is_simulation: is_money_short = False
        else: is_money_short = real_available_cash < one_portion_amt

        base_price = current_price if current_price > 0 else prev_close
        if base_price <= 0: 
            return {"orders": [], "core_orders": [], "bonus_orders": [], "smart_core_orders": [], "smart_bonus_orders": [], "t_val": t_val, "one_portion": one_portion_amt, "process_status": "⛔가격오류", "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

        if market_type == "PRE_CHECK":
            process_status = "🌅프리마켓"
            if qty > 0 and target_price > 0 and current_price >= target_price and not is_reverse:
                core_orders.append({"side": "SELL", "price": current_price, "qty": qty, "type": "LIMIT", "desc": "🌅프리:목표돌파익절"})
            orders = core_orders + bonus_orders
            return {"orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders, "smart_core_orders": [], "smart_bonus_orders": [], "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

        if market_type == "REG":
            if qty == 0:
                process_status = "✨새출발"
                buy_price = round(self._ceil(base_price * 1.15) - 0.01, 2)
                buy_qty = math.floor(one_portion_amt / buy_price) if buy_price > 0 else 0
                if buy_qty > 0:
                    core_orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty, "type": "LOC", "desc": "🆕새출발"})
                orders = core_orders + bonus_orders
                return {"orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders, "smart_core_orders": [], "smart_bonus_orders": [], "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

            if is_reverse:
                sell_divisor = 10 if split <= 20 else 20
                sell_qty = math.floor(qty / sell_divisor) 

                is_emergency_cash_needed = (real_available_cash < base_price) and (rev_day > 1)

                if rev_day == 1 or is_emergency_cash_needed:
                    process_status = "🩸리버스(긴급수혈)" if is_emergency_cash_needed else "🚨리버스(1일차)"
                    if sell_qty > 0:
                        # 🛡️ [V19.7] 불필요한 (MOC) 중복 텍스트 제거
                        desc_str = "🩸수혈매도" if is_emergency_cash_needed else "🛡️의무매도"
                        core_orders.append({"side": "SELL", "price": 0, "qty": sell_qty, "type": "MOC", "desc": desc_str})
                else:
                    process_status = f"🔄리버스({rev_day}일차)"
                    buy_qty = 0
                    buy_price = 0
                    if one_portion_amt > 0 and star_price > 0:
                        buy_price = round(star_price - 0.01, 2)
                        if buy_price > 0: 
                            buy_qty = math.floor(one_portion_amt / buy_price)
                            if buy_qty > 0:
                                core_orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty, "type": "LOC", "desc": "⚓잔금매수"})
                    
                    if sell_qty > 0 and star_price > 0:
                        core_orders.append({"side": "SELL", "price": star_price, "qty": sell_qty, "type": "LOC", "desc": "🌟별값매도"})

                    if one_portion_amt > 0 and buy_price > 0:
                        for i in range(1, 6):
                            target_qty = buy_qty + i 
                            raw_jup_price = self._floor(one_portion_amt / target_qty)
                            capped_jup_price = min(raw_jup_price, buy_price - 0.01)
                            jup_price = round(capped_jup_price, 2)
                            if jup_price > 0:
                                bonus_orders.append({"side": "BUY", "price": jup_price, "qty": 1, "type": "LOC", "desc": f"🧹리버스줍줍({i})" })
                
                if market_type == "REG":
                    self.cfg.set_reverse_state(ticker, True, rev_day, exit_target)
                        
                core_orders, bonus_orders, smart_core_orders, smart_bonus_orders = apply_wash_trade_shield(core_orders, bonus_orders, smart_core_orders, smart_bonus_orders)        
                orders = core_orders + bonus_orders
                return {"orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders, "smart_core_orders": [], "smart_bonus_orders": [], "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

            if is_last_lap: process_status = "🏁마지막회차"
            elif is_money_short: process_status = "🛡️방어모드(부족)"
            elif t_val < (split / 2): process_status = "🌓전반전"
            else: process_status = "🌕후반전"

            can_buy = not is_money_short and not is_last_lap
            is_turbo_active = False if force_turbo_off else self.cfg.get_turbo_mode()
            
            safe_ceiling = min(avg_price, star_price) if star_price > 0 else avg_price

            if is_turbo_active and not is_last_lap:
                if is_simulation or real_available_cash >= one_portion_amt:
                    ref_price = min(avg_price, prev_close)
                    raw_turbo = self._ceil(ref_price * 0.95) - 0.01
                    turbo_price = round(min(raw_turbo, safe_ceiling - 0.01), 2)
                    turbo_qty = math.floor(one_portion_amt / turbo_price) if turbo_price > 0 else 0
                    if turbo_qty > 0:
                        core_orders.append({"side": "BUY", "price": turbo_price, "qty": turbo_qty, "type": "LOC", "desc": "🏎️가속매수"})

            standard_buy_qty = 0 
            N = math.floor(one_portion_amt / avg_price) if avg_price > 0 else 0
            p_avg = round(min(self._ceil(avg_price) - 0.01, safe_ceiling - 0.01), 2)
            
            if can_buy:
                p_star = round(star_price - 0.01, 2)

                if t_val < (split / 2):
                    half_amt = one_portion_amt * 0.5
                    q_avg_init = math.floor(half_amt / p_avg) if p_avg > 0 else 0
                    q_star = math.floor(half_amt / p_star) if p_star > 0 else 0
                    total_basic = q_avg_init + q_star
                    if total_basic < N: q_avg = q_avg_init + (N - total_basic)
                    else: q_avg = q_avg_init
                    
                    if q_avg > 0:
                        core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "LOC", "desc": "⚓평단매수"})
                        standard_buy_qty += q_avg
                    if q_star > 0:
                        core_orders.append({"side": "BUY", "price": p_star, "qty": q_star, "type": "LOC", "desc": "💫별값매수"})
                        standard_buy_qty += q_star
                else: 
                    if p_star > 0:
                        q_star = math.floor(one_portion_amt / p_star)
                        if q_star > 0:
                            core_orders.append({"side": "BUY", "price": p_star, "qty": q_star, "type": "LOC", "desc": "💫별값매수"})
                            standard_buy_qty += q_star

            if one_portion_amt > 0 and (is_simulation or not is_money_short):
                base_qty_for_jup = math.floor(one_portion_amt / avg_price) if avg_price > 0 else 0
                if base_qty_for_jup > 0:
                    for i in range(1, 6):
                        jup_price = self._floor(one_portion_amt / (base_qty_for_jup + i))
                        capped_jup_price = round(min(jup_price, avg_price - 0.01), 2)
                        if capped_jup_price > 0:
                            bonus_orders.append({"side": "BUY", "price": capped_jup_price, "qty": 1, "type": "LOC", "desc": f"🧹줍줍({i})"})

            if qty > 0:
                q_qty = math.ceil(qty / 4)
                rem_qty = qty - q_qty
                
                if version == "V17":
                    sell_trigger = star_price if t_val < (split / 2) else math.ceil(avg_price * 1.0025 * 100) / 100.0
                    if sell_trigger > 0 and q_qty > 0:
                        core_orders.append({"side": "SELL", "price": sell_trigger, "qty": q_qty, "type": "LOC", "desc": "🦇시크릿쿼터"})
                    if target_price > 0 and rem_qty > 0:
                        core_orders.append({"side": "SELL", "price": target_price, "qty": rem_qty, "type": "LIMIT", "desc": "🎯목표매도"})
                        
                    if can_buy and p_avg > 0:
                        smart_core_orders.append({"side": "BUY", "price": p_avg, "qty": N, "type": "LOC", "desc": "🦇스마트방어(평단)"})
                        for i in range(1, 6):
                            j_price = self._floor(one_portion_amt / (N + i))
                            c_j_price = round(min(j_price, p_avg - 0.01), 2)
                            if c_j_price > 0:
                                smart_bonus_orders.append({"side": "BUY", "price": c_j_price, "qty": 1, "type": "LOC", "desc": f"🧹스마트줍줍({i})"})
                else:
                    if star_price > 0 and q_qty > 0:
                        core_orders.append({"side": "SELL", "price": star_price, "qty": q_qty, "type": "LOC", "desc": "🌟별값매도"})
                    if target_price > 0 and rem_qty > 0:
                        core_orders.append({"side": "SELL", "price": target_price, "qty": rem_qty, "type": "LIMIT", "desc": "🎯목표매도"})

            core_orders, bonus_orders, smart_core_orders, smart_bonus_orders = apply_wash_trade_shield(core_orders, bonus_orders, smart_core_orders, smart_bonus_orders)        
            orders = core_orders + bonus_orders
            
            return {
                "orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders,
                "smart_core_orders": smart_core_orders, "smart_bonus_orders": smart_bonus_orders,
                "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status,
                "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio,
                "real_cash_used": real_available_cash
            }
