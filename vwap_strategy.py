# ==========================================================
# [vwap_strategy.py]
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# ==========================================================
import math
import pytz
from datetime import datetime

class VwapStrategy:
    def __init__(self, config):
        self.cfg = config
        
        # 💡 [VWAP 코어] 장 마감 30분(15:30~15:59) 전용 역사적 거래량 유동성 프로파일 (U-Curve 꼬리)
        # 종가 부근으로 갈수록 ETF 리밸런싱 및 데이트레이더 청산으로 거래량이 기하급수적으로 폭증하는 패턴 반영
        raw_profile = [
            0.010, 0.011, 0.012, 0.013, 0.014,  # 15:30 ~ 15:34
            0.015, 0.016, 0.018, 0.020, 0.022,  # 15:35 ~ 15:39
            0.025, 0.028, 0.031, 0.035, 0.039,  # 15:40 ~ 15:44
            0.043, 0.048, 0.053, 0.059, 0.065,  # 15:45 ~ 15:49
            0.071, 0.078, 0.085, 0.093, 0.101,  # 15:50 ~ 15:54
            0.110, 0.120, 0.131, 0.143, 0.160   # 15:55 ~ 15:59
        ]
        
        # 💡 가중치 정규화 (전체 합이 1.0이 되도록 교정)
        total_weight = sum(raw_profile)
        self.vol_profile = [round(w / total_weight, 4) for w in raw_profile]

    def _get_current_bin_index(self):
        est = pytz.timezone('US/Eastern')
        now = datetime.now(est)
        
        # 💡 장 마감 30분 전 타임 윈도우 락온 검증
        if now.hour == 15 and 30 <= now.minute <= 59:
            return now.minute - 30
        return -1

    def get_vwap_plan(self, ticker, current_price, remaining_target, side="BUY"):
        """
        [VWAP 동적 슬라이싱 엔진]
        remaining_target: BUY일 경우 '남은 매수 예산(USD)', SELL일 경우 '남은 매도 수량(주)'
        """
        bin_idx = self._get_current_bin_index()
        
        if bin_idx == -1 or current_price <= 0:
            return {
                "orders": [], 
                "process_status": "⏳VWAP대기/종료", 
                "allocated_qty": 0,
                "bin_weight": 0.0
            }
            
        current_weight = self.vol_profile[bin_idx]
        remaining_weight = sum(self.vol_profile[bin_idx:])
        
        # 💡 ZeroDivision 방어
        if remaining_weight <= 0:
            remaining_weight = 1.0
            
        # 💡 남은 시간 대비 현재 분(Minute)의 상대적 할당 비율 연산
        slice_ratio = current_weight / remaining_weight
        
        orders = []
        process_status = f"🎯VWAP({bin_idx+1}/30분)"
        allocated_qty = 0
        
        if side == "BUY":
            # 예산 기반 분할 (Budget Slicing)
            slice_budget = remaining_target * slice_ratio
            allocated_qty = math.floor(slice_budget / current_price)
            
            if allocated_qty > 0:
                # 💡 스케줄러가 실시간 매도 1호가(Ask)로 덮어씌우므로, 기준가(Fallback)는 왜곡 없이 순수 현재가로 세팅
                safe_price = max(0.01, round(current_price, 2)) 
                orders.append({
                    "side": "BUY", 
                    "price": safe_price, 
                    "qty": allocated_qty, 
                    "type": "LIMIT", 
                    "desc": f"🎯VWAP매수({bin_idx+1})"
                })
                
        elif side == "SELL":
            # 수량 기반 분할 (Quantity Slicing)
            allocated_qty = math.floor(remaining_target * slice_ratio)
            
            # 자연 종료 원칙 엄수: 마지막 분(15:59)에도 억지 시장가(MOC) 스윕 없이 지정가로만 타격
            if allocated_qty > 0:
                # 💡 스케줄러가 실시간 매수 1호가(Bid)로 덮어씌우므로, 기준가(Fallback)는 왜곡 없이 순수 현재가로 세팅
                safe_price = max(0.01, round(current_price, 2))
                orders.append({
                    "side": "SELL", 
                    "price": safe_price, 
                    "qty": allocated_qty, 
                    "type": "LIMIT", 
                    "desc": f"🎯VWAP매도({bin_idx+1})"
                })

        return {
            "orders": orders,
            "process_status": process_status,
            "allocated_qty": allocated_qty,
            "bin_weight": current_weight
        }

