# ==========================================================
# [telegram_avwap_console.py] - 🌟 V43.07 신규 AVWAP 독립 관제탑 플러그인 🌟
# 🚨 NEW: 통합지시서(/sync)의 과부하를 막기 위해 AVWAP 듀얼 모멘텀 레이더를 분리 독립시킴.
# 🚨 MODIFIED: [V43.07] 당일 저가(Day Low) 0점 앵커 기반 ATR5/ATR14 체력 소진율 시각화 바(Bar) 이식.
# 🚨 NEW: [V43.07] 체력 소진율(90%, 80%, 70%)에 따른 목표 수익률 자율주행(Auto) 엔진 및 스위치 장착.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class AvwapConsolePlugin:
    def __init__(self, config, broker, strategy, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.tx_lock = tx_lock

    async def get_console_message(self, app_data):
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        active_tickers = self.cfg.get_active_tickers()
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        if "SOXL" in avwap_tickers:
            avwap_tickers.append("SOXS")
            
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
            
        active_avwap = [t for t in avwap_tickers if self.cfg.get_avwap_hybrid_mode("SOXL" if t == "SOXS" else t)]
        if not active_avwap:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ <code>/settlement</code> 메뉴에서 AVWAP 하이브리드 모드를 켜주세요.", None

        tracking_cache = app_data.get('sniper_tracking', {})
        
        # 1. 기초자산(SOXX) 모멘텀 스캔
        base_tkr = "SOXX"
        base_prev_vwap, base_curr_vwap = 0.0, 0.0
        avg_vwap_5m = 0.0
        try:
            base_prev_vwap, base_curr_vwap = await asyncio.wait_for(
                asyncio.to_thread(self.broker.get_daily_vwap_info, base_tkr), timeout=4.0
            )
            df_1m = await asyncio.wait_for(
                asyncio.to_thread(self.broker.get_1min_candles_df, base_tkr), timeout=4.0
            )
            if df_1m is not None and not df_1m.empty:
                df = df_1m.copy()
                df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                df['vol'] = df['volume'].astype(float)
                df['vol_tp'] = df['tp'] * df['vol']
                
                recent_5 = df.tail(5)
                sum_vol_5 = recent_5['vol'].sum()
                if sum_vol_5 > 0:
                    avg_vwap_5m = recent_5['vol_tp'].sum() / sum_vol_5
        except Exception as e:
            logging.error(f"AVWAP 관제탑 기초자산 스캔 에러: {e}")

        msg = f"🔫 <b>[ 차세대 AVWAP 듀얼 모멘텀 관제탑 ]</b>\n\n"
        msg += f"🏛️ <b>[ 기초자산 ({base_tkr}) 모멘텀 ]</b>\n"
        if base_prev_vwap > 0:
            msg += f"▫️ 전일 VWAP: ${base_prev_vwap:,.2f}\n"
            rt_gap = ((base_curr_vwap - base_prev_vwap) / base_prev_vwap) * 100
            msg += f"▫️ 실시간 VWAP: ${base_curr_vwap:,.2f} ({rt_gap:+.2f}%)\n"
            if avg_vwap_5m > 0 and base_curr_vwap > 0:
                avg_5m_gap = ((avg_vwap_5m - base_curr_vwap) / base_curr_vwap) * 100
                msg += f"▫️ 5분 평균 VWAP: ${avg_vwap_5m:,.2f} ({avg_5m_gap:+.2f}%)\n"
        else:
            msg += f"▫️ 실시간 VWAP: ${base_curr_vwap:,.2f}\n"

        keyboard = []

        # 2. 롱/숏 개별 종목 팩트 스캔 및 체력 렌더링
        for t in active_avwap:
            curr_p = await asyncio.to_thread(self.broker.get_current_price, t)
            prev_c = await asyncio.to_thread(self.broker.get_previous_close, t)
            day_high, day_low = await asyncio.to_thread(self.broker.get_day_high_low, t)
            atr5, atr14 = await asyncio.to_thread(self.broker.get_atr_data, t)
            
            curr_p = float(curr_p) if curr_p else 0.0
            prev_c = float(prev_c) if prev_c else 0.0
            day_low = float(day_low) if day_low else prev_c
            
            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            
            is_multi = getattr(self.cfg, 'get_avwap_multi_strike_mode', lambda x: False)(t)
            user_target_pct = getattr(self.cfg, 'get_avwap_target_profit', lambda x: 4.0)(t)
            target_mode = tracking_cache.get(f"AVWAP_TARGET_MODE_{t}", "AUTO") # 기본값 자율주행
            
            label = "롱" if t == "SOXL" else "숏"
            msg += f"\n🎯 <b>[ {t} ({label}) 작전반 ]</b>\n"

            if base_prev_vwap > 0 and base_curr_vwap > 0 and avg_vwap_5m > 0:
                if t == "SOXS":
                    momentum_color = "🟢" if base_curr_vwap < base_prev_vwap and avg_vwap_5m < base_curr_vwap else "🔴"
                    trend_str = "하락 돌파 (진입허용)" if base_curr_vwap < base_prev_vwap and avg_vwap_5m < base_curr_vwap else "조건 미달 (대기)"
                    msg += f"▫️ 모멘텀: {momentum_color} {trend_str}\n"
                else:
                    momentum_color = "🟢" if base_curr_vwap > base_prev_vwap and avg_vwap_5m > base_curr_vwap else "🔴"
                    trend_str = "상승 돌파 (진입허용)" if base_curr_vwap > base_prev_vwap and avg_vwap_5m > base_curr_vwap else "조건 미달 (대기)"
                    msg += f"▫️ 모멘텀: {momentum_color} {trend_str}\n"

            msg += f"▫️ 독립 물량/평단: {avwap_qty}주 / ${avwap_avg:.2f}\n"

            # 🚨 [V43.07] 당일 저가(Day Low) 0점 앵커 기반 체력 소진율 연산
            exh_5 = 0.0
            if atr5 > 0 and atr14 > 0 and prev_c > 0 and day_low > 0:
                ref_price = avwap_avg if (avwap_qty > 0 and avwap_avg > 0) else curr_p
                ref_label = "매수평단" if (avwap_qty > 0 and avwap_avg > 0) else "현재가"
                
                atr5_price = prev_c * (atr5 / 100.0)
                atr14_price = prev_c * (atr14 / 100.0)
                
                atr5_limit = day_low + atr5_price
                atr14_limit = day_low + atr14_price
                
                exh_5 = ((ref_price - day_low) / atr5_price * 100) if atr5_price > 0 else 0
                exh_14 = ((ref_price - day_low) / atr14_price * 100) if atr14_price > 0 else 0
                
                def make_bar(exh):
                    pos = min(9, max(0, int(exh / 10)))
                    return "━" * pos + "🎯" + "━" * (9 - pos)
                
                msg += f"▫️ 0점 앵커(당일 저가): <b>${day_low:.2f}</b>\n"
                msg += f"▫️ {ref_label} 위치: <b>${ref_price:.2f}</b>\n\n"
                
                msg += f"🔋 <b>단기 체력 (ATR5: ${atr5_limit:.2f} 한계)</b>\n"
                msg += f"   [0%] {make_bar(exh_5)} [100%] <b>({exh_5:.0f}% 소진)</b>\n"
                
                msg += f"🔋 <b>중기 체력 (ATR14: ${atr14_limit:.2f} 한계)</b>\n"
                msg += f"   [0%] {make_bar(exh_14)} [100%] <b>({exh_14:.0f}% 소진)</b>\n"
                
                if exh_5 >= 90:
                    msg += " ⚠️ <i>[경고] 일일 단기 체력 90% 소진. 휩소 방어를 위해 익절라인 하향 조정 권장!</i>\n"

            # 🚨 [V43.07] 체력 소진율 연동 자율주행 수익률 산출
            if target_mode == "AUTO":
                if exh_5 >= 90: dynamic_target = 2.0
                elif exh_5 >= 80: dynamic_target = 3.0
                elif exh_5 >= 70: dynamic_target = 4.0
                else: dynamic_target = user_target_pct
                target_display = f"🤖자율주행 (+{dynamic_target:.1f}%)"
            else:
                target_display = f"🖐️수동고정 (+{user_target_pct:.1f}%)"

            msg += f"▫️ 목표 익절: <b>{target_display}</b> | 하드스탑: <b>-8.0%</b>\n"

            status_txt = "👀 타점 대기"
            if is_shutdown: status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
            elif avwap_qty > 0: status_txt = "🎯 딥매수 완료 (익절 감시중)"
            msg += f"▫️ 상태: <b>{status_txt}</b>\n"

            # 콘솔 버튼 렌더링
            toggle_target_label = "🤖 익절 자율주행 모드 전환" if target_mode == "MANUAL" else "🖐️ 수동 고정 모드 전환"
            toggle_target_action = "TARGET_AUTO" if target_mode == "MANUAL" else "TARGET_MANUAL"

            row1 = [
                InlineKeyboardButton(f"🎯 {t} 수동 목표(%)", callback_data=f"AVWAP_SET:TARGET:{t}"),
                InlineKeyboardButton(toggle_target_label, callback_data=f"AVWAP_SET:{toggle_target_action}:{t}")
            ]
            
            strike_icon_btn = f"💼 {t} 조기퇴근 모드" if is_multi else f"🔁 {t} 다중출장 모드"
            strike_action = "EARLY" if is_multi else "MULTI"
            row2 = [InlineKeyboardButton(strike_icon_btn, callback_data=f"AVWAP_SET:{strike_action}:{t}")]
            
            keyboard.append(row1)
            keyboard.append(row2)

        keyboard.append([InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE")])
        keyboard.append([InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")])

        return msg, InlineKeyboardMarkup(keyboard)
