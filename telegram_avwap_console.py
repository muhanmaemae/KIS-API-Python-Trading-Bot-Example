# ==========================================================
# FILE: telegram_avwap_console.py
# MODIFIED: [V44.30] AVWAP 관제탑 순수 모니터링화 (설정 제어 버튼 소각)
# MODIFIED: [V44.31] 체력 분석 기준 팩트 교정 - 현재가가 아닌 '당일 고가(High)' 기준으로 방전율 및 잔여 체력 계산 락온 완료
# NEW: [1단계 타임라인 수술] 10:00 EST 타임쉴드 버그를 10:20 EST로 절대 락온 및 UI 텍스트 팩트 교정.
# 🚨 MODIFIED: [V44.50 이벤트 루프 교착 방어] 관제탑 렌더링 시 발생하는 모든 JSON 설정 파일 스캔 및 속성 조회를 비동기 래핑 완료.
# 🚨 MODIFIED: [V44.61 팩트 교정] 관제탑 실시간 VWAP 연산 시 프리마켓 노이즈 전면 소각 및 정규장 100% 락온
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
        
        # 🚨 MODIFIED: 파일 I/O 비동기 래핑
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        if "SOXL" in avwap_tickers:
            avwap_tickers.append("SOXS")
            
        if not avwap_tickers:
             return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
        
        # 🚨 [V44.30 수술] 모드 활성화 여부 상관없이 무조건 렌더링하도록 락다운 해제
        active_avwap = avwap_tickers

        tracking_cache = app_data.get('sniper_tracking', {})
        
        # 1. 기초자산(SOXX) 모멘텀 스캔 (타임아웃 족쇄 4초)
        base_tkr = "SOXX"
        base_prev_vwap, base_curr_vwap = 0.0, 0.0
        avg_vwap_5m = 0.0
        base_day_high, base_day_low, base_prev_c = 0.0, 0.0, 0.0
        
        df_1m = None
        try:
            # 기초자산 당일 고/저/전일종가 스캔
            try:
                base_prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, base_tkr), timeout=2.0)
                base_prev_c = float(base_prev_c_val) if base_prev_c_val else 0.0
                
                base_hl = await asyncio.wait_for(asyncio.to_thread(self.broker.get_day_high_low, base_tkr), timeout=2.0)
                base_day_high = float(base_hl[0]) if base_hl else 0.0
                base_day_low = float(base_hl[1]) if base_hl else 0.0
            except Exception as e:
                logging.debug(f"🚨 기초자산 H/L/PrevC 스캔 에러: {e}")

            avwap_ctx = None
            if hasattr(self.strategy, 'v_avwap_plugin'):
                avwap_ctx = await asyncio.wait_for(
                    asyncio.to_thread(self.strategy.v_avwap_plugin.fetch_macro_context, base_tkr), timeout=4.0
                )
             
            if avwap_ctx:
                base_prev_vwap = float(avwap_ctx.get('prev_vwap', 0.0))
                
            df_1m = await asyncio.wait_for(
                asyncio.to_thread(self.broker.get_1min_candles_df, base_tkr), timeout=4.0
            )
             if df_1m is not None and not df_1m.empty:
                df = df_1m.copy()
                
                # 🚨 MODIFIED: [V44.61 팩트 수술] 관제탑 실시간 VWAP 연산 시 프리마켓 노이즈 원천 차단
                # [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
                # YF API가 프리마켓 데이터를 포함하여 반환하므로 순수 정규장 모멘텀만을 
                # 측정하기 위해 반드시 '093000' ~ '155900' 구간만 필터링해야 합니다.
                if 'time_est' in df.columns:
                    df = df[(df['time_est'] >= '093000') & (df['time_est'] <= '155900')]
                
                if not df.empty:
                    df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                    df['vol'] = df['volume'].astype(float)
                    df['vol_tp'] = df['tp'] * df['vol']
                    
                    cum_vol = df['vol'].sum()
                    if cum_vol > 0:
                        base_curr_vwap = df['vol_tp'].sum() / cum_vol
                    else:
                        base_curr_vwap = float(df['close'].iloc[-1])
                        
                    recent_5 = df.tail(5)
                    sum_vol_5 = recent_5['vol'].sum()
                    if sum_vol_5 > 0:
                        avg_vwap_5m = recent_5['vol_tp'].sum() / sum_vol_5
                    else:
                        avg_vwap_5m = base_curr_vwap
                else:
                    base_curr_vwap = float(df_1m['close'].iloc[-1])
                    avg_vwap_5m = base_curr_vwap

        except asyncio.TimeoutError:
            logging.error(f"🚨 AVWAP 관제탑 기초자산({base_tkr}) 스캔 타임아웃 발생")
        except Exception as e:
            logging.error(f"🚨 AVWAP 관제탑 기초자산 스캔 에러: {e}")

        msg = f"🔫 <b>[ 차세대 AVWAP 듀얼 모멘텀 관제탑 ]</b>\n\n"
        msg += f"🏛️ <b>[ 기초자산 ({base_tkr}) 모멘텀 스캔 ]</b>\n"
         
        if base_prev_c > 0 and base_day_high > 0 and base_day_low > 0:
            b_high_pct = ((base_day_high - base_prev_c) / base_prev_c) * 100
            b_low_pct = ((base_day_low - base_prev_c) / base_prev_c) * 100
            msg += f"▫️ 당일 고가: <b>${base_day_high:.2f}</b> ({b_high_pct:+.2f}%)\n"
            msg += f"▫️ 당일 저가: <b>${base_day_low:.2f}</b> ({b_low_pct:+.2f}%)\n"
        
        if base_prev_vwap > 0:
            msg += f"▫️ 전일 VWAP: <b>${base_prev_vwap:,.2f}</b>\n"
            rt_gap = ((base_curr_vwap - base_prev_vwap) / base_prev_vwap) * 100
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b> ({rt_gap:+.2f}%)\n"
            if avg_vwap_5m > 0 and base_curr_vwap > 0:
                avg_5m_gap = ((avg_vwap_5m - base_curr_vwap) / base_curr_vwap) * 100
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b> ({avg_5m_gap:+.2f}%)\n"
        else:
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b>\n"
            if avg_vwap_5m > 0:
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b>\n"

        keyboard = []

        for t in active_avwap:
             # 🚨 MODIFIED: 파일 I/O 속성 조회 비동기 래핑
            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), "SOXL" if t == "SOXS" else t)
            active_str = "🟢 가동 중" if is_avwap_active else "⚪ 대기 중 (OFF)"
            
            try:
                curr_p = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, t), timeout=2.0)
            except Exception: curr_p = 0.0
            
            try:
                prev_c = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, t), timeout=2.0)
            except Exception: prev_c = 0.0
            
            try:
                 day_high, day_low = await asyncio.wait_for(asyncio.to_thread(self.broker.get_day_high_low, t), timeout=2.0)
            except Exception: day_high, day_low = 0.0, 0.0
            
            try:
                atr5, _ = await asyncio.wait_for(asyncio.to_thread(self.broker.get_atr_data, t), timeout=3.0)
            except Exception: atr5 = 0.0
            
             curr_p = float(curr_p) if curr_p else 0.0
            prev_c = float(prev_c) if prev_c else 0.0
            day_high = float(day_high) if day_high else curr_p
            day_low = float(day_low) if day_low else curr_p
            
            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            
            # 🚨 MODIFIED: 파일 I/O 속성 조회 비동기 래핑
            is_multi = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_multi_strike_mode', lambda x: False), t)
            user_target_pct = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_target_profit', lambda x: 4.0), t)
             target_mode = tracking_cache.get(f"AVWAP_TARGET_MODE_{t}", "AUTO") 
            
            label = "롱" if t == "SOXL" else "숏"
            msg += f"\n🎯 <b>[ {t} ({label}) 작전반 - {active_str} ]</b>\n"

            momentum_met = False
            trend_str = "🔴 <b>조건 미달 (대기)</b>"
            
            if t == "SOXS":
                criteria = "당일VWAP &lt; 전일VWAP &amp; 5분평균 &lt; 당일VWAP"
                if base_prev_vwap > 0 and base_curr_vwap > 0 and avg_vwap_5m > 0:
                    if base_curr_vwap < base_prev_vwap and avg_vwap_5m < base_curr_vwap:
                         momentum_met = True
                        trend_str = "🟢 <b>조건 충족 (숏 타격 허용)</b>"
                    else:
                        trend_str = "🔴 <b>조건 미달 (진입 차단)</b>"
                else:
                     trend_str = "⚠️ 데이터 수집 대기 중"
            else:
                criteria = "당일VWAP &gt; 전일VWAP &amp; 5분평균 &gt; 당일VWAP"
                if base_prev_vwap > 0 and base_curr_vwap > 0 and avg_vwap_5m > 0:
                    if base_curr_vwap > base_prev_vwap and avg_vwap_5m > base_curr_vwap:
                        momentum_met = True
                        trend_str = "🟢 <b>조건 충족 (롱 타격 허용)</b>"
                    else:
                        trend_str = "🔴 <b>조건 미달 (진입 차단)</b>"
                else:
                    trend_str = "⚠️ 데이터 수집 대기 중"

            msg += f"▫️ 판별 기준: <code>{criteria}</code>\n"
            msg += f"▫️ 모멘텀 상태: {trend_str}\n"

            strike_icon_txt = "💼 무제한 출장" if is_multi else "🏠 조기퇴근(1회)"
             if strikes > 0:
                msg += f"▫️ 모드: <b>{strike_icon_txt} ({strikes}회차 교전 완료)</b>\n"
            else:
                msg += f"▫️ 모드: <b>{strike_icon_txt} 세팅됨</b>\n"

            msg += f"▫️ 독립 물량/평단: {avwap_qty}주 / ${avwap_avg:.2f}\n"

            exh_5 = 0.0
            rem_5_pct = 0.0

            if atr5 > 0 and prev_c > 0 and day_low > 0:
                ref_price = avwap_avg if (avwap_qty > 0 and avwap_avg > 0) else curr_p
                ref_label = "매수평단" if (avwap_qty > 0 and avwap_avg > 0) else "현재가"
                
                 high_pct = ((day_high - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                low_pct = ((day_low - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                curr_pct = ((ref_price - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                
                rebound_gap = ref_price - day_low if ref_price >= day_low else 0.0
                actual_rebound_pct = (rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                
                high_rebound_gap = day_high - day_low if day_high >= day_low else 0.0
                high_rebound_pct = (high_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                 curr_rebound_pct = actual_rebound_pct
                
                # 🚨 MODIFIED: [V44.31 수술] 현재가(actual_rebound_pct)가 아닌 당일 고가(high_rebound_pct) 기준으로 방전율 및 잔여 체력 계산
                exh_5 = (high_rebound_pct / atr5 * 100) if atr5 > 0 else 0
                rem_5_pct = atr5 - high_rebound_pct
                
                rem_5_str = f"+{rem_5_pct:.2f}% 추가 상승 여력" if rem_5_pct >= 0 else "체력 완전 고갈 (오버슈팅)"

                def make_bar(exh):
                    pos = min(5, max(0, math.ceil(exh / 20.0)))
                     return "━" * pos + "🎯" + "━" * (5 - pos)
                
                msg += f"\n📊 <b>[ {t} 당일 체력 정밀 분석 ]</b>\n"
                msg += f"▫️ 전일 종가: <b>${prev_c:.2f}</b> (베이스라인)\n"
                msg += f"▫️ 당일 고가: <b>${day_high:.2f}</b> ({high_pct:+.2f}%/<b>+{high_rebound_pct:.2f}%</b>)\n"
                 msg += f"▫️ 당일 저가: <b>${day_low:.2f}</b> ({low_pct:+.2f}%/<b>베이스</b>)\n"
                msg += f"▫️ {ref_label}: <b>${ref_price:.2f}</b> ({curr_pct:+.2f}%/<b>+{curr_rebound_pct:.2f}%</b>)\n\n"
                
                msg += f"🔋 <b>단기 체력 (ATR5 예상진폭: {atr5:.2f}%)</b>\n"
                msg += f"▫️ 잔여 체력: <b>{rem_5_str}</b>\n"
                msg += f"   [0%] {make_bar(exh_5)} [+{atr5:.2f}%]\n"
                msg += f"               <b>({exh_5:.0f}% 소진 / 고가 기준)</b>\n"

            if target_mode == "AUTO":
                if exh_5 >= 90: base_target = 2.0
                elif exh_5 >= 80: base_target = 3.0
                 elif exh_5 >= 70: base_target = 4.0
                else: base_target = 5.0
                
                if rem_5_pct > 0:
                    rem_cap = math.floor(rem_5_pct * 10) / 10.0
                     dynamic_target = min(base_target, rem_cap)
                    dynamic_target = max(2.0, dynamic_target)
                else:
                    dynamic_target = 2.0
                
                applied_pct = dynamic_target
                 target_display = f"🤖자율주행 (+{applied_pct:.1f}%)"
            else:
                applied_pct = user_target_pct
                target_display = f"🖐️수동고정 (+{applied_pct:.1f}%)"
                
            if avwap_qty > 0 and avwap_avg > 0:
                 locked_pct = tracking_cache.get(f"AVWAP_LOCKED_TARGET_PCT_{t}", applied_pct)
                target_price = avwap_avg * (1 + locked_pct / 100.0)
                hardstop_price = avwap_avg * (1 - 8.0 / 100.0)
                if target_mode == "AUTO":
                    target_display = f"🤖자율주행 (+{locked_pct:.1f}%)"
                 msg += f"▫️ 목표 익절: <b>${target_price:.2f}</b> ({target_display}) | 하드스탑: <b>${hardstop_price:.2f}</b> (-8.0%)\n"
            else:
                msg += f"▫️ 목표 익절: <b>{target_display}</b> | 하드스탑: <b>-8.0%</b>\n"

            status_txt = "👀 타점 스캔중"
            if not is_avwap_active:
                status_txt = "⚪ 모드 비활성 (레이더 관측 중)"
             elif is_shutdown: 
                status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
            elif avwap_qty > 0: 
                status_txt = "🎯 딥매수 완료 (익절 감시중)"
            else:
                try:
                     base_curr_p = float(df_1m['close'].iloc[-1]) if df_1m is not None and not df_1m.empty else 0.0
                    avwap_state_dict = {"strikes": strikes}
                    
                    decision = self.strategy.v_avwap_plugin.get_decision(
                        base_ticker=base_tkr,
                         exec_ticker=t,
                        base_curr_p=base_curr_p,
                        exec_curr_p=curr_p,
                        base_day_open=0.0,
                         avwap_avg_price=avwap_avg,
                        avwap_qty=avwap_qty,
                        avwap_alloc_cash=0.0,
                        context_data=avwap_ctx,
                        df_1min_base=df_1m,
                         now_est=now_est,
                        avwap_state=avwap_state_dict,
                        regime_data=None,
                        prev_close=prev_c,
                         day_low=day_low,
                        atr5=atr5
                    )
                    reason = decision.get('reason', '')
                    if reason:
                         status_txt = f"⏳ 대기 ({reason})"
                except Exception as e:
                    logging.debug(f"AVWAP 상태 텍스트 추출 에러: {e}")

            msg += f"▫️ 상태: <b>{status_txt}</b>\n"

        # 🚨 MODIFIED: [V44.30] 설정 모드 스위칭 버튼 영구 소각 (순수 모니터링 기능만 유지)
        keyboard.append([
             InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"
        msg += f"💡 <i>설정 제어는 /settlement (전술설정) 메뉴에서 가능합니다.</i>"

        return msg, InlineKeyboardMarkup(keyboard)
