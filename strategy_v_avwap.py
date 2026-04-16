# ==========================================================
# [strategy_v_avwap.py]
# 💡 V-REV 하이브리드 전용 차세대 AVWAP 스나이퍼 플러그인 (Dual-Referencing)
# ⚠️ 초공격형 당일 청산 암살자 (V-REV 잉여 현금 100% 몰빵 & -3% 하드스탑)
# ⚠️ 옵션 B 아키텍처: 기초자산(SOXX) 시그널 스캔 + 파생상품(SOXL) 미시구조 타격
# 🚨 [PEP 8 포맷팅 패치] 미사용 변수(time_0930) 소각 (Ruff F841 교정 완료)
# 🚨 [V25.23 디커플링] KIS API 하드코딩 종속성 적출 및 범용 1분봉 컬럼 정규화 완비
# 🚨 [V27.06 긴급 수술] NameError (#ffffff) 소각 및 ZeroDivision 방어막 구축
# 🚨 [V27.07 그랜드 수술] 코파일럿 합작 - 20MA NaN 붕괴, VWAP 침묵, 10시 누수, 소수점 주문 4대 맹점 전면 철거
# ==========================================================
import logging
import datetime
import pytz
import math
import yfinance as yf
import pandas as pd

class VAvwapHybridPlugin:
    def __init__(self):
        self.plugin_name = "AVWAP_HYBRID_DUAL"
        self.leverage = 3.0             
        self.base_stop_loss_pct = 0.01  
        self.base_target_pct = 0.01     
        self.base_dip_buy_pct = 0.0067  
        
    def fetch_macro_context(self, base_ticker):
        try:
            tkr = yf.Ticker(base_ticker)
            df_daily = tkr.history(period="2mo", interval="1d", timeout=5)
            df_30m = tkr.history(period="60d", interval="30m", timeout=5)

            # 🚨 [수술 완료] 20MA의 iloc[-2] 연산을 위해 최소 21일치 데이터 보장 (NaN 붕괴 원천 차단)
            if df_daily.empty or len(df_daily) < 21 or df_30m.empty:
                return None

            prev_close = float(df_daily['Close'].iloc[-2])
            ma_20 = float(df_daily['Close'].rolling(window=20).mean().iloc[-2])

            # 🚨 [수술 완료] 연산 결과가 NaN일 경우 컨텍스트 무효화
            if math.isnan(ma_20) or math.isnan(prev_close):
                return None

            if df_30m.index.tz is None:
                df_30m.index = df_30m.index.tz_localize('UTC').tz_convert('US/Eastern')
            else:
                df_30m.index = df_30m.index.tz_convert('US/Eastern')

            first_30m = df_30m[df_30m.index.time == datetime.time(9, 30)]
            today_est = datetime.datetime.now(pytz.timezone('US/Eastern')).date()
            past_first_30m = first_30m[first_30m.index.date < today_est]
            
            if len(past_first_30m) >= 20:
                avg_vol_20 = float(past_first_30m['Volume'].tail(20).mean())
            elif len(past_first_30m) > 0:
                avg_vol_20 = float(past_first_30m['Volume'].mean())
            else:
                avg_vol_20 = 0.0

            return {
                "prev_close": prev_close,
                "ma_20": ma_20,
                "avg_vol_20": avg_vol_20
            }
            
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 실패 ({base_ticker}): {e}")
            return None

    def get_decision(self, base_ticker, exec_ticker, base_curr_p, exec_curr_p, base_day_open, avwap_avg_price, avwap_qty, avwap_alloc_cash, context_data, df_1min_base, now_est):
        curr_time = now_est.time()
        
        time_1000 = datetime.time(10, 0)
        time_1400 = datetime.time(14, 0)
        time_1430 = datetime.time(14, 30)
        time_1555 = datetime.time(15, 55)

        base_vwap = base_curr_p
        base_current_30m_vol = 0.0
        vwap_success = False # 🚨 [수술 완료] VWAP 연산 성공 여부 플래그
        
        if df_1min_base is not None and not df_1min_base.empty:
            try:
                df = df_1min_base.copy()
                df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                df['vol'] = df['volume'].astype(float)
                df['vol_tp'] = df['tp'] * df['vol']
                
                cum_vol = df['vol'].sum()
                if cum_vol > 0:
                    base_vwap = df['vol_tp'].sum() / cum_vol
                    vwap_success = True
                
                # 🚨 [수술 완료] 문자열 규격화(zfill) 및 10:00 캔들 누수 차단('100000')
                if 'time_est' in df.columns:
                    df['time_est'] = df['time_est'].astype(str).str.zfill(6)
                    mask_30m = (df['time_est'] >= '093000') & (df['time_est'] < '100000')
                    base_current_30m_vol = df.loc[mask_30m, 'vol'].sum()
            except Exception as e:
                logging.error(f"🚨 [V_AVWAP] 기초자산 1분봉 VWAP 연산 실패: {e}")

        # 🚨 [수술 완료] VWAP 연산 실패 시 침묵(Phantom Buy) 방지 및 매수 동결
        if not vwap_success and avwap_qty == 0:
            return {'action': 'WAIT', 'reason': 'VWAP_데이터_결측_동결', 'vwap': base_vwap}

        if avwap_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p
            exec_return = (exec_curr_p - safe_avg) / safe_avg
            base_equivalent_return = exec_return / self.leverage
            
            if base_equivalent_return <= -self.base_stop_loss_pct:
                return {'action': 'SELL', 'qty': int(avwap_qty), 'target_price': 0.0, 'reason': 'HARD_STOP_DUAL'}
            
            if curr_time >= time_1555:
                return {'action': 'SELL', 'qty': int(avwap_qty), 'target_price': 0.0, 'reason': 'TIME_STOP'}
                
            if vwap_success and curr_time >= time_1430 and base_curr_p >= base_vwap * (1 + self.base_target_pct):
                return {'action': 'SELL', 'qty': int(avwap_qty), 'target_price': 0.0, 'reason': 'SQUEEZE_TARGET_DUAL'}
                
            return {'action': 'HOLD', 'reason': '보유중_관망', 'vwap': base_vwap}

        if not context_data:
            return {'action': 'WAIT', 'reason': '매크로_데이터_수집대기', 'vwap': base_vwap}

        prev_c = context_data['prev_close']
        ma_20 = context_data['ma_20']
        avg_vol_20 = context_data['avg_vol_20']

        is_bull_regime = (prev_c > ma_20) and (base_day_open > ma_20)
        if not is_bull_regime:
            return {'action': 'SHUTDOWN', 'reason': '기초자산_역배열_하락장_영구동결', 'vwap': base_vwap}
            
        if base_day_open <= prev_c * (1 - self.base_dip_buy_pct):
            return {'action': 'SHUTDOWN', 'reason': '기초자산_시가_갭하락_영구동결', 'vwap': base_vwap}
            
        if curr_time >= time_1000:
            if avg_vol_20 > 0 and base_current_30m_vol >= (avg_vol_20 * 2.0) and base_curr_p < base_vwap:
                return {'action': 'SHUTDOWN', 'reason': '기초자산_RVOL_스파이크_영구동결', 'vwap': base_vwap}
                
        if time_1000 <= curr_time <= time_1400:
            if base_curr_p <= base_vwap * (1 - self.base_dip_buy_pct):
                # 🚨 [수술 완료] ZeroDivision 방어 및 명시적 int() 캐스팅으로 API Reject 원천 차단
                if exec_curr_p > 0 and avwap_alloc_cash > 0:
                    buy_qty = int(math.floor(avwap_alloc_cash / exec_curr_p))
                    if buy_qty > 0:
                        return {'action': 'BUY', 'qty': buy_qty, 'target_price': exec_curr_p, 'reason': 'VWAP_BOUNCE_DUAL', 'vwap': base_vwap}
                return {'action': 'WAIT', 'reason': '예산_부족_관망', 'vwap': base_vwap}
                    
        return {'action': 'WAIT', 'reason': '타점_대기중', 'vwap': base_vwap}
