# ==========================================================
# [volatility_engine.py] - 🌟 100% 통합 무결점 완성본 🌟
# ⚠️ V3.2 패치: 기초지수 1년 ATR 절대 진폭 고정 및 공포지수 방향타 스위치 엔진 탑재
# 💡 [V24.09 패치] 야후 파이낸스 교착(Deadlock) 방어용 timeout=5 전면 이식 완료
# 💡 [V24.11 패치] 클래스 래퍼(VolatilityEngine) 구조 도입 및 calculate_weight 공통 인터페이스 신설
# 🚨 [PEP 8 포맷팅 패치] 미사용 변수(weight) 100% 소각 (Ruff F841 교정 완료)
# 🚨 [V27.17 그랜드 수술] 코파일럿 합작 - 가중치 무제한 폭주(Black Swan) 락온 방어(0.5~2.0), 
# UnboundLocalError 런타임 즉사 교정, 임시 파일 찌꺼기(Disk Leak) 소각, 
# 야후 파이낸스 다중인덱스(MultiIndex) 붕괴 스마트 우회 엔진 및 ATR 최소 데이터 검증망 이식
# NEW: [V40.XX 옴니 매트릭스] 60MA/120MA 듀얼 모멘텀 시장 국면 판별 엔진 탑재
# ==========================================================
import yfinance as yf
import pandas as pd
import numpy as np
import os
import json
import tempfile
import logging
import asyncio
from zoneinfo import ZoneInfo
from datetime import datetime

CACHE_FILE = "data/volatility_cache.json"

# 🚨 [수술 완료] 블랙스완/극저변동성 발생 시 계좌 직사 및 API Reject를 막기 위한 가중치 절대 상/하한선 (Bug #1)
WEIGHT_MIN = 0.5   
WEIGHT_MAX = 2.0   

# 🚨 [수술 완료] 구조적 시장 변화에 대응하기 위한 기준 ATR 상수화 (Bug #5)
QQQ_DEFAULT_ATR_PCT  = 1.65   
SOXX_DEFAULT_ATR_PCT = 2.93   
MIN_ATR_ROWS = 14  

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ 🚨 [수술 완료] 야후 파이낸스 API 업데이트로 인한 MultiIndex 순서 붕괴 방어 (Bug #4) """
    if isinstance(df.columns, pd.MultiIndex):
        if 'Ticker' in df.columns.names:
            df.columns = df.columns.droplevel('Ticker')
        elif df.columns.nlevels == 2:
            price_fields = {'Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close'}
            level0_vals = set(df.columns.get_level_values(0))
            drop_level = 0 if not level0_vals.intersection(price_fields) else 1
            df.columns = df.columns.droplevel(drop_level)
    return df

def _load_cache(key, default_val):
    """ 🛡️ 통신 장애 시 직전 영업일의 1년 평균값을 로드하는 1차 방어막 """
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)
                val = data.get(key)
                if val is not None and float(val) > 0:
                    return float(val)
        except Exception:
            pass
    return default_val

def _save_cache(key, value):
    """ 🛡️ 원자적 쓰기(fsync)를 통해 무결성이 보장된 로컬 캐시 저장 """
    data = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)
        except Exception:
            pass
    
    data[key] = value
    
    dir_name = os.path.dirname(CACHE_FILE)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
        
    # 🚨 [수술 완료] 에러 시 임시 파일 찌꺼기(Disk Leak) 영구 소각 방어막 이식 (Bug #3)
    fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CACHE_FILE)
    except Exception as e:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        logging.error(f"⚠️ [Engine] 캐시 저장 실패 및 임시 파일 소각: {e}")

def _calculate_1y_atr(ticker, cache_key, default_atr):
    """ 💡 기초지수의 최근 1년(252일) ATR14 평균값을 동적으로 연산하여 반환 """
    try:
        df = yf.download(ticker, period="2y", interval="1d", progress=False, timeout=5)
        if df.empty:
            return _load_cache(cache_key, default_atr)
            
        df = _flatten_columns(df)
                
        df['Prev_Close'] = df['Close'].shift(1)
        
        tr1 = df['High'] - df['Low']
        tr2 = (df['High'] - df['Prev_Close']).abs()
        tr3 = (df['Low'] - df['Prev_Close']).abs()
        
        df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['ATR14'] = df['TR'].rolling(window=14).mean()
        df['ATR14_pct'] = (df['ATR14'] / df['Close']) * 100
        
        df_valid = df.dropna(subset=['ATR14_pct'])
        df_1y = df_valid.tail(252)
        
        # 🚨 [수술 완료] 최소 14일 이상의 데이터가 보장되지 않으면 캐시 폴백 (Bug #5)
        if df_1y.empty or len(df_1y) < MIN_ATR_ROWS:
            logging.warning(f"⚠️ [Engine] {ticker} ATR 데이터 부족 ({len(df_1y)}행 < {MIN_ATR_ROWS}): 캐시/기본값 사용")
            return _load_cache(cache_key, default_atr)
            
        atr_1y_avg = float(df_1y['ATR14_pct'].mean())
        if pd.isna(atr_1y_avg) or atr_1y_avg <= 0:
            raise ValueError("Invalid ATR")
            
        _save_cache(cache_key, atr_1y_avg)
        return atr_1y_avg
        
    except Exception as e:
        logging.error(f"⚠️ [Engine] {ticker} ATR 연산 오류: {e}")
        return _load_cache(cache_key, default_atr)

def get_tqqq_target_drop():
    """ [ TQQQ 스나이퍼 ] 실시간 VXN과 QQQ 1년 ATR을 결합하여 타격선 계산 """
    try:
        vxn_data = yf.download("^VXN", period="2y", interval="1d", progress=False, timeout=5)
        if vxn_data.empty: 
            return round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
            
        vxn_data = _flatten_columns(vxn_data)
                
        valid_closes = vxn_data['Close'].dropna()
        valid_closes_1y = valid_closes.tail(252)
        
        if valid_closes_1y.empty:
            return round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
            
        try:
            mean_vxn = float(valid_closes_1y.mean())
            if pd.isna(mean_vxn) or mean_vxn <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("VXN_MEAN", mean_vxn)
        except Exception:
            # 🚨 [수술 완료] UnboundLocalError 런타임 즉사 버그 교정 (반환값 정상 할당)
            mean_vxn = _load_cache("VXN_MEAN", 20.0)
        
        # 💡 [V3.2 패치] 1배수 기초지수 QQQ의 1년 ATR * 3배 동적 스케일링 (가중치 배제 절대 진폭 고정)
        qqq_1y_atr = _calculate_1y_atr("QQQ", "QQQ_ATR_1Y", QQQ_DEFAULT_ATR_PCT)
        base_amp = round(-(qqq_1y_atr * 3), 2)
        
        target_drop = base_amp
        return target_drop
        
    except Exception as e:
        logging.error(f"❌ VXN 스캔 오류: {e}")
        return round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)

def get_soxl_target_drop():
    """ [ SOXL 스나이퍼 ] SOXX HV와 SOXX 1년 ATR을 결합하여 타격선 계산 """
    try:
        soxx_data = yf.download("SOXX", period="2y", interval="1d", progress=False, timeout=5)
        if soxx_data.empty or len(soxx_data) < 21: 
            return round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
        
        soxx_data = _flatten_columns(soxx_data)
                
        closes = soxx_data['Close'].dropna()
        log_returns = np.log(closes / closes.shift(1))
        hv_20d = log_returns.rolling(window=20).std() * np.sqrt(252) * 100
        
        valid_hvs = hv_20d.dropna()
        valid_hvs_1y = valid_hvs.tail(252)
        
        if valid_hvs_1y.empty:
            return round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
            
        try:
            mean_hv = float(valid_hvs_1y.mean())
            if pd.isna(mean_hv) or mean_hv <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("SOXX_HV_MEAN", mean_hv)
        except Exception:
            # 🚨 [수술 완료] UnboundLocalError 런타임 즉사 버그 교정 (반환값 정상 할당)
            mean_hv = _load_cache("SOXX_HV_MEAN", 25.0)
        
        # 💡 [V3.2 패치] 1배수 기초지수 SOXX의 1년 ATR * 3배 동적 스케일링 (가중치 배제 절대 진폭 고정)
        soxx_1y_atr = _calculate_1y_atr("SOXX", "SOXX_ATR_1Y", SOXX_DEFAULT_ATR_PCT)
        base_amp = round(-(soxx_1y_atr * 3), 2)
        
        target_drop = base_amp
        return target_drop
        
    except Exception as e:
        logging.error(f"❌ SOXX HV 연산 오류: {e}")
        return round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)

def get_tqqq_target_drop_full():
    """ 💡 [텔레그램 UI 표시용] TQQQ 상세 데이터 반환 (4개 파라미터 리턴) """
    try:
        vxn_data = yf.download("^VXN", period="2y", interval="1d", progress=False, timeout=5)
        
        if vxn_data.empty: 
            fallback_amp = round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
            
        vxn_data = _flatten_columns(vxn_data)
                
        valid_closes = vxn_data['Close'].dropna()
        valid_closes_1y = valid_closes.tail(252)
        
        if valid_closes_1y.empty:
            fallback_amp = round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
            
        current_vxn = float(valid_closes_1y.iloc[-1])
        
        try:
            mean_vxn = float(valid_closes_1y.mean())
            if pd.isna(mean_vxn) or mean_vxn <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("VXN_MEAN", mean_vxn)
        except Exception:
            mean_vxn = _load_cache("VXN_MEAN", 20.0)
            
        # 🚨 [수술 완료] 블랙스완 가중치 무한대 폭주 락온 (Bug #1)
        if mean_vxn <= 0:
            weight = 1.0
        else:
            raw_weight = current_vxn / mean_vxn
            weight = max(WEIGHT_MIN, min(WEIGHT_MAX, raw_weight))
        
        qqq_1y_atr = _calculate_1y_atr("QQQ", "QQQ_ATR_1Y", QQQ_DEFAULT_ATR_PCT)
        base_amp = round(-(qqq_1y_atr * 3), 2)
        target_drop = base_amp
        
        return current_vxn, weight, target_drop, base_amp
        
    except Exception as e:
        logging.error(f"❌ VXN 상세 스캔 오류: {e}")
        fallback_amp = round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
        return 0.0, 1.0, fallback_amp, fallback_amp

def get_soxl_target_drop_full():
    """ 💡 [텔레그램 UI 표시용] SOXL 상세 데이터 반환 (4개 파라미터 리턴) """
    try:
        soxx_data = yf.download("SOXX", period="2y", interval="1d", progress=False, timeout=5)
        if soxx_data.empty or len(soxx_data) < 21: 
            fallback_amp = round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
        
        soxx_data = _flatten_columns(soxx_data)
                
        closes = soxx_data['Close'].dropna()
        log_returns = np.log(closes / closes.shift(1))
        hv_20d = log_returns.rolling(window=20).std() * np.sqrt(252) * 100
        
        valid_hvs = hv_20d.dropna()
        valid_hvs_1y = valid_hvs.tail(252)
        
        if valid_hvs_1y.empty:
            fallback_amp = round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
            return 0.0, 1.0, fallback_amp, fallback_amp
            
        latest_hv = float(valid_hvs_1y.iloc[-1])
        
        try:
            mean_hv = float(valid_hvs_1y.mean())
            if pd.isna(mean_hv) or mean_hv <= 0:
                raise ValueError("Invalid Mean")
            _save_cache("SOXX_HV_MEAN", mean_hv)
        except Exception:
            mean_hv = _load_cache("SOXX_HV_MEAN", 25.0)
        
        # 🚨 [수술 완료] 블랙스완 가중치 무한대 폭주 락온 (Bug #1)
        if mean_hv <= 0:
            weight = 1.0
        else:
            raw_weight = latest_hv / mean_hv
            weight = max(WEIGHT_MIN, min(WEIGHT_MAX, raw_weight))
        
        soxx_1y_atr = _calculate_1y_atr("SOXX", "SOXX_ATR_1Y", SOXX_DEFAULT_ATR_PCT)
        base_amp = round(-(soxx_1y_atr * 3), 2)
        target_drop = base_amp
        
        return latest_hv, weight, target_drop, base_amp
        
    except Exception as e:
        logging.error(f"❌ SOXX HV 상세 연산 오류: {e}")
        fallback_amp = round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
        return 0.0, 1.0, fallback_amp, fallback_amp

# NEW: [V40.XX 옴니 매트릭스] 60MA/120MA 시장 국면 판별 엔진 동기 처리부
def _fetch_market_regime_sync() -> dict:
    """
    야후 파이낸스에서 SOXX 데이터를 동기적으로 다운로드하여 MA 라인을 연산합니다.
    비동기 루프 블로킹 방지를 위해 반드시 asyncio.to_thread로 래핑되어 호출되어야 합니다.
    """
    try:
        # 120일선을 구하기 위해 넉넉하게 150영업일 데이터 호출
        ticker = yf.Ticker("SOXX")
        df = ticker.history(period="150d", interval="1d")
        
        if df.empty or len(df) < 120:
            return {"status": "error", "msg": "YF 데이터 부족 (120MA 연산 불가)"}
            
        # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각 방어막]
        # 야후 파이낸스 MultiIndex 붕괴 우회 엔진 적용
        df = _flatten_columns(df)
            
        # 60일, 120일 이동평균선 연산
        df['60MA'] = df['Close'].rolling(window=60).mean()
        df['120MA'] = df['Close'].rolling(window=120).mean()
        
        # Look-ahead Bias 방지를 위해 가장 최근에 확정된(종료된) 영업일의 데이터를 추출
        # 10:00 KST에 호출되므로, iloc[-1]은 완벽하게 '어제(미국 기준)'의 종가를 의미함
        last_row = df.iloc[-1]
        
        # API 결측치(None) 및 타입 오류로 인한 런타임 붕괴 방지 (Safe Casting)
        close_price = float(last_row['Close']) if not pd.isna(last_row['Close']) else 0.0
        ma60 = float(last_row['60MA']) if not pd.isna(last_row['60MA']) else 0.0
        ma120 = float(last_row['120MA']) if not pd.isna(last_row['120MA']) else 0.0
        
        if close_price == 0.0 or ma60 == 0.0 or ma120 == 0.0:
            return {"status": "error", "msg": "결측치(NaN) 유입으로 판별 불가"}

        # 듀얼 모멘텀 판별 로직
        if close_price > ma60:
            regime = "BULL"
            target_ticker = "SOXL"
        elif close_price < ma120:
            regime = "BEAR"
            target_ticker = "SOXS"
        else:
            # 60MA와 120MA 사이의 휩소(Whipsaw) 구간 -> 양방향 셧다운 (Bypass)
            regime = "SIDEWAYS"
            target_ticker = "NONE"
            
        return {
            "status": "success",
            "regime": regime,
            "target_ticker": target_ticker,
            "close": close_price,
            "ma60": ma60,
            "ma120": ma120
        }
        
    except Exception as e:
        return {"status": "error", "msg": str(e)}

# NEW: [V40.XX 옴니 매트릭스] 시장 국면 판별 비동기 래퍼 (스케줄러 호출용)
async def determine_market_regime() -> dict:
    """
    비동기 데드락 원천 차단 방어막이 씌워진 시장 국면 판별 함수입니다.
    매일 특정 스케줄(마스터 스위치 판별 시간 등)에 호출되어 당일의 운명(SOXL/SOXS/NONE)을 락온합니다.
    """
    try:
        # 최대 10초 무한 대기 족쇄(Timeout) 가동
        result = await asyncio.wait_for(
            asyncio.to_thread(_fetch_market_regime_sync),
            timeout=10.0
        )
        return result
    except asyncio.TimeoutError:
        return {"status": "error", "msg": "YF 통신 타임아웃 (10초 초과)"}
    except Exception as e:
        return {"status": "error", "msg": f"비동기 래핑 오류: {str(e)}"}


class VolatilityEngine:
    def __init__(self):
        pass
        
    def calculate_weight(self, ticker):
        """ 
        main.py의 scheduled_volatility_scan 함수가 호출하는 공통 인터페이스.
        기존 0.85/1.15 하드코딩을 대체하여 팩트 기반의 가중치를 반환합니다.
        """
        try:
            if ticker == "TQQQ":
                _, weight, _, _ = get_tqqq_target_drop_full()
            elif ticker == "SOXL":
                _, weight, _, _ = get_soxl_target_drop_full()
            else:
                weight = 1.0

            # 🚨 [수술 완료] 최종 안전망: 메인 관제탑으로 넘어가기 전 한 번 더 강력한 Clamp 적용
            clamped = max(WEIGHT_MIN, min(WEIGHT_MAX, float(weight)))
            return {'weight': clamped}

        except Exception as e:
            logging.error(f"⚠️ [VolatilityEngine] {ticker} 가중치 산출 래퍼 오류: {e}")
            return {'weight': 1.0}
