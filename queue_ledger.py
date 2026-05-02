# ==========================================================
# [queue_ledger.py]
# ⚠️ 신규 역추세 엔진(V_REV) 전용 LIFO 로트(Lot) 장부 관리 모듈
# 💡 [핵심 수술] 수량 동기화(CALIB) 및 Pop 차감 로직 내 Safe Casting (None 방어) 전면 이식 완료
# 🚨 [V27.02 핫픽스] 동일 일자(Same Day) 로트(Lot) 파편화 방지 및 자동 병합(Merge) 엔진 탑재
# 🚨 [V27.02 핫픽스] CALIB_ADD (보정 추가) 시 평단가 $0.00 붕괴 버그 원천 차단
# 🚨 [V27.14 그랜드 수술] 코파일럿 합작 - Atomic Write(장부 증발 방어), Thread Lock(동시접근 덮어쓰기 차단), 유령 로트(0주) 무한루프 소각, EST 타임존 병합 통일 및 백업 자가 치유(Self-Healing) 파이프라인 완벽 구축
# 🚨 [V27.15 핫픽스] 초기화 Torn Write 방어, add_lot $0.00 주입 차단, pop_lots 미달 차감 감사 추적 및 sync_with_broker 런타임 붕괴 방어막 이식
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo('America/New_York') 이식으로 LMT 버그 차단
# ==========================================================
import os
import json
import time
import math
import threading
import shutil
import tempfile
from zoneinfo import ZoneInfo
from datetime import datetime
import logging

class QueueLedger:
    def __init__(self, file_path="data/queue_ledger.json"):
        self.file_path = file_path
        self._lock = threading.Lock()
        self._ensure_file()

    def _ensure_file(self):
        dir_name = os.path.dirname(self.file_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        if not os.path.exists(self.file_path):
            with self._lock:
                if not os.path.exists(self.file_path):
                    self._save_unsafe({})

    def _get_trading_date_str(self):
        est = ZoneInfo('America/New_York')
        return datetime.now(est).strftime("%Y-%m-%d")

    def _load_unsafe(self):
        """Must be called while holding self._lock."""
        last_exc = None
        for _ in range(3):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if not content.strip():
                        return {} 
                    return json.loads(content)
            except json.JSONDecodeError as e:
                last_exc = e
                break
            except FileNotFoundError:
                return {}
            except Exception as e:
                last_exc = e
                time.sleep(0.1)
        
        backup_path = self.file_path + ".bak"
        if os.path.exists(backup_path):
            logging.error(f"🚨 [QueueLedger] JSON 손상 감지. 백업 파일에서 복원 시도: {backup_path}")
            try:
                with open(backup_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as be:
                logging.error(f"🚨 [QueueLedger] 백업 복원도 실패: {be}")
        
        raise RuntimeError(f"🚨 [FATAL ERROR] {self.file_path} 장부 파일 읽기 실패. 데이터 유실 방지를 위해 시스템을 중단합니다. 원인: {last_exc}")

    # MODIFIED: [V44.49 tempfile 기반 멱등성 보장 원자적 쓰기 락온]
    def _save_unsafe(self, data):
        """Must be called while holding self._lock."""
        dir_name = os.path.dirname(self.file_path) or '.'
        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)

        for attempt in range(3):
            fd = None
            tmp_path = None
            try:
                fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    fd = None
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.file_path)
                tmp_path = None
                
                try: shutil.copy2(self.file_path, self.file_path + ".bak")
                except Exception: pass
                
                return
            except Exception as e:
                logging.warning(f"⚠️ [QueueLedger] 장부 저장 재시도 ({attempt+1}/3): {e}")
                if fd is not None:
                    try: os.close(fd)
                    except OSError: pass
                if tmp_path and os.path.exists(tmp_path):
                    try: os.remove(tmp_path)
                    except OSError: pass
                time.sleep(0.1)
                
        logging.error(f"🚨 [QueueLedger] 장부 저장 최종 실패: {self.file_path} — 데이터 유실 위험!")

    def get_queue(self, ticker):
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            return [lot for lot in q if int(float(lot.get("qty") or 0)) > 0]

    def get_total_qty(self, ticker):
        q = self.get_queue(ticker)
        return sum(int(float(item.get("qty") or 0)) for item in q)

    def add_lot(self, ticker, qty, price, lot_type="NORMAL"):
        qty = int(float(qty or 0))
        if qty <= 0: return
        
        price_f = float(price or 0.0)
        if price_f <= 0.0:
            logging.error(f"🚨 [QueueLedger] add_lot 중단: {ticker} — 유효하지 않은 매수 가격 (price={price}). 로트 추가 취소.")
            return
            
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(float(lot.get("qty") or 0)) > 0] 
            
            today_str = self._get_trading_date_str()
            
            if q and q[-1].get("date", "").startswith(today_str):
                old_qty = int(float(q[-1].get("qty", 0)))
                old_price = float(q[-1].get("price", 0.0))
                
                new_qty = old_qty + qty
                new_price = ((old_qty * old_price) + (qty * price_f)) / new_qty if new_qty > 0 else 0.0
                
                q[-1]["qty"] = new_qty
                q[-1]["price"] = round(new_price, 4)
                q[-1]["date"] = datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
            else:
                q.append({
                    "qty": qty,
                    "price": price_f, 
                    "date": datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"),
                    "type": lot_type
                })
                
            data[ticker] = q
            self._save_unsafe(data)

    def pop_lots(self, ticker, target_qty):
        original_target = int(float(target_qty or 0))
        target_qty = original_target
        if target_qty <= 0: return 0
        
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(float(lot.get("qty") or 0)) > 0] 
            
            popped_total = 0

            while q and target_qty > 0:
                last_lot = q[-1]
                lot_qty = int(float(last_lot.get("qty") or 0))
                
                if lot_qty == 0:
                    q.pop()
                    continue
                    
                if lot_qty <= target_qty:
                    popped = q.pop()
                    popped_qty = int(float(popped.get("qty") or 0))
                    popped_total += popped_qty
                    target_qty -= popped_qty
                else:
                    last_lot["qty"] = lot_qty - target_qty
                    popped_total += target_qty
                    target_qty = 0

            if popped_total < original_target:
                logging.error(f"🚨 [QueueLedger] pop_lots 미달: {ticker} — 요청 {original_target}주 중 {popped_total}주만 차감. 브로커 매도 수량과 장부 불일치 가능성. 즉시 sync_with_broker 실행 권고.")

            data[ticker] = q
            self._save_unsafe(data)
            return popped_total

    def sync_with_broker(self, ticker, actual_qty, actual_avg=0.0):
        with self._lock:
            data = self._load_unsafe()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(float(lot.get("qty") or 0)) > 0] 
            
            current_q_qty = sum(int(float(item.get("qty") or 0)) for item in q)
            actual_qty = int(float(actual_qty or 0))

            if current_q_qty == actual_qty:
                return False 

            today_str = self._get_trading_date_str()

            if current_q_qty < actual_qty:
                diff = actual_qty - current_q_qty
                
                calib_price = float(actual_avg or 0.0)
                
                if calib_price <= 0.0:
                    calib_price = float(q[-1].get("price", 0.0)) if q else 0.0
                
                if calib_price <= 0.0:
                    logging.error(f"🚨 [QueueLedger] sync_with_broker CALIB_ADD 중단: {ticker} — 실제 평단가 불명 (actual_avg={actual_avg}). $0 로트 주입 방지.")
                    data[ticker] = q
                    self._save_unsafe(data)
                    return True
                
                if q and q[-1].get("date", "").startswith(today_str):
                    old_qty = int(float(q[-1].get("qty", 0)))
                    old_price = float(q[-1].get("price", 0.0))
                    
                    new_qty = old_qty + diff
                    new_price = ((old_qty * old_price) + (diff * calib_price)) / new_qty if new_qty > 0 else 0.0

                    q[-1]["qty"] = new_qty
                    q[-1]["price"] = round(new_price, 4)
                    q[-1]["date"] = datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    q.append({
                        "qty": diff,
                        "price": round(calib_price, 4), 
                        "date": datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"),
                        "type": "CALIB_ADD"
                    })
            else:
                diff = current_q_qty - actual_qty
                
                while q and diff > 0:
                    last_lot = q[-1]
                    lot_qty = int(float(last_lot.get("qty") or 0))
                    
                    if lot_qty == 0:
                        q.pop()
                        continue
                        
                    if lot_qty <= diff:
                        q.pop()
                        diff -= lot_qty 
                    else:
                        last_lot["qty"] = lot_qty - diff
                        diff = 0
                        
                if diff > 0:
                    logging.warning(f"⚠️ [QueueLedger] sync_with_broker CALIB_SUB 미달: {ticker} 큐 물량이 브로커보다 {diff}주 부족합니다. 큐가 초기화되었습니다.")

            data[ticker] = q
            self._save_unsafe(data)
            return True
