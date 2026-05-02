# ==========================================================
# FILE: telegram_states.py
# ==========================================================
# MODIFIED: [V44.30 수동 입력 렌더링 수술] 텔레그램 창에 수동 목표 수익률(%) 입력 후, /avwap 콘솔 갱신이 아닌 /settlement(환경설정) 화면으로 직결되도록 제자리 렌더링(edit_message_text) 파이프라인 개조 완료.
# MODIFIED: [V44.44 이벤트 루프 교착 방어] 큐 장부 지층 수동 수정(EDIT_Q) 시 발생하는 직접적인 파일 I/O 작업을 비동기(asyncio.to_thread) 래핑하여 텔레그램 데드락 방어막 이식.
# MODIFIED: [V44.45 헌법 수술] 파일 I/O 원자적 쓰기(Atomic Write) 엔진 전면 이식 및 런타임 붕괴 방어막(fsync) 하드코딩 완료.
# MODIFIED: [V44.48 수동 조작 데드코드 영구 소각 및 런타임 무결성 확보] 큐 장부에 존재하지 않는 _load 메서드 호출 찌꺼기 100% 소각.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import os
import json
import asyncio
import tempfile
from telegram import Update
from telegram.ext import ContextTypes

class TelegramStates:
    def __init__(self, config, broker, queue_ledger, sync_engine):
        self.cfg = config
        self.broker = broker
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        if not controller._is_admin(update):
            return
            
        chat_id = update.effective_chat.id
        text = update.message.text.strip() if update.message.text else ""
        
        if "통합 지시서" in text or "지시서 조회" in text:
            return await controller.cmd_sync(update, context)
        elif "장부 동기화" in text or "장부 조회" in text:
            return await controller.cmd_record(update, context)
        elif "명예의 전당" in text:
            return await controller.cmd_history(update, context)
        elif "코어 스위칭" in text or "전술 설정" in text or "모드변환" in text or "분할변경" in text:
            return await controller.cmd_settlement(update, context)
        elif "시드머니" in text or "시드 변경" in text or "시드 관리" in text:
            return await controller.cmd_seed(update, context)
        elif "종목 선택" in text:
            return await controller.cmd_ticker(update, context)
        elif "스나이퍼" in text:
            return await controller.cmd_mode(update, context)
        elif "버전" in text or "업데이트 내역" in text:
            return await controller.cmd_version(update, context)
        elif "비상 해제" in text:
            return await controller.cmd_reset(update, context)
        elif "시스템 업데이트" in text or "엔진 업데이트" in text:
            return await controller.cmd_update(update, context)

        state = controller.user_states.get(chat_id)
        
        if not state:
            return

        try:
            if state.startswith("EDITQ_"):
                parts = state.split("_", 2)
                ticker = parts[1]
                target_date = parts[2]
                
                input_parts = text.split()
                if len(input_parts) != 2:
                    del controller.user_states[chat_id]
                    return await update.message.reply_text("❌ 입력 형식 오류입니다. 띄어쓰기로 수량과 평단가를 입력해주세요. (수정 취소됨)")
                
                try:
                    qty = int(input_parts[0])
                    price = float(input_parts[1])
                except ValueError:
                    del controller.user_states[chat_id]
                    return await update.message.reply_text("❌ 수량/평단가는 숫자로 입력하세요. (수정 취소됨)")
                
                try:
                    # 🚨 MODIFIED: [V44.44 이벤트 루프 교착 방어] API 호출 비동기 래핑
                    curr_p = await asyncio.wait_for(
                        asyncio.to_thread(self.broker.get_current_price, ticker), 
                        timeout=3.0
                    )
                    if curr_p and curr_p > 0 and (price < curr_p * 0.7 or price > curr_p * 1.3):
                        del controller.user_states[chat_id]
                        return await update.message.reply_text(f"🚨 <b>팻핑거 방어 가동:</b> 입력가(${price:.2f})가 현재가(${curr_p:.2f}) 대비 ±30%를 초초과합니다. 다시 시도해주세요.", parse_mode='HTML')
                except Exception:
                    pass

                # 🚨 MODIFIED: [V44.44 이벤트 루프 교착 방어] 파일 I/O 비동기 래핑 및 원자적 쓰기 강제
                def _update_q_ledger():
                    q_file = "data/queue_ledger.json"
                    all_q = {}
                    if os.path.exists(q_file):
                        try:
                            with open(q_file, 'r', encoding='utf-8') as f:
                                all_q = json.load(f)
                        except Exception:
                            pass
                    
                    ticker_q = all_q.get(ticker, [])
                    for item in ticker_q:
                        if item.get('date') == target_date:
                            item['qty'] = qty
                            item['price'] = price
                            break
                    
                    all_q[ticker] = ticker_q
                    
                    dir_name = os.path.dirname(q_file) or '.'
                    os.makedirs(dir_name, exist_ok=True)
                    fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
                    
                    try:
                        with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                            json.dump(all_q, f_out, ensure_ascii=False, indent=4)
                            f_out.flush()
                            os.fsync(f_out.fileno())
                        os.replace(tmp_path, q_file)
                    except Exception as e:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                        raise e
                        
                    # MODIFIED: [V44.48 수동 조작 데드코드 영구 소각 및 런타임 무결성 확보]
                    # (기존 hasattr(self.queue_ledger, '_load') 찌꺼기 100% 영구 소각 완료)
                            
                await asyncio.to_thread(_update_q_ledger)
                
                del controller.user_states[chat_id]
                short_date = target_date[:10]
                await update.message.reply_text(f"✅ <b>[{ticker}] 지층 정밀 수정 완료! KIS 원장과 동기화합니다.</b>\n▫️ {short_date} | {qty}주 | ${price:.2f}", parse_mode='HTML')
                
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                if not self.sync_engine.sync_locks[ticker].locked():
                    await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=False)
                    
                return

            val = float(text)
            parts = state.split("_")
            
            # 🚨 [V44.30] AVWAP 수동 목표수익률 입력 후 /settlement 뷰포트로 즉결 이식
            if state.startswith("CONF_AVWAP_TARGET"):
                if val <= 0:
                    return await update.message.reply_text("❌ 오류: 목표 수익률은 0보다 커야 합니다.")
                ticker = parts[3]
                if hasattr(self.cfg, 'set_avwap_target_profit'):
                    await asyncio.to_thread(self.cfg.set_avwap_target_profit, ticker, val)
                
                del controller.user_states[chat_id]
                
                # 메모리에 MANUAL 상태를 확실히 인젝션
                if 'app_data' not in context.bot_data:
                    context.bot_data['app_data'] = {}
                context.bot_data['app_data'].setdefault('sniper_tracking', {})[f"AVWAP_TARGET_MODE_{ticker}"] = "MANUAL"
                
                if context.job_queue:
                    for job in context.job_queue.jobs():
                        if job.data is not None:
                            job.data.setdefault('sniper_tracking', {})[f"AVWAP_TARGET_MODE_{ticker}"] = "MANUAL"
                
                await update.message.reply_text(f"✅ <b>[{ticker}] 수동 목표 수익률이 {val}%로 설정되며 '🖐️수동 고정' 모드로 자동 전환되었습니다.</b>", parse_mode='HTML')
                
                # 설정 완료 후 /settlement(설정) 화면으로 직결
                try:
                    await controller.cmd_settlement(update, context)
                except Exception as e:
                    logging.error(f"수동 목표 설정 후 환경설정 복귀 에러: {e}")
                return

            elif state.startswith("SEED"):
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 시드머니는 0 이상이어야 합니다.")
                    
                action, ticker = parts[1], parts[2]
                curr = self.cfg.get_seed(ticker)
                new_v = curr + val if action == "ADD" else (max(0, curr - val) if action == "SUB" else val)
                await asyncio.to_thread(self.cfg.set_seed, ticker, new_v)
                await update.message.reply_text(f"✅ [{ticker}] 시드 변경: ${new_v:,.0f}")
                
            elif state.startswith("CONF_SPLIT"):
                if val < 1:
                    return await update.message.reply_text("❌ 오류: 분할 횟수는 1 이상이어야 합니다.")
                    
                ticker = parts[2]
                # 🚨 MODIFIED: 파일 I/O 비동기 래핑
                def _set_split():
                    d = self.cfg._load_json(self.cfg.FILES["SPLIT"], self.cfg.DEFAULT_SPLIT)
                    d[ticker] = val
                    self.cfg._save_json(self.cfg.FILES["SPLIT"], d)
                await asyncio.to_thread(_set_split)
                await update.message.reply_text(f"✅ [{ticker}] 분할: {int(val)}회")
                
            elif state.startswith("CONF_TARGET"):
                ticker = parts[2]
                # 🚨 MODIFIED: 파일 I/O 비동기 래핑
                def _set_target():
                    d = self.cfg._load_json(self.cfg.FILES["PROFIT_CFG"], self.cfg.DEFAULT_TARGET)
                    d[ticker] = val
                    self.cfg._save_json(self.cfg.FILES["PROFIT_CFG"], d)
                await asyncio.to_thread(_set_target)
                await update.message.reply_text(f"✅ [{ticker}] 목표 수익률: {val}%")

            elif state.startswith("CONF_COMPOUND"):
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 복리율은 0 이상이어야 합니다.")
                    
                ticker = parts[2]
                await asyncio.to_thread(self.cfg.set_compound_rate, ticker, val)
                await update.message.reply_text(f"✅ [{ticker}] 졸업 시 자동 복리율: {val}%")

            elif state.startswith("CONF_FEE"):
                if val < 0.0 or val > 10.0:
                    return await update.message.reply_text("🚨 <b>오입력 차단:</b> 수수료율은 0.0% ~ 10.0% 사이여야 합니다.", parse_mode='HTML')
                    
                ticker = parts[2]
                await asyncio.to_thread(self.cfg.set_fee, ticker, val)
                await update.message.reply_text(f"💳 <b>[{ticker}] 증권사 거래 수수료: {val}% 적용 완료!</b>\n▫️ 다음 명예의 전당 정산부터 수익 연산 시 해당 수수료가 적용됩니다.", parse_mode='HTML')
                
            elif state.startswith("CONF_STOCK_SPLIT"):
                if val <= 0:
                    return await update.message.reply_text("❌ 오류: 액면 보정 비율은 0보다 커야 합니다.")
                    
                ticker = parts[2]
                await asyncio.to_thread(self.cfg.apply_stock_split, ticker, val)
                
                est = ZoneInfo('America/New_York')
                today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
                await asyncio.to_thread(self.cfg.set_last_split_date, ticker, today_str)
                
                await update.message.reply_text(f"✅ [{ticker}] 수동 액면 보정 완료\n▫️ 모든 장부 기록이 {val}배 비율로 정밀하게 소급 조정되었습니다.")

            elif state.startswith("VREV_GAP"):
                ticker = parts[2]
                if val > 0: val = -val
                
                if hasattr(self.cfg, 'set_vrev_gap_threshold'):
                    await asyncio.to_thread(self.cfg.set_vrev_gap_threshold, ticker, val)
                    
                await update.message.reply_text(f"📉 <b>[{ticker}] V-REV 장막판 갭 스위칭 임계치 설정 완료!</b>\n▫️ 팩트 타격선: 기초자산 VWAP 대비 <b>{val}%</b>\n▫️ 다음 타임 슬라이싱 스케줄부터 즉시 적용됩니다.", parse_mode='HTML')
                
        except ValueError:
            await update.message.reply_text("❌ 오류: 유효한 숫자를 입력하세요. (입력 대기 상태가 강제 해제되었습니다.)")
        except Exception as e:
            await update.message.reply_text(f"❌ 알 수 없는 오류 발생: {str(e)}")
        finally:
            if chat_id in controller.user_states:
                del controller.user_states[chat_id]
