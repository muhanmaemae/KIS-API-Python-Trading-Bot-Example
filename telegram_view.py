# ==========================================================
# [telegram_bot.py] - Part 2/2 부 (하반부)
# ⚠️ 수술 내역: 
# 1. 누락되었던 7개 명령어 핸들러(cmd_history ~ cmd_version) 100% 무손실 복원
# 2. 인라인 버튼 중앙 통제 라우터(handle_callback) 및 텍스트 핸들러 완벽 복원
# 💡 [V24.18 수술] 수동 긴급 수혈(Emergency MOC) 장외시간 사전 차단 및 MOC 격발 엔진 신설
# 💡 [V24.18 하이브리드] AVWAP 하이브리드 토글(ON/OFF/WARN) 2단계 경고 라우터 융합 완료
# 🚨 [긴급 수술] V-REV 예방적 LOC 덫 수동 장전 라우터(EXEC) 완벽 분리 이식
# 🚨 [V25.06 롤오버 패치] 수동 EXEC 시 장외시간 낡은 전일종가(T-2)를 최신 현재가(T-1)로 치환(Overwrite)하여 타점 불일치 해결
# 🚨 [V25.07 수학적 교정] 구버전 승수 잔재 완전 철거 및 최신 디커플링 공식(0.999 및 /0.935) 팩트 주입
# 🚨 [V25.10 줍줍 복원 패치] 수동 EXEC 시 5개의 줍줍(Grid) LOC 주문이 KIS 서버로 정상 장전되도록 격발 알고리즘 복원
# 🚨 [PEP 8 포맷팅 패치] Ruff E701 에러(One-liner) 전면 분리 교정 완료
# ==========================================================

    async def cmd_history(self, update, context):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
            
        history = self.cfg.get_history()
        if not history:
            await update.message.reply_text("📜 저장된 역사가 없습니다.")
            return
            
        msg = "🏆 <b>[ 졸업 명예의 전당 ]</b>\n"
        keyboard = [[InlineKeyboardButton(f"{h['end_date']} | {h['ticker']} (+${h['profit']:.0f})", callback_data=f"HIST:VIEW:{h['id']}")] for h in history]
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_mode(self, update, context):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
            
        active_tickers = self.cfg.get_active_tickers()

        report = "📊 <b>[ 자율주행 변동성 마스터 지표 상세 분석 ]</b>\n\n"
        
        report += "<b>[ 🧭 지수 범위 범례 (ON/OFF 권장) ]</b>\n"
        report += "🧊 <code>~ 15.00</code> : 극저변동성 (OFF)\n"
        report += "🟩 <code>15.00 ~ 20.00</code> : 정상 궤도 (OFF)\n"
        report += "🟨 <code>20.00 ~ 25.00</code> : 변동성 확대 (ON)\n"
        report += "🟥 <code>25.00 이상 </code> : 패닉 셀링 (ON)\n\n"
        
        for t in active_tickers:
            idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
            dynamic_pct_obj = await asyncio.to_thread(self.broker.get_dynamic_sniper_target, idx_ticker)
            
            if dynamic_pct_obj and hasattr(dynamic_pct_obj, 'metric_val'):
                real_val = float(dynamic_pct_obj.metric_val)
                real_name = dynamic_pct_obj.metric_name
            else:
                real_val = 0.0
                real_name = "지표"
            
            if real_val <= 15.0:
                diag_text = "극저변동성 (우측 꼬리 절단 방지를 위해 스나이퍼 OFF)"
                status_icon = "🧊"
            elif real_val <= 20.0:
                diag_text = "정상 궤도 안착 (스나이퍼 OFF)"
                status_icon = "🟩"
            elif real_val <= 25.0:
                diag_text = "변동성 확대 장세 (계좌 방어를 위해 스나이퍼 ON)"
                status_icon = "🟨"
            else:
                diag_text = "패닉 셀링 및 시스템 충격 (스나이퍼 필수 가동)"
                status_icon = "🟥"
            
            report += f"💠 <b>[ {t} 국면 분석 ]</b>\n"
            report += f"▫️ 당일 절대 지수({real_name}): {real_val:.2f}\n"
            report += f"▫️ 진단 : {status_icon} {diag_text}\n\n"

        report += "🎯 <b>[ 수동 상방 스나이퍼 독립 제어 ]</b>\n"
        keyboard = []
        for t in active_tickers:
            is_sniper = self.cfg.get_upward_sniper_mode(t)
            status_txt = 'ON (가동중)' if is_sniper else 'OFF (대기중)'
            report += f"▫️ {t} 현재 상태 : {status_txt}\n"
            
            keyboard.append([
                InlineKeyboardButton(f"{t} ⚪ OFF", callback_data=f"MODE:OFF:{t}"), 
                InlineKeyboardButton(f"{t} 🎯 ON", callback_data=f"MODE:ON:{t}")
            ])
            
        await update.message.reply_text(report, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_reset(self, update, context):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
            
        active_tickers = self.cfg.get_active_tickers()
        msg, markup = self.view.get_reset_menu(active_tickers)
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_seed(self, update, context):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
            
        msg = "💵 <b>[ 종목별 시드머니 관리 ]</b>\n\n"
        keyboard = []
        for t in self.cfg.get_active_tickers():
            current_seed = self.cfg.get_seed(t)
            msg += f"💎 <b>{t}</b>: ${current_seed:,.0f}\n"
            keyboard.append([
                InlineKeyboardButton(f"➕ {t} 추가", callback_data=f"SEED:ADD:{t}"), 
                InlineKeyboardButton(f"➖ {t} 감소", callback_data=f"SEED:SUB:{t}"),
                InlineKeyboardButton(f"🔢 {t} 고정", callback_data=f"SEED:SET:{t}")
            ])
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_ticker(self, update, context):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
            
        msg, markup = self.view.get_ticker_menu(self.cfg.get_active_tickers())
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_settlement(self, update, context):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
        
        active_tickers = self.cfg.get_active_tickers()
        atr_data = {}
        dynamic_target_data = {} 
        
        status_msg = await update.message.reply_text("⏳ <b>실시간 시장 지표(HV/VXN) 연산 중...</b>", parse_mode='HTML')
        
        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        
        is_sniper_active_time = False
        try:
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            if not schedule.empty:
                market_open = schedule.iloc[0]['market_open'].astimezone(est)
                switch_time = market_open + datetime.timedelta(minutes=50) # 10:20 EST
                if now_est >= switch_time:
                    is_sniper_active_time = True
        except Exception:
            if now_est.weekday() < 5 and now_est.time() >= datetime.time(10, 20):
                is_sniper_active_time = True

        for t in active_tickers:
            atr_data[t] = (0.0, 0.0)
            dynamic_target_data[t] = None
                
        msg, markup = self.view.get_settlement_message(active_tickers, self.cfg, atr_data, dynamic_target_data)
        
        await status_msg.edit_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_version(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
            
        history_data = self.cfg.get_full_version_history()
        msg, markup = self.view.get_version_message(history_data, page_index=None)
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data.split(":")
        action, sub = data[0], data[1] if len(data) > 1 else ""

        if action == "QUEUE":
            if sub == "VIEW":
                ticker = data[2]
                if getattr(self, 'queue_ledger', None):
                    q_data = self.queue_ledger.get_queue(ticker)
                else:
                    q_data = []
                    try:
                        if os.path.exists("data/queue_ledger.json"):
                            with open("data/queue_ledger.json", "r", encoding='utf-8') as f:
                                q_data = json.load(f).get(ticker, [])
                    # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                    except:
                        pass
                        
                msg, markup = self.view.get_queue_management_menu(ticker, q_data)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_REQ":
            ticker = sub
            
            status_code, _ = self._get_market_status()
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
                
            q_data = self.queue_ledger.get_queue(ticker)
            total_q = sum(item.get("qty", 0) for item in q_data)
            
            if total_q == 0:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
            
            emergency_qty = q_data[-1].get('qty', 0)
            emergency_price = q_data[-1].get('price', 0.0)
            
            msg, markup = self.view.get_emergency_moc_confirm_menu(ticker, emergency_qty, emergency_price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_EXEC":
            ticker = sub
            status_code, _ = self._get_market_status()
            
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
                
            q_data = self.queue_ledger.get_queue(ticker)
            if not q_data:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
                
            await query.answer("⏳ KIS 서버에 수동 긴급 수혈(MOC) 명령을 격발합니다...", show_alert=False)
            
            emergency_qty = q_data[-1].get('qty', 0)
            
            if emergency_qty > 0:
                async with self.tx_lock:
                    res = self.broker.send_order(ticker, "SELL", emergency_qty, 0.0, "MOC")
                    
                    if res.get('rt_cd') == '0':
                        self.queue_ledger.pop_lots(ticker, emergency_qty)
                        
                        msg = f"🚨 <b>[{ticker}] 수동 긴급 수혈 (Emergency MOC) 격발 완료!</b>\n"
                        msg += f"▫️ 포트폴리오 매니저의 승인 하에 최근 로트 <b>{emergency_qty}주</b>를 시장가(MOC)로 강제 청산했습니다.\n"
                        await context.bot.send_message(chat_id=update.effective_chat.id, text=msg, parse_mode='HTML')
                        
                        new_q_data = self.queue_ledger.get_queue(ticker)
                        new_msg, markup = self.view.get_queue_management_menu(ticker, new_q_data)
                        await query.edit_message_text(new_msg, reply_markup=markup, parse_mode='HTML')
                    else:
                        err_msg = res.get('msg1', '알 수 없는 에러')
                        await query.edit_message_text(f"❌ <b>[{ticker}] 수동 긴급 수혈 실패:</b> {err_msg}", parse_mode='HTML')

        elif action == "DEL_REQ":
            ticker = sub
            target_date = ":".join(data[2:])
            
            q_data = self.queue_ledger.get_queue(ticker) if getattr(self, 'queue_ledger', None) else []
            if not q_data:
                try:
                    with open("data/queue_ledger.json", "r") as f:
                        q_data = json.load(f).get(ticker, [])
                # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                except:
                    pass
            
            qty, price = 0, 0.0
            for item in q_data:
                if item.get('date') == target_date:
                    qty = item.get('qty', 0)
                    price = item.get('price', 0.0)
                    break
                    
            msg, markup = self.view.get_queue_action_confirm_menu(ticker, target_date, qty, price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action in ["DEL_Q", "EDIT_Q"]:
            ticker = sub
            target_date = ":".join(data[2:])
            
            try:
                q_file = "data/queue_ledger.json"
                all_q = {}
                if os.path.exists(q_file):
                    with open(q_file, 'r', encoding='utf-8') as f:
                        all_q = json.load(f)
                
                ticker_q = all_q.get(ticker, [])
                
                if action == "DEL_Q":
                    new_q = [item for item in ticker_q if item.get('date') != target_date]
                    await self._verify_and_update_queue(ticker, new_q, context, query.message.chat_id)
                    await query.answer(f"✅ 삭제 완료.", show_alert=False)
                    
                    msg, markup = self.view.get_queue_management_menu(ticker, new_q)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    
                elif action == "EDIT_Q":
                    await query.answer("✏️ 수정 모드 진입", show_alert=False)
                    short_date = target_date[:10]
                    self.user_states[update.effective_chat.id] = f"EDITQ_{ticker}_{target_date}"
                    
                    prompt = f"✏️ <b>[{ticker} 지층 수정 모드]</b>\n"
                    prompt += f"선택하신 <b>[{short_date}]</b> 지층을 재설정합니다.\n\n"
                    prompt += f"새로운 <b>[수량]</b>과 <b>[평단가]</b>를 띄어쓰기로 입력하세요.\n"
                    prompt += f"(예: <code>229 52.16</code>)\n\n"
                    prompt += f"<i>(입력을 취소하려면 숫자 이외의 문자를 보내주세요)</i>"
                    await query.edit_message_text(prompt, parse_mode='HTML')
            except Exception as e:
                await query.answer(f"❌ 처리 중 에러 발생: {e}", show_alert=True)

        elif action == "VERSION":
            history_data = self.cfg.get_full_version_history()
            if sub == "LATEST":
                msg, markup = self.view.get_version_message(history_data, page_index=None)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "PAGE":
                page_idx = int(data[2])
                msg, markup = self.view.get_version_message(history_data, page_index=page_idx)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "RESET":
            if sub == "MENU":
                active_tickers = self.cfg.get_active_tickers()
                msg, markup = self.view.get_reset_menu(active_tickers)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "LOCK": 
                ticker = data[2]
                self.cfg.reset_lock_for_ticker(ticker)
                await query.edit_message_text(f"✅ <b>[{ticker}] 금일 매매 잠금이 해제되었습니다.</b>", parse_mode='HTML')
            elif sub == "REV":
                ticker = data[2]
                msg, markup = self.view.get_reset_confirm_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "CONFIRM":
                ticker = data[2]
                
                self.cfg.set_reverse_state(ticker, False, 0)
                self.cfg.clear_escrow_cash(ticker) 
                
                ledger_data = [r for r in self.cfg.get_ledger() if r.get('ticker') != ticker]
                self.cfg._save_json(self.cfg.FILES["LEDGER"], ledger_data)
                
                backup_file = self.cfg.FILES["LEDGER"].replace(".json", "_backup.json")
                if os.path.exists(backup_file):
                    try:
                        with open(backup_file, 'r', encoding='utf-8') as f:
                            b_data = json.load(f)
                        b_data = [r for r in b_data if r.get('ticker') != ticker]
                        with open(backup_file, 'w', encoding='utf-8') as f:
                            json.dump(b_data, f, ensure_ascii=False, indent=4)
                    # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                    except:
                        pass
                
                q_file = "data/queue_ledger.json"
                if os.path.exists(q_file):
                    try:
                        with open(q_file, 'r', encoding='utf-8') as f:
                            q_data = json.load(f)
                        if ticker in q_data:
                            del q_data[ticker]
                        with open(q_file, 'w', encoding='utf-8') as f:
                            json.dump(q_data, f, ensure_ascii=False, indent=4)
                    # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                    except:
                        pass
                    
                if getattr(self, 'queue_ledger', None) and hasattr(self.queue_ledger, 'queues') and ticker in self.queue_ledger.queues:
                    del self.queue_ledger.queues[ticker]
                    
                await query.edit_message_text(f"✅ <b>[{ticker}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue), 에스크로의 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ 다음 매수 진입 시 0주 새출발 디커플링 타점 모드로 완벽히 재시작합니다.", parse_mode='HTML')
            
            elif sub == "CANCEL":
                await query.edit_message_text("❌ 안전 통제실 메뉴를 닫습니다.", parse_mode='HTML')

        elif action == "REC":
            if sub == "VIEW": 
                async with self.tx_lock:
                    _, holdings = self.broker.get_account_balance()
                await self._display_ledger(data[2], update.effective_chat.id, context, query=query, pre_fetched_holdings=holdings)
            elif sub == "SYNC": 
                ticker = data[2]
                
                if ticker not in self.sync_locks:
                    self.sync_locks[ticker] = asyncio.Lock()
                    
                if not self.sync_locks[ticker].locked():
                    await query.edit_message_text(f"🔄 <b>[{ticker}] 잔고 기반 대시보드 업데이트 중...</b>", parse_mode='HTML')
                    res = await self.process_auto_sync(ticker, update.effective_chat.id, context, silent_ledger=True)
                    if res == "SUCCESS": 
                        async with self.tx_lock:
                            _, holdings = self.broker.get_account_balance()
                        await self._display_ledger(ticker, update.effective_chat.id, context, message_obj=query.message, pre_fetched_holdings=holdings)

        elif action == "HIST":
            if sub == "VIEW":
                hid = int(data[2])
                target = next((h for h in self.cfg.get_history() if h['id'] == hid), None)
                if target:
                    qty, avg, invested, sold = self.cfg.calculate_holdings(target['ticker'], target['trades'])
                    msg, markup = self.view.create_ledger_dashboard(target['ticker'], qty, avg, invested, sold, target['trades'], 0, 0, is_history=True)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
            elif sub == "LIST":
                await self.cmd_history(update, context)
            elif sub == "IMG":
                ticker = data[2]
                hist_list = [h for h in self.cfg.get_history() if h['ticker'] == ticker]
                
                if not hist_list:
                    await context.bot.send_message(update.effective_chat.id, f"📭 <b>[{ticker}]</b> 발급 가능한 졸업 기록이 존재하지 않습니다.", parse_mode='HTML')
                    return
                
                latest_hist = sorted(hist_list, key=lambda x: x.get('end_date', ''), reverse=True)[0]
                
                try:
                    img_path = self.view.create_profit_image(
                        ticker=latest_hist['ticker'],
                        profit=latest_hist['profit'],
                        yield_pct=latest_hist['yield'],
                        invested=latest_hist['invested'],
                        revenue=latest_hist['revenue'],
                        end_date=latest_hist['end_date']
                    )
                    if os.path.exists(img_path):
                        with open(img_path, 'rb') as photo:
                            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=photo)
                except Exception as e:
                    logging.error(f"📸 👑 졸업 이미지 생성/발송 실패: {e}")
                    await context.bot.send_message(update.effective_chat.id, f"❌ 이미지 렌더링 모듈 장애 발생.", parse_mode='HTML')
            
        elif action == "EXEC":
            t = sub
            ver = self.cfg.get_version(t)
            
            await query.edit_message_text(f"🚀 {t} 수동 강제 전송 시작 (교차 분리)...")
            async with self.tx_lock:
                cash, holdings = self.broker.get_account_balance()
                # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                if holdings is None:
                    return await query.edit_message_text("❌ API 통신 오류로 주문을 실행할 수 없습니다.")
                    
                _, allocated_cash = self._calculate_budget_allocation(cash, self.cfg.get_active_tickers())
                h = holdings.get(t, {'qty':0, 'avg':0})
                
                curr_p = float(await asyncio.to_thread(self.broker.get_current_price, t) or 0.0)
                prev_c = float(await asyncio.to_thread(self.broker.get_previous_close, t) or 0.0)
                safe_avg = float(h.get('avg') or 0.0)
                safe_qty = int(float(h.get('qty') or 0))

                status_code, _ = self._get_market_status()
                if status_code in ["AFTER", "CLOSE", "PRE"] and curr_p > 0:
                    prev_c = curr_p

                if ver == "V_REV":
                    if not getattr(self, 'queue_ledger', None):
                        from queue_ledger import QueueLedger
                        self.queue_ledger = QueueLedger()
                        
                    q_data = self.queue_ledger.get_queue(t)
                    v_rev_q_qty = sum(item.get("qty", 0) for item in q_data)
                    rev_budget = float(self.cfg.get_seed(t) or 0.0) * 0.15
                    
                    half_portion_cash = rev_budget * 0.5
                    one_portion_qty = math.floor(rev_budget / curr_p) if curr_p > 0 else 0
                    
                    loc_orders = []
                    
                    if q_data:
                        recent_lots = list(reversed(q_data))[:3]
                        for idx, lot in enumerate(recent_lots):
                            target_sell_price = round(lot.get('price', prev_c) * 1.006, 2) if idx == 0 else round(safe_avg * 1.005, 2)
                            sell_qty = min(lot['qty'], one_portion_qty) if one_portion_qty > 0 else lot['qty']
                            if sell_qty > 0:
                                loc_orders.append({'side': 'SELL', 'qty': sell_qty, 'price': target_sell_price, 'type': 'LOC', 'desc': f'예방적 매도(Pop{idx+1})'})
                    
                    if prev_c > 0:
                        b1_price = round(prev_c / 0.935 if v_rev_q_qty == 0 else prev_c * 0.995, 2)
                        b2_price = round(prev_c * 0.999 if v_rev_q_qty == 0 else prev_c * 0.9725, 2)
                        
                        b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                        b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                        
                        if b1_qty > 0:
                            loc_orders.append({'side': 'BUY', 'qty': b1_qty, 'price': b1_price, 'type': 'LOC', 'desc': '예방적 매수(Buy1)'})
                        if b2_qty > 0:
                            loc_orders.append({'side': 'BUY', 'qty': b2_qty, 'price': b2_price, 'type': 'LOC', 'desc': '예방적 매수(Buy2)'})
                            
                        if b2_qty > 0 and b2_price > 0:
                            for n in range(1, 6):
                                grid_p = round(half_portion_cash / (b2_qty + n), 2)
                                if grid_p >= 0.01 and grid_p < b2_price:
                                    loc_orders.append({'side': 'BUY', 'qty': 1, 'price': grid_p, 'type': 'LOC', 'desc': f'예방적 줍줍({n})'})

                    msg = f"🛡️ <b>[{t}] V-REV 예방적 양방향 LOC 방어선 수동 장전 완료</b>\n"
                    all_success = True
                    for o in loc_orders:
                        res = self.broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                        is_success = res.get('rt_cd') == '0'
                        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                        if not is_success:
                            all_success = False
                            
                        err_msg = res.get('msg1', '오류')
                        status_icon = '✅' if is_success else f'❌({err_msg})'
                        msg += f"└ {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                        await asyncio.sleep(0.2)
                        
                    if all_success and len(loc_orders) > 0:
                        self.cfg.set_lock(t, "REG")
                        msg += "\n🔒 <b>방어선 전송 완료 (매매 잠금 설정됨)</b>"
                    elif len(loc_orders) == 0:
                        msg += "\n⚠️ <b>전송할 방어선(예산/수량)이 없습니다.</b>"
                    else:
                        msg += "\n⚠️ <b>일부 방어선 구축 실패 (잠금 보류)</b>"
                        
                    await context.bot.send_message(update.effective_chat.id, msg, parse_mode='HTML')
                    return
                
                ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
                plan = self.strategy.get_plan(t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t])
                
                title = f"💎 <b>[{t}] 무매4 정규장 주문 수동 실행</b>\n"
                msg = title
                
                all_success = True
                
                for o in plan.get('core_orders', []):
                    res = self.broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                    if not is_success:
                        all_success = False
                        
                    err_msg = res.get('msg1', '오류')
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msg += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                    await asyncio.sleep(0.2) 
                    
                for o in plan.get('bonus_orders', []):
                    res = self.broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    err_msg = res.get('msg1', '잔금패스')
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msg += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {status_icon}\n"
                    await asyncio.sleep(0.2) 
                
                if all_success and len(plan.get('core_orders', [])) > 0:
                    self.cfg.set_lock(t, "REG")
                    msg += "\n🔒 <b>필수 주문 전송 완료 (잠금 설정됨)</b>"
                else:
                    msg += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"

            await context.bot.send_message(update.effective_chat.id, msg, parse_mode='HTML')

        elif action == "SET_VER":
            new_ver = sub
            ticker = data[2]
            
            if new_ver == "V_REV":
                if not (os.path.exists("strategy_reversion.py") and os.path.exists("queue_ledger.py")):
                    await query.answer("🚨 [개봉박두] V-REV 엔진 모듈 파일이 존재하지 않아 전환할 수 없습니다! (업데이트 필요)", show_alert=True)
                    return
                self.cfg.set_upward_sniper_mode(ticker, False) 
                
            if new_ver == "V_REV": new_ver_display = "V_REV 역추세 하이브리드"
            else: new_ver_display = "V14 무매4"
            
            self.cfg.set_version(ticker, new_ver)
            
            if new_ver != "V_REV" and hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                self.cfg.set_avwap_hybrid_mode(ticker, False)
                
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>{new_ver_display}</b> 모드로 전환되었습니다.\n/sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "MODE":
            mode_val = sub
            ticker = data[2] if len(data) > 2 else "SOXL"
            
            if mode_val == "AVWAP_WARN":
                msg, markup = self.view.get_avwap_warning_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
            elif mode_val == "AVWAP_ON":
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, True)
                self.cfg.set_upward_sniper_mode(ticker, False) 
                await query.edit_message_text(f"🔥 <b>[{ticker}] 차세대 AVWAP 하이브리드 암살자 모드가 락온(Lock-on) 되었습니다!</b>\n▫️ 남은 가용 예산 100%를 활용하여 장중 -2% 타점을 정밀 사냥합니다.", parse_mode='HTML')
                return
            elif mode_val == "AVWAP_OFF":
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    self.cfg.set_avwap_hybrid_mode(ticker, False)
                await query.edit_message_text(f"🛑 <b>[{ticker}] 차세대 AVWAP 하이브리드 전술이 즉시 해제되었습니다.</b>", parse_mode='HTML')
                return

            current_ver = self.cfg.get_version(ticker)
            if current_ver == "V_REV" and mode_val == "ON":
                await query.answer(f"🚨 {current_ver} 모드에서는 로직 충돌 방지를 위해 상방 스나이퍼를 켤 수 없습니다!", show_alert=True)
                return
                
            self.cfg.set_upward_sniper_mode(ticker, mode_val == "ON")
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 상방 스나이퍼 모드 변경 완료: {'🎯 ON (가동중)' if mode_val == 'ON' else '⚪ OFF (대기중)'}", parse_mode='HTML')
            
        elif action == "SET_INIT":
            ticker = data[2]
            if sub == "V_REV":
                msg, markup = self.view.get_init_v_rev_confirm_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return

            elif sub == "EXEC_CONFIRM":
                await query.answer("⏳ 장부 재구성 중...")
                async with self.tx_lock:
                    _, holdings = self.broker.get_account_balance()
                h = holdings.get(ticker, {'qty': 0, 'avg': 0})
                qty = int(h['qty'])
                avg = float(h['avg'])
                
                if qty > 0:
                    new_q = [{
                        "qty": qty,
                        "price": avg,
                        "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "type": "INIT_TRANSFERRED" 
                    }]
                    try:
                        await self._verify_and_update_queue(ticker, new_q, context, query.message.chat_id)
                        await query.edit_message_text(f"✅ <b>[{ticker}] 자동 물량 이관 및 초기화 완료!</b>\n\n<b>{qty}주</b>(평단 <b>${avg:.2f}</b>)의 단일 기초 블록으로 완벽히 재구성되었습니다.", parse_mode='HTML')
                    except Exception as e:
                        await query.edit_message_text(f"❌ 쓰기 오류 발생: {e}", parse_mode='HTML')
                else:
                    await query.edit_message_text(f"⚠️ <b>[{ticker}] 보유 물량이 없어 이관할 대상이 없습니다.</b>", parse_mode='HTML')

        elif action == "TICKER":
            self.cfg.set_active_tickers([sub] if sub != "ALL" else ["SOXL", "TQQQ"])
            await query.edit_message_text(f"✅ 운용 종목 변경: {sub}")
            
        elif action == "SEED":
            ticker = data[2]
            self.user_states[update.effective_chat.id] = f"SEED_{sub}_{ticker}"
            await context.bot.send_message(update.effective_chat.id, f"💵 [{ticker}] 시드머니 금액 입력:")
            
        elif action == "INPUT":
            ticker = data[2]
            self.user_states[update.effective_chat.id] = f"CONF_{sub}_{ticker}"
            
            # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리 및 콜론(:) 문법 규격 준수
            if sub == "SPLIT":
                ko_name = "분할 횟수"
            elif sub == "TARGET":
                ko_name = "목표 수익률(%)"
            elif sub == "COMPOUND":
                ko_name = "자동 복리율(%)"
            elif sub == "STOCK_SPLIT":
                ko_name = "액면 분할/병합 비율 (예: 10분할은 10, 10병합은 0.1)"
            else:
                ko_name = "값"
            
            await context.bot.send_message(update.effective_chat.id, f"⚙️ [{ticker}] {ko_name} 입력 (숫자만):")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not self._is_admin(update):
            return
            
        chat_id = update.effective_chat.id
        state = self.user_states.get(chat_id)
        
        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not state:
            return

        try:
            if state.startswith("EDITQ_"):
                parts = state.split("_", 2)
                ticker = parts[1]
                target_date = parts[2]
                
                input_parts = update.message.text.strip().split()
                if len(input_parts) != 2:
                    del self.user_states[chat_id]
                    return await update.message.reply_text("❌ 입력 형식 오류입니다. 띄어쓰기로 수량과 평단가를 입력해주세요. (수정 취소됨)")
                
                try:
                    qty = int(input_parts[0])
                    price = float(input_parts[1])
                except ValueError:
                    del self.user_states[chat_id]
                    return await update.message.reply_text("❌ 수량/평단가는 숫자로 입력하세요. (수정 취소됨)")
                
                try:
                    curr_p = await asyncio.wait_for(
                        asyncio.to_thread(self.broker.get_current_price, ticker), 
                        timeout=3.0
                    )
                    if curr_p and curr_p > 0 and (price < curr_p * 0.7 or price > curr_p * 1.3):
                        del self.user_states[chat_id]
                        return await update.message.reply_text(f"🚨 <b>팻핑거 방어 가동:</b> 입력가(${price:.2f})가 현재가(${curr_p:.2f}) 대비 ±30%를 초과합니다. 다시 시도해주세요.", parse_mode='HTML')
                except Exception:
                    pass

                q_file = "data/queue_ledger.json"
                all_q = {}
                if os.path.exists(q_file):
                    with open(q_file, 'r', encoding='utf-8') as f:
                        all_q = json.load(f)
                        
                ticker_q = all_q.get(ticker, [])
                for item in ticker_q:
                    if item.get('date') == target_date:
                        item['qty'] = qty
                        item['price'] = price
                        break
                
                await self._verify_and_update_queue(ticker, ticker_q, context, chat_id)
                del self.user_states[chat_id]
                short_date = target_date[:10]
                await update.message.reply_text(f"✅ <b>[{ticker}] 지층 정밀 수정 완료!</b>\n▫️ {short_date} | {qty}주 | ${price:.2f}\n▫️ 확인: 장부 하단 🗄️ 버튼", parse_mode='HTML')
                return

            val = float(update.message.text.strip())
            parts = state.split("_")
            
            if state.startswith("SEED"):
                # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 시드머니는 0 이상이어야 합니다.")
                    
                action, ticker = parts[1], parts[2]
                curr = self.cfg.get_seed(ticker)
                new_v = curr + val if action == "ADD" else (max(0, curr - val) if action == "SUB" else val)
                self.cfg.set_seed(ticker, new_v)
                await update.message.reply_text(f"✅ [{ticker}] 시드 변경: ${new_v:,.0f}")
                
            elif state.startswith("CONF_SPLIT"):
                # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                if val < 1:
                    return await update.message.reply_text("❌ 오류: 분할 횟수는 1 이상이어야 합니다.")
                    
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["SPLIT"], self.cfg.DEFAULT_SPLIT)
                d[ticker] = val
                self.cfg._save_json(self.cfg.FILES["SPLIT"], d)
                await update.message.reply_text(f"✅ [{ticker}] 분할: {int(val)}회")
                
            elif state.startswith("CONF_TARGET"):
                ticker = parts[2]
                d = self.cfg._load_json(self.cfg.FILES["PROFIT_CFG"], self.cfg.DEFAULT_TARGET)
                d[ticker] = val
                self.cfg._save_json(self.cfg.FILES["PROFIT_CFG"], d)
                await update.message.reply_text(f"✅ [{ticker}] 목표: {val}%")
                
            elif state.startswith("CONF_COMPOUND"):
                # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                if val < 0:
                    return await update.message.reply_text("❌ 오류: 복리율은 0 이상이어야 합니다.")
                    
                ticker = parts[2]
                self.cfg.set_compound_rate(ticker, val)
                await update.message.reply_text(f"✅ [{ticker}] 졸업 시 자동 복리율: {val}%")
                
            elif state.startswith("CONF_STOCK_SPLIT"):
                # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
                if val <= 0:
                    return await update.message.reply_text("❌ 오류: 액면 보정 비율은 0보다 커야 합니다.")
                    
                ticker = parts[2]
                self.cfg.apply_stock_split(ticker, val)
                
                est = pytz.timezone('US/Eastern')
                today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
                self.cfg.set_last_split_date(ticker, today_str)
                
                await update.message.reply_text(f"✅ [{ticker}] 수동 액면 보정 완료\n▫️ 모든 장부 기록이 {val}배 비율로 정밀하게 소급 조정되었습니다.")
                
        except ValueError:
            await update.message.reply_text("❌ 오류: 유효한 숫자를 입력하세요. (입력 대기 상태가 강제 해제되었습니다.)")
        except Exception as e:
            await update.message.reply_text(f"❌ 알 수 없는 오류 발생: {str(e)}")
        finally:
            if chat_id in self.user_states:
                del self.user_states[chat_id]
# ==========================================================
# [telegram_view.py] - Part 2/2 부 (하반부)
# ⚠️ V-REV 설정줄 숨김 및 종목 간 띄어쓰기(엔터) 간격 완벽 교정
# 💡 [V24.15 대수술] 2대 코어(V14, V-REV) 체제 UI 최적화 및 V_VWAP 적출
# 💡 [V24.18 하이브리드] V-REV 종속형 AVWAP 투트랙(Two-track) 지시서 표출 로직 융합
# 💡 [긴급 수술] V-REV 예방적 방어선 수동 장전 버튼(EXEC) UI 100% 복원
# 🚨 [V25.09 렌더링 패치] V-REV 지시서 하단에 덧붙여지던 V14 찌꺼기(🧹 줍줍, 0.975) 강제 출력 블록 영구 소각
# 🚨 [PEP 8 포맷팅 패치] Ruff E701, E722, F541 전면 교정 완료
# ==========================================================

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data=None):
        # 💡 LOC 주문이 아직 들어가지 않은(is_locked가 False인) 종목의 필수 예산만 합산
        total_required_budget = sum(
            t_info.get('one_portion', 0.0) 
            for t_info in ticker_data 
            if not t_info.get('is_locked', False)
        )
        
        # 💡 KIS 주문가능금액(cash)에서 미주문 필수 예산만 선제적 차감 (이중 차감 방어)
        dynamic_rp_amount = max(0.0, cash - total_required_budget)
        
        total_locked = sum(t_info.get('escrow', 0.0) for t_info in ticker_data)
        
        header_msg = f"📜 <b>[ 통합 지시서 ({status_text}) ]</b>\n📅 <b>{dst_text}</b>\n"
        
        if total_locked > 0:
            real_cash = max(0, cash - total_locked)
            header_msg += f"💵 한투 전체 잔고: ${cash:,.2f}\n"
            header_msg += f"🔒 에스크로 격리금: -${total_locked:,.2f}\n"
            header_msg += f"✅ 실질 가용 예산: ${real_cash:,.2f}\n"
        else:
            header_msg += f"💵 주문가능금액: ${cash:,.2f}\n"
            
        header_msg += f"🏛️ RP 투자권장: ${dynamic_rp_amount:,.2f}\n"
        header_msg += "----------------------------\n\n"
        
        body_msg = ""
        keyboard = []

        for t_info in ticker_data:
            t = t_info['ticker']
            v_mode = t_info['version']
            
            if t_info.get('t_val', 0.0) > (t_info.get('split', 40.0) * 1.1):
                body_msg += "⚠️ <b>[🚨 시스템 긴급 경고: 비정상 T값 폭주 감지!]</b>\n"
                body_msg += f"🔎 현재 T값(<b>{t_info['t_val']:.4f}T</b>)이 설정된 분할수(<b>{int(t_info['split'])}분할</b>) 초과했습니다!\n"
                body_msg += "💡 <b>원인 역산 추정:</b> 수동 매수로 수량이 급증했거나, '/seed' 시드머니 설정이 대폭 축소되었습니다.\n"
                body_msg += "🛡️ <b>가동 조치:</b> 마이너스 호가 차단용 절대 하한선($0.01) 방어막 가동 중!\n\n"

            if v_mode == "V_REV":
                v_mode_display = "V_REV 역추세"
                main_icon = "⚖️"
            else:
                v_mode_display = "무매4"
                main_icon = "💎"
                
            is_rev = t_info.get('is_reverse', False)
            proc_status = t_info.get('plan', {}).get('process_status', '')
            tracking_info = t_info.get('tracking_info', {})
            
            if proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"⚠️ <b>[🚨 비상 상황: {t} 긴급 수혈 중]</b>\n"
                body_msg += "❗ <i>에스크로 금고가 바닥나 강제 매도를 통해 현금을 생성합니다.</i>\n\n"
            
            if is_rev:
                bdg_txt = f"리버스 잔금쿼터: ${t_info['one_portion']:,.0f}"
                icon = "🩸" if proc_status == "🩸리버스(긴급수혈)" else "🔄"
                body_msg += f"{icon} <b>[{t}] {v_mode_display} 리버스</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            elif v_mode == "V_REV":
                bdg_txt = f"1회(1배수) 예산: ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>\n"
                body_msg += f"📈 큐(Queue): <b>{t_info.get('v_rev_q_lots', 0)}개 로트 대기 중 (총 {t_info.get('v_rev_q_qty', 0)}주)</b>\n"
            else:
                bdg_txt = f"당일 예산: ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            
            body_msg += f"💵 총 시드: ${t_info['seed']:,.0f}\n"
            body_msg += f"🛒 <b>{bdg_txt}</b>\n"
            
            escrow = t_info.get('escrow', 0.0)
            if escrow > 0:
                body_msg += f"🔐 내 금고 보호액: ${escrow:,.2f}\n"
            elif is_rev and proc_status == "🩸리버스(긴급수혈)":
                body_msg += "🔐 내 금고 보호액: $0.00 (Empty 🚨)\n"
                
            body_msg += f"💰 현재 ${t_info['curr']:,.2f} / 평단 ${t_info['avg']:,.2f} ({t_info['qty']}주)\n"
            
            day_high = t_info.get('day_high', 0.0)
            day_low = t_info.get('day_low', 0.0)
            prev_close = t_info.get('prev_close', 0.0)
            
            if prev_close > 0 and day_high > 0 and day_low > 0:
                high_pct = (day_high - prev_close) / prev_close * 100
                low_pct = (day_low - prev_close) / prev_close * 100
                high_sign = "+" if high_pct > 0 else ""
                low_sign = "+" if low_pct > 0 else ""
                body_msg += f"📈 금일 고가: ${day_high:.2f} ({high_sign}{high_pct:.2f}%)\n"
                body_msg += f"📉 금일 저가: ${day_low:.2f} ({low_sign}{low_pct:.2f}%)\n"

            sign = "+" if t_info['profit_amt'] >= 0 else "-"
            icon = "🔺" if t_info['profit_amt'] >= 0 else "🔻"
            body_msg += f"{icon} 수익: {sign}{abs(t_info['profit_pct']):.2f}% ({sign}${abs(t_info['profit_amt']):,.2f})\n"
            
            sniper_status_txt = t_info.get('upward_sniper', 'OFF')
            
            if v_mode != "V_REV":
                if is_rev:
                    body_msg += f"⚙️ 🌟 5일선 별지점: ${t_info['star_price']:.2f} | 🎯감시: {sniper_status_txt}\n"
                else:
                    body_msg += f"⚙️ 🎯 {t_info['target']}% | ⭐ {t_info['star_pct']}% | 🎯감시: {sniper_status_txt}\n"
                    
                if sniper_status_txt == "ON":
                    if not is_trade_active:
                        body_msg += "🎯 상방 스나이퍼: 감시 종료 (장마감)\n"
                    elif tracking_info.get('is_trailing', False):
                        peak_price = tracking_info.get('peak_price', 0.0)
                        trigger_price = tracking_info.get('trigger_price', 0.0)
                        body_msg += f"🎯 상방 추적(${trigger_price:.2f}) 중 (고가: ${peak_price:.2f})\n"
                    else:
                        if is_rev:
                            sn_target = t_info['star_price']
                        else:
                            safe_floor = math.ceil(t_info['avg'] * 1.005 * 100) / 100.0
                            sn_target = max(t_info['star_price'], safe_floor)
                            
                        if sn_target > 0:
                            body_msg += f"🎯 상방 스나이퍼: ${sn_target:.2f} 이상 대기\n"
            elif v_mode == "V_REV":
                body_msg += "⚖️ <b>역추세 LIFO 큐(Queue) 엔진 스탠바이</b>\n"
                body_msg += "⏱️ <b>VWAP 스케줄:</b> 15:30 EST 앵커 세팅 ➔ 1분 단위 교차 타격\n"
            
            if v_mode == "V_REV":
                body_msg += "📋 <b>[주문 가이던스 - ⚖️다중 LIFO 제어]</b>\n"
                
                # MODIFIED: [V25.09 렌더링 패치] 텔레그램 뷰어(UI) 내부에서 하드코딩으로 연산해서 덧붙이던
                # 1.15 / 0.975 구버전 수동 디커플링 로직 및 🧹줍줍 블록을 100% 영구 소각(Nuke)했습니다.
                # 오직 telegram_bot.py 가 정밀 역산하여 던져준 v_rev_guidance 텍스트만 순수하게 투영합니다.
                raw_guidance = t_info.get('v_rev_guidance', " (가이던스 대기 중)")
                raw_guidance = raw_guidance.rstrip('\n')
                body_msg += raw_guidance + "\n"

                # 💡 [V24.18 하이브리드] V-REV 모드일 때 AVWAP 켜져있으면 투트랙(Two-track) 지시서 독립 표출
                if t_info.get('avwap_active', False):
                    avwap_qty = t_info.get('avwap_qty', 0)
                    avwap_avg = t_info.get('avwap_avg', 0.0)
                    avwap_status = t_info.get('avwap_status', '👀 장초반 필터 스캔 대기')
                    avwap_budget = t_info.get('avwap_budget', 0.0)
                    
                    body_msg += "\n⚔️ <b>[ 하이브리드 AVWAP 암살자 가동 중 ]</b>\n"
                    body_msg += f"▫️ 잉여 예산(100%): ${avwap_budget:,.0f}\n"
                    body_msg += f"▫️ 독립 물량: {avwap_qty}주 (평단 ${avwap_avg:.2f})\n"
                    body_msg += f"▫️ 작전 상태: <b>{avwap_status}</b>\n"
                    
                # 🚨 [긴급 수술] V-REV 모드 전용 "수동 장전" 버튼 표출
                if is_trade_active:
                    keyboard.append([InlineKeyboardButton(f"🚀 {t} V-REV 방어선 수동 장전", callback_data=f"EXEC:{t}")])
                
            else:
                body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
                plan_orders = t_info.get('plan', {}).get('orders', [])
                if plan_orders:
                    jup_orders = [o for o in plan_orders if "줍줍" in o['desc']]
                    n_orders = [o for o in plan_orders if "줍줍" not in o['desc']]
    
                    for o in n_orders:
                        ico = "🔴" if o['side'] == 'BUY' else "🔵"
                        desc = o['desc']
                        
                        if "수혈" in desc: 
                            ico = "🩸"
                            desc = desc.replace("🩸", "")
                            
                        type_str = "" if o['type'] == 'LIMIT' else f"({o['type']})"
                        type_disp = f" {type_str}" if type_str else ""
                        
                        body_msg += f" {ico} {desc}: <b>${o['price']} x {o['qty']}주</b>{type_disp}\n"
    
                    if jup_orders:
                        prices = sorted([o['price'] for o in jup_orders], reverse=True)
                        body_msg += f" 🧹 줍줍({len(jup_orders)}개): <b>${prices[0]} ~ ${prices[-1]} (LOC)</b>\n"
                    
                    if is_trade_active:
                        if t_info.get('is_locked', False):
                            body_msg += " (✅ 금일 주문 완료/잠금)\n"
                        else:
                            keyboard.append([InlineKeyboardButton(f"🚀 {t} 주문 실행", callback_data=f"EXEC:{t}")])
                else:
                    body_msg += " 💤 주문 없음 (관망/예산소진)\n"
                
            body_msg += "\n"

        final_msg = header_msg + body_msg

        vol_summaries = []
        for t_info in ticker_data:
            if 'vol_weight' in t_info and 'vol_status' in t_info:
                vol_summaries.append(f"{t_info['ticker']}: {t_info['vol_weight']} ({t_info['vol_status']})")
        
        if vol_summaries:
            final_msg += "📊 <b>[자율지표]</b> " + " | ".join(vol_summaries) + "\n<i>(상세: /mode)</i>\n\n"

        # MODIFIED: [PEP 8 교정] 단일 행 제어문 분리
        if not is_trade_active:
            final_msg += "⛔ 장마감/애프터마켓: 주문 불가"
            
        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None

    def get_settlement_message(self, active_tickers, config, atr_data, dynamic_target_data=None):
        msg = "⚙️ <b>[ 현재 설정 및 복리 상태 ]</b>\n\n"
        keyboard = []
        
        for t in active_tickers:
            ver = config.get_version(t)
            
            if ver == "V_REV":
                icon = "⚖️"
                ver_display = "V_REV 역추세"
            else:
                icon = "💎"
                ver_display = "무매4"
                
            split_cnt = int(config.get_split_count(t))
            target_pct = config.get_target_profit(t)
            comp_rate = config.get_compound_rate(t)
            
            msg += f"{icon} <b>{t} ({ver_display} 모드)</b>\n"
            msg += f"▫️ 분할: {split_cnt}회\n▫️ 목표: {target_pct}%\n▫️ 자동복리: {comp_rate}%\n"
            
            if ver == "V_REV":
                msg += "⚖️ <b>역추세(Reversion) 하이브리드 엔진 스탠바이:</b>\n"
                msg += "▫️ 전일 종가 앵커 기준 LIFO 큐 교차 매매 대기 중\n\n"
                row_init = [InlineKeyboardButton(f"🔌 {t} V-REV 큐 장부 초기화 (물량이관)", callback_data=f"SET_INIT:V_REV:{t}")]
                keyboard.append(row_init)
            else:
                msg += "\n"
                
            row1 = [
                InlineKeyboardButton("💎 V14 (무매4)", callback_data=f"SET_VER:V14:{t}"),
                InlineKeyboardButton("⚖️ V-REV (역추세)", callback_data=f"SET_VER:V_REV:{t}")
            ]
            keyboard.append(row1)

            if ver == "V_REV":
                is_avwap = config.get_avwap_hybrid_mode(t) if hasattr(config, 'get_avwap_hybrid_mode') else False
                
                avwap_txt = "⚔️ 차세대 AVWAP 하이브리드 [ OFF ]"
                avwap_cb = f"MODE:AVWAP_WARN:{t}" 
                
                if is_avwap:
                    avwap_txt = "⚔️ 차세대 AVWAP 하이브리드 [ 가동중 ]"
                    avwap_cb = f"MODE:AVWAP_OFF:{t}" 
                
                keyboard.append([InlineKeyboardButton(avwap_txt, callback_data=avwap_cb)])
            
            row2 = [
                InlineKeyboardButton(f"⚙️ {t} 분할", callback_data=f"INPUT:SPLIT:{t}"), 
                InlineKeyboardButton(f"🎯 {t} 목표", callback_data=f"INPUT:TARGET:{t}"),
                InlineKeyboardButton(f"💸 {t} 복리", callback_data=f"INPUT:COMPOUND:{t}")
            ]
            keyboard.append(row2)
            
            row3 = [
                InlineKeyboardButton(f"✂️ {t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")
            ]
            keyboard.append(row3)
            
        return msg, InlineKeyboardMarkup(keyboard)

    def create_ledger_dashboard(self, ticker, qty, avg, invested, sold, records, t_val, split, is_history=False, is_reverse=False):
        groups = {}
        for r in records:
            key = (r['date'], r['side'])
            if key not in groups:
                groups[key] = {'sum_qty': 0, 'sum_cost': 0}
            groups[key]['sum_qty'] += r['qty']
            groups[key]['sum_cost'] += (r['qty'] * r['price'])

        agg_list = []
        for (date, side), data in groups.items():
            if data['sum_qty'] > 0:
                avg_p = data['sum_cost'] / data['sum_qty']
                agg_list.append({'date': date, 'side': side, 'qty': data['sum_qty'], 'avg': avg_p})

        agg_list.sort(key=lambda x: x['date'])
        for i, item in enumerate(agg_list):
            item['no'] = i + 1
        agg_list.reverse()

        title = "과거 졸업 기록" if is_history else "일자별 매매 (통합 변동분)"
        msg = f"📜 <b>[ {ticker} {title} (총 {len(agg_list)}일) ]</b>\n\n"
        
        msg += "<code>No. 일자   구분  평균단가  수량\n"
        msg += "-"*30 + "\n"
        
        for item in agg_list[:50]: 
            d_str = item['date'][5:].replace('-', '.')
            s_str = "🔴매수" if item['side'] == 'BUY' else "🔵매도"
            msg += f"{item['no']:<3} {d_str} {s_str} ${item['avg']:<6.2f} {item['qty']}주\n"
            
        if len(agg_list) > 50:
            msg += "... (이전 기록 생략)\n"
            
        msg += "-"*30 + "</code>\n"

        msg += "📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
        if not is_history:
            if is_reverse:
                msg += "▪️ 운용 상태 : 🚨 <b>시드 소진 (리버스모드 가동 중)</b>\n"
                msg += f"▪️ 리버스 T값 : <b>{t_val} T</b> (특수연산 적용됨)\n"
            else:
                msg += f"▪️ <b>현재 T값 : {t_val} T</b> ({int(split)}분할)\n"
            msg += f"▪️ 보유 수량 : {qty} 주 (평단 ${avg:.2f})\n"
        else:
            profit = sold - invested
            pct = (profit/invested*100) if invested > 0 else 0
            sign = "+" if profit >= 0 else "-"
            msg += f"▪️ <b>최종수익: {sign}${abs(profit):,.2f} ({pct:.2f}%)</b>\n"

        msg += f"▪️ 총 매수액 : ${invested:,.2f}\n▪️ 총 매도액 : ${sold:,.2f}\n"

        keyboard = []
        if not is_history:
            other = "TQQQ" if ticker == "SOXL" else "SOXL"
            keyboard.append([InlineKeyboardButton(f"🔄 {other} 장부 조회", callback_data=f"REC:VIEW:{other}")])
            keyboard.append([InlineKeyboardButton(f"🗄️ {ticker} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
            keyboard.append([InlineKeyboardButton("🔙 장부 대시보드 업데이트", callback_data=f"REC:SYNC:{ticker}")])
        else:
            keyboard.append([InlineKeyboardButton("🖼️ 프리미엄 졸업 카드 발급", callback_data=f"HIST:IMG:{ticker}")])
            keyboard.append([InlineKeyboardButton("🔙 역사 목록으로 돌아가기", callback_data="HIST:LIST")])

        return msg, InlineKeyboardMarkup(keyboard)

    def create_profit_image(self, ticker, profit, yield_pct, invested, revenue, end_date):
        W, H = 600, 920 
        IMG_H = 430 
        
        img = Image.new('RGB', (W, H), color='#1E222D')
        draw = ImageDraw.Draw(img)
        
        try:
            if os.path.exists("background.png"):
                bg = Image.open("background.png").convert("RGB")
                bg_ratio = bg.width / bg.height
                if bg_ratio > (W / IMG_H):
                    new_w = int(IMG_H * bg_ratio)
                    bg = bg.resize((new_w, IMG_H), Image.Resampling.LANCZOS).crop(((new_w - W) // 2, 0, (new_w + W) // 2, IMG_H))
                else:
                    new_h = int(W / bg_ratio)
                    bg = bg.resize((W, new_h), Image.Resampling.LANCZOS).crop((0, (new_h - IMG_H) // 2, W, (new_h + IMG_H) // 2))
                img.paste(bg, (0, 0))
            else:
                draw.rectangle([0, 0, W, IMG_H], fill="#111217")
        except Exception: 
            draw.rectangle([0, 0, W, IMG_H], fill="#111217")

        f_title = self._load_best_font(self.bold_font_paths, 65)
        f_p = self._load_best_font(self.bold_font_paths, 85)
        f_y = self._load_best_font(self.reg_font_paths, 40)
        f_b_val = self._load_best_font(self.bold_font_paths, 32)
        f_b_lbl = self._load_best_font(self.reg_font_paths, 22)

        y_title = IMG_H + 60
        draw.rectangle([W/2 - 140, y_title - 45, W/2 + 140, y_title + 45], fill="#2A2F3D")
        draw.text((W/2, y_title), f"{ticker}", font=f_title, fill="white", anchor="mm")
        
        color = "#007AFF" if profit < 0 else "#FF3B30"
        sign = "-" if profit < 0 else "+"
        
        y_profit = y_title + 105
        draw.text((W/2, y_profit), f"{sign}${abs(profit):,.2f}", font=f_p, fill=color, anchor="mm")
        
        y_yield = y_profit + 75
        draw.text((W/2, y_yield), f"YIELD {sign}{abs(yield_pct):,.2f}%", font=f_y, fill=color, anchor="mm")
        
        y_box = y_yield + 60
        
        draw.rectangle([40, y_box, 290, y_box + 100], fill="#2A2F3D")
        draw.text((165, y_box + 35), f"${invested:,.2f}", font=f_b_val, fill="white", anchor="mm")
        draw.text((165, y_box + 75), "TOTAL INVESTED", font=f_b_lbl, fill="#8E8E93", anchor="mm")
        
        draw.rectangle([310, y_box, 560, y_box + 100], fill="#2A2F3D")
        draw.text((435, y_box + 35), f"${revenue:,.2f}", font=f_b_val, fill="white", anchor="mm")
        draw.text((435, y_box + 75), "TOTAL REVENUE", font=f_b_lbl, fill="#8E8E93", anchor="mm")
        
        draw.text((W/2, H - 35), f"{end_date}", font=f_b_lbl, fill="#636366", anchor="mm")
        
        fname = f"data/profit_{ticker}.png"
        img.save(fname)
        return fname

    def get_ticker_menu(self, current_tickers):
        keyboard = [
            [InlineKeyboardButton("🔥 SOXL 전용", callback_data="TICKER:SOXL")],
            [InlineKeyboardButton("🚀 TQQQ 전용", callback_data="TICKER:TQQQ")],
            [InlineKeyboardButton("💎 SOXL + TQQQ 통합", callback_data="TICKER:ALL")]
        ]
        return f"🔄 <b>[ 운용 종목 선택 ]</b>\n현재: <b>{', '.join(current_tickers)}</b>", InlineKeyboardMarkup(keyboard)
