# ==========================================================
# [plugin_updater.py]
# ⚠️ 자가 업데이트 및 GCP 데몬 제어 전용 플러그인
# 💡 깃허브 원격 저장소 강제 동기화 (git fetch & reset --hard)
# 💡 OS 레벨 데몬 재가동 제어 (sudo systemctl restart)
# 🚨 [V27.00 핫픽스] 사용자별 데몬 이름(DAEMON_NAME) .env 동적 로드 이식 완료
# 🛡️ [V27.05 추가] 업데이트 직전 stable_backup 폴더로 롤백용 안전띠 결속 기능 탑재
# 🚨 [V30.06 NEW] 장중 업데이트 레드존(Red-Zone) 원천 차단:
# VWAP 타임 슬라이싱 및 장마감 정산의 무결성을 위해 
# EST 14:55 ~ 16:10 사이의 업데이트 및 재가동을 100% 차단함.
# 🚨 MODIFIED: [V42.16 핫픽스] pytz 영구 적출 및 ZoneInfo 락온 (ModuleNotFoundError 런타임 붕괴 방어)
# 🚨 MODIFIED: [V44.52 휴일 락다운 해제 수술] 주말(토, 일) 및 휴장일에는 시계(14:55~16:10)를 무시하고 무조건 업데이트를 허용하는 달력 팩트 스캔 엔진(Bypass) 이식 완료.
# 🚨 MODIFIED: [V44.53 제1헌법 및 16계명 절대 락온] 달력 API(mcal) 스캔을 비동기(to_thread) 래핑하고 5초 타임아웃(Fail-Open)을 강제하여 이벤트 루프 교착(Deadlock) 원천 차단.
# 🚨 MODIFIED: [V44.55 데몬 셧다운 교착(Zombie) 영구 소각] OS systemctl 의존성 철거 및 하드 킬(Self-Kill) 엔진 이식 완료.
# ==========================================================
import logging
import asyncio
import subprocess
import os
import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

class SystemUpdater:
    def __init__(self):
        self.remote_branch = "origin/main"
        
        load_dotenv()
        # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막] systemd 데몬 이름(daemon_name)은 .env 파일이 아닌 OS의 .service 파일 내 Environment 속성에서 다이렉트로 주입받아야 한다. 인프라 샌드박스와 애플리케이션 설정의 혼용을 절대 금지한다.
        # MODIFIED: [환경변수 스캔 범위 확장] systemd에서 주입한 소문자 daemon_name 우선 조회 및 대문자 폴백 팩트 교정
        self.daemon_name = os.getenv("daemon_name") or os.getenv("DAEMON_NAME", "mybot")

    # 🚨 [제1헌법 준수] 동기 I/O 차단을 위해 async 격상
    async def is_update_allowed(self):
        """
        현재 시간이 업데이트 금지 시간대(레드존)인지 검사합니다.
        기준: 14:55 EST ~ 16:10 EST (VWAP 가동 및 장마감 정산 보호)
        """
        # 🚨 MODIFIED: [V42.16] pytz 적출 및 ZoneInfo 이식 완료
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        # 🚨 [V44.52 휴일 락다운 해제] 주말(토, 일)이면 무조건 업데이트 허용 (Bypass)
        if now_est.weekday() >= 5:
            return True, ""

        # 🚨 [V44.53 제1헌법/16계명 적용] 비동기 래핑 및 타임아웃 족쇄 체결
        def _check_holiday():
            import pandas_market_calendars as mcal
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            return schedule.empty

        try:
            is_holiday = await asyncio.wait_for(asyncio.to_thread(_check_holiday), timeout=5.0)
            if is_holiday:
                return True, ""
        except asyncio.TimeoutError:
            logging.error("⚠️ [Updater] 달력 API 타임아웃. 휴장일 판별을 건너뛰고 시간 검사 강제 진행 (Fail-Open).")
        except Exception as e:
            logging.debug(f"업데이트 락다운 달력 스캔 에러 (무시하고 시간 검사 진행): {e}")

        curr_time = now_est.time()
        start_lock = datetime.time(14, 55)
        end_lock = datetime.time(16, 10)
        
        if start_lock <= curr_time <= end_lock:
            return False, "⚠️ <b>[배포 금지]</b> 지금은 VWAP 타격 및 장마감 정산 윈도우입니다. (14:55~16:10 EST 업데이트 강제 차단)"
        return True, ""

    async def _create_safety_backup(self):
        """
        [롤백 봇(Rescue) 전용 아키텍처]
        업데이트를 시도한다는 것 = 현재 코드가 정상 작동 중이라는 뜻이므로,
        새로운 코드를 받기 전에 현재 파이썬 파일들을 stable_backup 폴더에 피신시킵니다.
        """
        try:
            backup_dir = "stable_backup"
            os.makedirs(backup_dir, exist_ok=True)
            
            # 현재 폴더의 모든 .py 파일들을 stable_backup 폴더로 복사 (에러 무시)
            proc = await asyncio.create_subprocess_shell(
                f"cp -p *.py {backup_dir}/ 2>/dev/null || true",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            await proc.communicate()
            logging.info("🛡️ [Updater] 롤백 봇을 위한 안전띠(stable_backup) 결속 완료")
        except Exception as e:
            logging.error(f"🚨 [Updater] 안전띠 결속 중 에러 발생 (업데이트는 계속 진행): {e}")

    async def pull_latest_code(self):
        """
        깃허브 서버와 통신하여 로컬의 변경 사항을 완벽히 무시하고
        원격 저장소의 최신 코드로 강제 덮어쓰기(Hard Reset)를 수행합니다.
        """
        # 🚨 [비동기 래핑 대응] await 추가
        allowed, msg = await self.is_update_allowed()
        if not allowed:
            logging.warning(f"🛑 [Updater] 깃허브 강제 동기화 차단 (레드존): {msg}")
            return False, msg

        # 💡 [안전띠 결속] 깃허브 동기화 직전에 현재 상태를 백업합니다!
        await self._create_safety_backup()

        try:
            fetch_proc = await asyncio.create_subprocess_shell(
                "git fetch --all",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            _, fetch_err = await fetch_proc.communicate()
            
            if fetch_proc.returncode != 0:
                error_msg = fetch_err.decode('utf-8').strip()
                logging.error(f"🚨 [Updater] Git Fetch 실패: {error_msg}")
                return False, f"Git Fetch 실패: {error_msg} (서버에서 git init 및 remote add 명령을 선행하십시오)"

            reset_proc = await asyncio.create_subprocess_shell(
                f"git reset --hard {self.remote_branch}",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            _, reset_err = await reset_proc.communicate()
            
            if reset_proc.returncode != 0:
                error_msg = reset_err.decode('utf-8').strip()
                logging.error(f"🚨 [Updater] Git Reset 실패: {error_msg}")
                return False, f"Git Reset 실패: {error_msg}"

            logging.info("✅ [Updater] 깃허브 최신 코드 강제 동기화 완료")
            return True, "깃허브 최신 코드가 로컬에 완벽히 동기화되었습니다."
            
        except Exception as e:
            logging.error(f"🚨 [Updater] 동기화 중 치명적 예외 발생: {e}")
            return False, f"업데이트 프로세스 예외 발생: {e}"

    # 🚨 [제1헌법 준수] 동기 함수 의존성 해결을 위해 async 격상
    async def restart_daemon(self):
        """
        GCP 리눅스 OS에 데몬 재가동 명령을 하달하는 대신,
        파이썬 프로세스를 즉각 폭파(Hard Kill)시킵니다.
        systemd의 Restart=always 속성이 즉시 봇을 부활시킵니다.
        """
        # 🚨 [비동기 래핑 대응] await 추가
        allowed, _ = await self.is_update_allowed()
        if not allowed:
            logging.error("❌ 레드존 시간대 데몬 재가동 시도가 감지되어 OS 강제 차단했습니다.")
            return False

        try:
            logging.info(f"🔄 [Updater] 좀비 셧다운 방어를 위해 파이썬 프로세스를 즉시 자폭(Hard Kill)시킵니다. (systemd가 부활시킴)")
            
            # MODIFIED: [V44.55] sudo systemctl 의존성 영구 철거 및 즉각 셧다운(os._exit) 타격
            os._exit(0)
            
            return True
        except Exception as e:
            logging.error(f"🚨 [Updater] 데몬 자폭 명령 하달 실패: {e}")
            return False
