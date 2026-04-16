# ==========================================================
# [plugin_updater.py]
# ⚠️ 자가 업데이트 및 GCP 데몬 제어 전용 플러그인
# 💡 깃허브 원격 저장소 강제 동기화 (git fetch & reset --hard)
# 💡 OS 레벨 데몬 재가동 제어 (sudo systemctl restart)
# 🚨 [V27.00 핫픽스] 사용자별 데몬 이름(DAEMON_NAME) .env 동적 로드 이식 완료
# 🛡️ [V27.05 추가] 업데이트 직전 stable_backup 폴더로 롤백용 안전띠 결속 기능 탑재
# ==========================================================
import logging
import asyncio
import subprocess
import os
from dotenv import load_dotenv

class SystemUpdater:
    def __init__(self):
        self.remote_branch = "origin/main"
        
        # 💡 [핵심 수술] .env 파일에서 사용자가 지정한 데몬 이름을 스캔, 없으면 'mybot'으로 폴백
        load_dotenv()
        self.daemon_name = os.getenv("DAEMON_NAME", "mybot")

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

    def restart_daemon(self):
        """
        GCP 리눅스 OS에 데몬 재가동 명령을 하달합니다.
        격발 즉시 봇 프로세스가 SIGTERM 신호를 받고 종료되므로,
        반드시 텔레그램 보고 메시지를 선행 발송한 후 호출해야 합니다.
        """
        try:
            logging.info(f"🔄 [Updater] OS 쉘에 {self.daemon_name} 데몬 재가동 명령을 하달합니다.")
            
            subprocess.Popen(
                ["sudo", "systemctl", "restart", self.daemon_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return True
        except Exception as e:
            logging.error(f"🚨 [Updater] 데몬 재가동 명령 하달 실패: {e}")
            return False
