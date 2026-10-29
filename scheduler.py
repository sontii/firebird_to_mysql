## keeps the container running and starts main.py every day at RUN_AT (local time, see TZ)
## RUN_AT can hold more times: "04:00,13:00"
## python scheduler.py --once   runs main.py right now, once (test)
## every run is one applog.futas row, main.py logs to applog.log with the same run id
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

from dblog import startRun, endRun

runTimes = [t.strip() for t in os.getenv("RUN_AT", "04:00").split(",") if t.strip()]


def nextRun(now):
    runs = []
    for runTime in runTimes:
        hour, minute = map(int, runTime.split(":"))
        run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if run <= now:
            run += timedelta(days=1)
        runs.append(run)
    return min(runs)


def runMain():
    runId = startRun()
    ## fresh process every run: main.py exits on error and keeps no state between runs
    result = subprocess.run([sys.executable, "main.py"], env={**os.environ, "LOG_FUTAS_ID": runId})
    endRun(runId, result.returncode)
    print(f"{datetime.now():%Y.%m.%d %H:%M:%S} main.py exit code: {result.returncode}", flush=True)
    return result.returncode


if "--once" in sys.argv:
    sys.exit(runMain())

while True:
    run = nextRun(datetime.now())
    print(f"{datetime.now():%Y.%m.%d %H:%M:%S} next run: {run:%Y.%m.%d %H:%M}", flush=True)

    ## short sleeps, so clock changes (DST) cannot shift the run
    while datetime.now() < run:
        time.sleep(min(60, max(1, (run - datetime.now()).total_seconds())))

    runMain()
