import subprocess
import argparse
from datetime import datetime, timedelta
import pytz
import re

def parse_args():
    parser = argparse.ArgumentParser(description="🧠 Slurm 等候优先级分析器 + sprio 拆解")
    parser.add_argument("-u", "--user", required=True, help="要分析的用户名（支持逗号分隔）")
    parser.add_argument("-p", "--partition", default="xiaojie", help="Partition（队列），默认：xiaojie")
    return parser.parse_args()

def get_pacific_time():
    utc_now = datetime.utcnow()
    pacific = pytz.timezone("US/Pacific")
    return pytz.utc.localize(utc_now).astimezone(pacific)

def run_cmd(cmd):
    try:
        return subprocess.check_output(cmd, shell=True).decode().strip()
    except subprocess.CalledProcessError:
        return ""

def get_pending_jobs(user, partition):
    cmd = f"squeue -p {partition} -u {user} -t PD -o '%i %V %R' --noheader"
    lines = run_cmd(cmd).splitlines()
    jobs = []
    for line in lines:
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        jobid, submit_str, reason = parts
        try:
            submit_dt = datetime.strptime(submit_str, "%Y-%m-%dT%H:%M:%S")
            pacific = pytz.timezone("US/Pacific")
            submit_time = pacific.localize(submit_dt)
        except:
            continue
        jobs.append({
            "JobID": jobid,
            "SubmitTime": submit_time,
            "Reason": reason,
        })
    return jobs

def get_job_priority(jobid):
    output = run_cmd(f"scontrol show job {jobid}")
    match = re.search(r"Priority=(\d+)", output)
    if match:
        return int(match.group(1))
    return None

def get_sprio_breakdown(jobid):
    output = run_cmd(f"sprio -j {jobid}")
    lines = output.splitlines()
    if len(lines) < 2:
        return {}
    fields = lines[0].split()
    values = lines[1].split()
    breakdown = dict(zip(fields, values))
    return breakdown

def get_all_pending_jobs(partition):
    cmd = f"squeue -p {partition} -t PD -o '%i %u %V' --noheader"
    lines = run_cmd(cmd).splitlines()
    jobs = []
    for line in lines:
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        jobid, user, submit_str = parts
        try:
            submit_dt = datetime.strptime(submit_str, "%Y-%m-%dT%H:%M:%S")
            pacific = pytz.timezone("US/Pacific")
            submit_time = pacific.localize(submit_dt)
        except:
            continue
        jobs.append({
            "JobID": jobid,
            "User": user,
            "SubmitTime": submit_time,
            "Priority": get_job_priority(jobid)
        })
    return jobs

def analyze(user_list, partition):
    now = get_pacific_time()
    all_jobs = get_all_pending_jobs(partition)

    for user in user_list:
        your_jobs = get_pending_jobs(user, partition)

        print(f"\n🎯 用户 {user} 在 partition `{partition}` 的排队优先级分析：")
        print(f"当前时间：{now.strftime('%b %d %H:%M:%S')}（PST）")
        print(f"\n你当前有 {len(your_jobs)} 个排队任务：\n")

        for job in your_jobs:
            jobid = job["JobID"]
            wait = now - job["SubmitTime"]
            priority = get_job_priority(jobid)
            job["Priority"] = priority

            print(f"🔹 JobID {jobid}")
            print(f"    等待时间: {str(wait).split('.')[0]}")
            print(f"    Reason: {job['Reason']}")
            print(f"    Priority: {priority}")

            # 🎯 sprio 分解
            sprio = get_sprio_breakdown(jobid)
            if sprio:
                print(f"    📊 优先级分解：")
                print(f"      AGE: {sprio.get('AGE','?')}  FAIRSHARE: {sprio.get('FAIRSHARE','?')}  "
                      f"JOBSIZE: {sprio.get('JOBSIZE','?')}  QOS: {sprio.get('QOS','?')}  TRES: {sprio.get('TRES','?')}")
            print("")

        # 分析被谁“压着”
        print("🔍 可能影响你任务调度的其他用户任务：\n")
        for your_job in your_jobs:
            your_priority = your_job["Priority"]
            your_submit = your_job["SubmitTime"]
            blockers = []
            for other in all_jobs:
                if other["User"] == user:
                    continue
                if other["Priority"] and other["Priority"] > your_priority:
                    if other["SubmitTime"] > your_submit:
                        label = "❗插队可能"
                    else:
                        label = "⏳正常等待"
                    blockers.append((other["JobID"], other["User"], other["Priority"], other["SubmitTime"], label))
            if blockers:
                print(f"  💢 Job {your_job['JobID']} 被以下任务挡住：")
                for b in sorted(blockers, key=lambda x: -x[2]):
                    print(f"     - JobID {b[0]} ({b[1]}), 优先级: {b[2]}, 提交: {b[3].strftime('%m-%d %H:%M')} {b[4]}")
            else:
                print(f"  ✅ Job {your_job['JobID']} 暂无明显阻挡任务")
        print("\n📌 建议：")
        print(" - Reason: Priority 表示你因优先级低排队，可以尝试减少资源/提高 QOS；")
        print(" - 查看 sprio 分解中 FAIRSHARE/QOS 分项得分，判断是否资源使用偏多；")
        print(" - 若被明显插队，可截图找管理员问问 😉")
        print("-" * 60)

if __name__ == "__main__":
    args = parse_args()
    user_list = args.user.split(",")
    analyze(user_list, args.partition)

