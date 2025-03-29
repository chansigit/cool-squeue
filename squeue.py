import argparse
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich import box
import subprocess
import time
from datetime import datetime, timedelta
import pytz
import sys
import select
import tty
import termios

console = Console()

def parse_args():
    parser = argparse.ArgumentParser(description="🌈 彩色终端 Slurm squeue 监控器")
    parser.add_argument("-u", "--user", help="只显示某用户（支持逗号分隔）", default=None)
    parser.add_argument("-p", "--partition", help="指定分区（支持逗号分隔）", default="xiaojie")
    parser.add_argument("-r", "--refresh", help="刷新间隔（秒）", type=float, default=1)
    parser.add_argument("--highlight-user", help="加粗&👑用户（只用于高亮，不影响筛选）", default="chensj16")
    return parser.parse_args()

def get_pacific_time():
    now_utc = datetime.utcnow()
    pacific = pytz.timezone("US/Pacific")
    return pytz.utc.localize(now_utc).astimezone(pacific)

def format_submit_time(iso_time):
    try:
        naive_dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%S")
        pacific = pytz.timezone("US/Pacific")
        return pacific.localize(naive_dt)
    except:
        return None

def build_layout():
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="footer", size=3)
    )
    return layout

def get_squeue_table_and_stats(users=None, partitions=None, highlight_user=None):
    columns = ["🆔 JobID", "👤 User", "🟢 状态", "📛 Job名称", "📅 提交时间", "🖥️ 节点数", "⏱️ 已运行", "⏳ 剩余", "💬 原因"]
    table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE_HEAVY)

    for col in columns:
        table.add_column(col)

    cmd = "squeue -o '%i %u %t %.30j %.20V %D %M %L %R' --noheader"
    if partitions:
        part_str = ",".join(partitions)
        cmd = f"squeue -p {part_str} -o '%i %u %t %.30j %.20V %D %M %L %R' --noheader"

    try:
        result = subprocess.check_output(cmd, shell=True).decode().strip().split("\n")
    except subprocess.CalledProcessError:
        return table, {}

    now = get_pacific_time()
    stats = {"R": 0, "PD": 0, "CG": 0, "OTHER": 0}

    for line in result:
        if not line.strip():
            continue
        fields = line.strip().split(None, 8)
        if len(fields) < 9:
            fields += [""] * (9 - len(fields))

        job_user = fields[1]
        if users and job_user not in users:
            continue

        # 👑 标记高亮用户
        is_special = (job_user == highlight_user)
        fields[1] = f"👑 {job_user}" if is_special else job_user

        # 时间
        submit_raw = fields[4]
        dt = format_submit_time(submit_raw)
        pretty_submit = dt.strftime("%b%d %H:%M") if dt else submit_raw
        fields[4] = f"📅 {pretty_submit}"

        # 状态与颜色
        state = fields[2]
        emoji = {"R": "✅", "PD": "⏸️", "CG": "🔄"}.get(state, "❔")
        color = {"R": "green", "PD": "yellow", "CG": "cyan"}.get(state, "white")
        zh_state = {"R": "运行中", "PD": "等待中", "CG": "完成中"}.get(state, "其他")

        if state == "PD" and dt:
            wait_time = now - dt
            if wait_time > timedelta(hours=1):
                zh_state += " ⚠️"

        stats[state if state in stats else "OTHER"] += 1

        fields[2] = f"{emoji} {zh_state}"
        fields[3] = f"📦 [bold blue]{fields[3]}[/bold blue]"
        fields[8] = f"💡 [italic white]{fields[8]}[/italic white]"

        row_style = f"bold {color}" if is_special else color
        table.add_row(*fields, style=row_style)

    return table, stats

def update_layout(layout, users, partitions, highlight_user):
    now_str = get_pacific_time().strftime("%b %d %H:%M:%S")
    layout["header"].update(Panel(f"🕒 当前时间（太平洋时间）：{now_str}", style="bold cyan"))

    table, stats = get_squeue_table_and_stats(users=users, partitions=partitions, highlight_user=highlight_user)
    layout["body"].update(table)

    summary = (
        f"✅ 运行中：{stats.get('R', 0)}    "
        f"⏸️ 等待中：{stats.get('PD', 0)}    "
        f"🔄 完成中：{stats.get('CG', 0)}    "
        f"❔ 其他：{stats.get('OTHER', 0)}"
    )
    layout["footer"].update(Panel(summary, style="bold green"))

def key_pressed():
    """检查是否有键盘按下（非阻塞）"""
    dr, _, _ = select.select([sys.stdin], [], [], 0)
    return dr != []

if __name__ == "__main__":
    args = parse_args()
    users = args.user.split(",") if args.user else None
    partitions = args.partition.split(",") if args.partition else ["xiaojie"]
    highlight_user = args.highlight_user

    layout = build_layout()

    # 设置终端为非阻塞模式读取
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    tty.setcbreak(fd)

    try:
        with Live(layout, refresh_per_second=args.refresh, screen=True) as live:
            while True:
                update_layout(layout, users, partitions, highlight_user)
                time.sleep(args.refresh)
                if key_pressed():
                    ch = sys.stdin.read(1)
                    if ch.lower() == 'q':
                        break
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        print("\n👋 退出监控工具，再见！")


