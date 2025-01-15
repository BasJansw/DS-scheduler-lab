import subprocess
import re
import select
from rich.console import Console
from rich.columns import Columns
from rich.panel import Panel
from rich.live import Live
from rich.text import Text
import json
import matplotlib.pyplot as plt
import argparse
import os, sys
import seaborn as sns

# SCHEDULERS = [None]
SCHEDULERS = ["RoundRobinSched", "IOPrioSched", "PrioSchedWeightedAvg"]
# SCHEDULER_FLAGS = [["--slice_time_prio=10000000"],["--slice_time_prio=5000000"],["--slice_time_prio=20000000"], ["--slice_time_prio=2500000"], ["--slice_time_prio=40000000"], ["--slice_time_prio=80000000"]]
SCHEDULER_FLAGS = [["--slice_time_prio=10000000","--slice_time=5000000"],["--slice_time_prio=10000000","--slice_time=10000000"],["--slice_time_prio=10000000","--slice_time=20000000"],["--slice_time_prio=10000000","--slice_time=40000000"]]
ITERATIONS = 30
BENCHMARKS = ["dotty", "reactors", "page-rank", "db-shootout"]
BUILD = False
DATA_FOLDER = "zdata/"

if BUILD:
  subprocess.run(["./build.sh"])
  
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)
    os.chmod(DATA_FOLDER, 0o777)

def run_benchmarks_with_scheduler(SCHEDULER, FLAGS):
  subprocess.run(["pkill", "-9", "java"])

  console = Console()
  if SCHEDULER:
    scheduler_process = subprocess.Popen(["./run.sh", str(SCHEDULER), "--verbose"] + FLAGS, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
  else:
    scheduler_process = None

  for BENCHMARK in BENCHMARKS:
    experiment_name = f"{BENCHMARK}-{ITERATIONS}-{SCHEDULER}-{"".join(FLAGS)}"

    if os.path.exists(f'{DATA_FOLDER}{experiment_name}.json'):
      print(f"Skipping {BENCHMARK} with {SCHEDULER} and flags {FLAGS} as results already exist.")
      continue

    print(f"Running {BENCHMARK} with {SCHEDULER} and flags {FLAGS}")

    command = ['java', '-jar', 'renaissance-gpl-0.16.0.jar', '-r', str(ITERATIONS), BENCHMARK]
    benchmark_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    last_timestep = 0

    total_wait_times = [[]]
    total_enqueues = [[]]
    total_prio_wait_time = [[]]
    total_prio_enqueues = [[]]
    total_normal_wait_time = [[]]
    total_normal_enqueues = [[]]

    benchmark_started = False

    times = []

    scheduler_output = Text()
    benchmark_output = Text()

    with open('output.txt', 'w') as f, Live(console=console, refresh_per_second=4) as live:
      while True:
        reads = [benchmark_process.stdout.fileno()]
        if scheduler_process:
          reads.append(scheduler_process.stdout.fileno())
        ret = select.select(reads, [], [])

        for fd in ret[0]:
          if scheduler_process and fd == scheduler_process.stdout.fileno():
            scheduler_line = scheduler_process.stdout.readline()
            if scheduler_line:
              scheduler_output.append(scheduler_line.strip() + "\n")
              f.write(f"Scheduler: {scheduler_line}")

              
          if fd == benchmark_process.stdout.fileno():
            benchmark_line = benchmark_process.stdout.readline()
            if benchmark_line:
              benchmark_output.append(benchmark_line.strip() + "\n")
              f.write(f"Benchmark: {benchmark_line}")

              if "started ===" in benchmark_line:
                benchmark_started = True

              if "completed (" in benchmark_line:
                time = re.search(r'\d+\.\d+', benchmark_line).group()
                times.append(float(time))

                total_wait_times.append([])
                total_enqueues.append([])
                total_prio_wait_time.append([])
                total_prio_enqueues.append([])
                total_normal_wait_time.append([])
                total_normal_enqueues.append([])

                benchmark_started = False
              
        # Truncate the output to fit within the screen
        max_lines = console.size.height - 4  # Adjust based on your terminal size
        scheduler_output_str = str(scheduler_output)
        if "step:" in scheduler_output_str:
          stats_sections = scheduler_output_str.split("step:")
          if len(stats_sections) > 2:
            scheduler_output_str = stats_sections[-2]

            timestep = int(re.search(r'(\d+)', scheduler_output_str).group(1))

            if timestep is not last_timestep and benchmark_started:
              total_wait_time = int(re.search(r'total_wait_time: (\d+)', scheduler_output_str).group(1))
              total_enqueue = int(re.search(r'total_enqueues: (\d+)', scheduler_output_str).group(1))
              total_wait_times[-1].append(total_wait_time)
              total_enqueues[-1].append(total_enqueue)

              try:
                total_prio_wait = int(re.search(r'total_prio_wait_time: (\d+)', scheduler_output_str).group(1))
                total_prio_enqueue = int(re.search(r'total_prio_enqueues: (\d+)', scheduler_output_str).group(1))
                total_normal_wait = int(re.search(r'total_normal_wait_time: (\d+)', scheduler_output_str).group(1))
                total_normal_enqueue = int(re.search(r'total_normal_enqueues: (\d+)', scheduler_output_str).group(1))
                total_prio_wait_time[-1].append(total_prio_wait)
                total_prio_enqueues[-1].append(total_prio_enqueue)
                total_normal_wait_time[-1].append(total_normal_wait)
                total_normal_enqueues[-1].append(total_normal_enqueue)
              except:
                pass

              last_timestep = timestep

        truncated_scheduler_output = Text("\n".join(scheduler_output_str.splitlines()[-max_lines:]))
        truncated_benchmark_output = Text("\n".join(str(benchmark_output).splitlines()[-max_lines:]))

        live.update(Columns([Panel(truncated_benchmark_output, title="Benchmark"), Panel(truncated_scheduler_output, title="Scheduler" + (" (tracking)" if benchmark_started else ""))]))

        if benchmark_process.poll() is not None:
          break

      if scheduler_process: 
        scheduler_process.terminate()
        scheduler_process.wait()
        benchmark_process.stdout.close()
        benchmark_process.wait()

      results = {
        "times": times,
        "total_wait_times": total_wait_times,
        "total_enqueues": total_enqueues,
        "total_prio_wait_time": total_prio_wait_time,
        "total_prio_enqueues": total_prio_enqueues,
        "total_normal_wait_time": total_normal_wait_time,
        "total_normal_enqueues": total_normal_enqueues,
        "scheduler_output": str(scheduler_output),
        "benchmark_output": str(benchmark_output)
      }

      with open(f'{DATA_FOLDER}{experiment_name}.json', 'w') as json_file:
        # json.dumps(results, json_file, indent=4)
        json_file.write(json.dumps(results, indent=4))

def run_benchmarks():
  for SCHEDULER in SCHEDULERS:
    for FLAGS in SCHEDULER_FLAGS:
      try:
        run_benchmarks_with_scheduler(SCHEDULER, FLAGS)
      except e:
        print(f"Failed to run {SCHEDULER} with flags {FLAGS}")
        print(e)
        pass
    
if __name__ == "__main__":
  if not os.geteuid() == 0:
    sys.exit("Please run with sudo")


  parser = argparse.ArgumentParser(
                    prog='Benchmark Scheduler',
                    description='Tests the capabilities of the scheduler')


  parser.add_argument('-b', '--build',
                    action='store_true')  # on/off flag
  args = parser.parse_args()
  BUILD = args.build


  run_benchmarks()