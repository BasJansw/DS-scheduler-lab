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

available_schedulers = ["RoundRobinSched", "IOPrioSched", "PrioSchedWeightedAvg"]
SCHEDULER = available_schedulers[2]
ITERATIONS = 2
BENCHMARKS = ["dotty"]
BUILD = False
experiment_name = f"{SCHEDULER}-{'+'.join(BENCHMARKS)}-{ITERATIONS}"
DATA_FOLDER = "zdata/"
if not os.path.exists(DATA_FOLDER):
  os.makedirs(DATA_FOLDER)
  os.chmod(DATA_FOLDER, 0o777)
if not os.path.exists(DATA_FOLDER+experiment_name):
  os.makedirs(DATA_FOLDER+experiment_name)
  os.chmod(DATA_FOLDER+experiment_name, 0o777)

# def save_figures(avg_wait_times, slice_usages, test_names):
#   if slice_usages[0]:
#     for i, slice_usage in list(enumerate(slice_usages)) + [("avg", [sum(values) / len(values) for values in zip(*slice_usages)])]:
#       plt.plot(slice_usage)
#       plt.xlabel('Time step')
#       plt.ylabel('Slice usage (%)')
#       plt.title(f'Slice Usage Over Time ({SCHEDULER}, {",".join(BENCHMARKS)}, {"Average" if i == "avg" else "Iteration " + str(i)})')
#       plt.grid(True)
#       plt.savefig(f"{DATA_FOLDER}{experiment_name}/slice-usages-{i}.png")
#       plt.close()

#       sns.kdeplot(slice_usage if i != "avg" else [item for sublist in slice_usages for item in sublist], bw_adjust=0.5, fill=True, alpha=0.7)
#       plt.xlabel('Slice usage (%)')
#       plt.ylabel('Probability Density')
#       plt.title(f'Slice Usage PDF ({SCHEDULER}, {",".join(BENCHMARKS)}, {"Average" if i == "avg" else "Iteration " + str(i)})')
#       plt.grid(True)
#       plt.xlim(0, 1)
#       plt.savefig(f"{DATA_FOLDER}{experiment_name}/slice-usages-pdf-{i}.png")
#       plt.close()


#   if avg_wait_times[0]:
#     for i, avg_wait_time in list(enumerate(avg_wait_times)) + [("avg", [sum(values) / len(values) for values in zip(*avg_wait_times)])]:
#       plt.plot(avg_wait_time)
#       plt.xlabel('Time step')
#       plt.ylabel('Average wait time')
#       plt.title(f'Average Wait Time Over Time ({SCHEDULER}, {"+".join(BENCHMARKS)}, {"Average" if i == "avg" else "Iteration " + str(i)})')
#       plt.grid(True)
#       plt.savefig(f"{DATA_FOLDER}{experiment_name}/avg-wait-times-{i}.png")
#       plt.close()

def run_benchmark():
  subprocess.run(["pkill", "-9", "java"])
  if BUILD:
    subprocess.run(["./build.sh"])

  console = Console()
  scheduler_process = subprocess.Popen(["./run.sh", str(SCHEDULER), "--verbose"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
  command = ['java', '-jar', 'renaissance-gpl-0.16.0.jar', '-r', str(ITERATIONS), 'reactors', 'dotty']
  benchmark_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

  last_timestep = 0
  avg_wait_times = { "dotty": [[]], "reactors": [[]]}
  slice_usages = { "dotty": [[]], "reactors": [[]]}

  current_benchmark = ""

  # dotty_times = []
  # reactors_times = []

  times = { "dotty": [], "reactors": []}

  scheduler_output = Text()
  benchmark_output = Text()

  with open('output.txt', 'w') as f, Live(console=console, refresh_per_second=4) as live:
    while True:
      reads = [scheduler_process.stdout.fileno(), benchmark_process.stdout.fileno()]
      ret = select.select(reads, [], [])

      for fd in ret[0]:
        if fd == scheduler_process.stdout.fileno():
          scheduler_line = scheduler_process.stdout.readline()
          if scheduler_line:
            scheduler_output.append(scheduler_line.strip() + "\n")
            f.write(f"Scheduler: {scheduler_line}")

            
        if fd == benchmark_process.stdout.fileno():
          benchmark_line = benchmark_process.stdout.readline()
          if benchmark_line:
            benchmark_output.append(benchmark_line.strip() + "\n")
            f.write(f"Benchmark: {benchmark_line}")

            # if "iteration" in benchmark_line and "completed" in benchmark_line:
            #   avg_wait_times.append([])
            #   slice_usages.append([])
            #   test_names.append(benchmark_line.split()[1])

            if "dotty (scala)" in benchmark_line and "started ===" in benchmark_line:
              current_benchmark = "dotty"
            elif "reactors (concurrency)" in benchmark_line and "started ===" in benchmark_line:
              current_benchmark = "reactors"

            if "completed ===" in benchmark_line:
              time = re.search(r'\d+\.\d+', benchmark_line).group()
              times[current_benchmark].append(float(time))

              avg_wait_times[current_benchmark].append([])
              slice_usages[current_benchmark].append([])
            

            # if "dotty (scala)" in benchmark_line and "completed" in benchmark_line:
            #   time = re.search(r'\d+\.\d+', benchmark_line).group()
            #   times["dotty"].append(float(time))

            #   avg_wait_times["dotty"].append([])
            #   slice_usages["dotty"].append([])
            # if "reactors (concurrency)" in benchmark_line and "completed" in benchmark_line:
            #   time = re.search(r'\d+\.\d+', benchmark_line).group()
            #   times["reactors"].append(float(time))

            #   avg_wait_times["reactors"].append([])
            #   slice_usages["reactors"].append([])

      # Truncate the output to fit within the screen
      max_lines = console.size.height - 4  # Adjust based on your terminal size
      scheduler_output_str = str(scheduler_output)
      if "Stats:" in scheduler_output_str:
        stats_sections = scheduler_output_str.split("Stats:")
        if len(stats_sections) > 2:
          scheduler_output_str = stats_sections[-2]

          timestep = int(re.search(r'Timestep: (\d+)', scheduler_output_str).group(1))

          if timestep is not last_timestep:
            avg_wait_time = int(re.search(r'Average wait time: (\d+)', scheduler_output_str).group(1))
            slice_usage = int(re.search(r'Slice usage: (\d+)', scheduler_output_str).group(1))
            slice_usage_frac = float(re.search(r'Slice usage\(\%\): (\d+\.\d+)', scheduler_output_str).group(1))

            avg_wait_times[current_benchmark][-1].append(avg_wait_time)
            slice_usages[current_benchmark][-1].append(slice_usage_frac)

            last_timestep = timestep

      truncated_scheduler_output = Text("\n".join(scheduler_output_str.splitlines()[-max_lines:]))
      truncated_benchmark_output = Text("\n".join(str(benchmark_output).splitlines()[-max_lines:]))

      live.update(Columns([Panel(truncated_benchmark_output, title="Benchmark"), Panel(truncated_scheduler_output, title="Scheduler")]))

      if benchmark_process.poll() is not None:
        break

    scheduler_process.terminate()
    scheduler_process.wait()
    benchmark_process.stdout.close()
    benchmark_process.wait()

    # print("Dotty times: ", dotty_times)
    # print("Reactors times: ", reactors_times)
    # save_figures(avg_wait_times[:-1], slice_usages[:-1], test_names)

    results = {
      "dotty": {
        "times": times["dotty"],
        "slice_usages": slice_usages["dotty"],
        "avg_wait_times": avg_wait_times["dotty"]
      },
      "reactors": {
        "times": times["reactors"],
        "slice_usages": slice_usages["reactors"],
        "avg_wait_times": avg_wait_times["reactors"]
      }
    }

    with open(f'{DATA_FOLDER}results.json', 'w') as json_file:
      # json.dumps(results, json_file, indent=4)
      json_file.write(json.dumps(results, indent=4))

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


  run_benchmark()