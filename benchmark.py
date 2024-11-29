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

SCHEDULER = "IOPrioSched"
ITERATIONS = 1
BENCHMARKS = ["dotty"]

def run_benchmark():
  experiment_name = f"{SCHEDULER}-{'+'.join(BENCHMARKS)}-{ITERATIONS}"

  subprocess.run(["pkill", "-9", "java"])

  console = Console()
  scheduler_process = subprocess.Popen(["./run.sh", str(SCHEDULER), "--verbose"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
  command = ['java', '-jar', 'renaissance-gpl-0.16.0.jar', '-r', str(ITERATIONS)] + BENCHMARKS
  benchmark_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

  last_timestep = 0
  avg_wait_times = [[]]
  slice_usages = [[]]

  dotty_times = []
  reactors_times = []

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

            if "dotty (scala)" in benchmark_line and "completed" in benchmark_line:
              time = re.search(r'\d+\.\d+', benchmark_line).group()
              dotty_times.append(float(time))
            if "reactors (concurrency)" in benchmark_line and "completed" in benchmark_line:
              time = re.search(r'\d+\.\d+', benchmark_line).group()
              reactors_times.append(float(time))

              avg_wait_times.append([])
              slice_usages.append([])


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

            avg_wait_times[-1].append(avg_wait_time)
            slice_usages[-1].append(slice_usage_frac)

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

    if slice_usages[0]:
      plt.plot(slice_usages[0])
      plt.xlabel('Time step')
      plt.ylabel('Slice usage (%)')
      plt.title(f'Slice Usage Over Time ({SCHEDULER}, {",".join(BENCHMARKS)}, First Iteration)')
      plt.grid(True)
      plt.savefig(f"{experiment_name}-slice-usages.png")
      plt.close()

    if avg_wait_times[0]:
      plt.plot(avg_wait_times[0])
      plt.xlabel('Time step')
      plt.ylabel('Average wait time')
      plt.title(f'Average Wait Time Over Time ({SCHEDULER}, {"+".join(BENCHMARKS)}, First Iteration)')
      plt.grid(True)
      plt.savefig(f"{experiment_name}-avg-wait-times.png")
      plt.close()

    results = {
      "dotty_times": dotty_times,
      "reactors_times": reactors_times
    }

    with open('times.json', 'w') as json_file:
      json.dump(results, json_file, indent=4)

if __name__ == "__main__":
  run_benchmark()