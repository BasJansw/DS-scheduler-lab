import os
import json
import matplotlib.pyplot as plt

# LIST OF WHAT WE WANT:
# box plot total times
# pdf slice usage
# box plot total wait times

# ALL COMPAREABLES
compareables = ["RoundRobinSched-dotty+reactors-30", "PrioSchedWeightedAvg-dotty+reactors-30", "IOPrioSched-dotty+reactors-30", "None-dotty+reactors-30"]




BENCHMARK = "reactors"
DATA_FOLDER = "zdata/"
# Ensure the necessary folders exist
os.makedirs(f"{DATA_FOLDER}/graphs", exist_ok=True)

def plot_boxplot(data, title, ylabel, filename):
    plt.figure(figsize=(10, 6))
    plt.boxplot(data)
    plt.xlabel('Scheduler')
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(range(1, len(compareables) + 1), map(lambda x: x.split("-")[0], compareables), rotation=70)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{DATA_FOLDER}/graphs/boxplot-{filename}.png")
    plt.close()

def plot_pdf(data, title, xlabel, filename):
    for i, d in enumerate(data):
        sns.kdeplot(d, bw_adjust=0.5, fill=True, alpha=0.7)
    plt.xlabel(xlabel)
    plt.ylabel('Probability Density')
    plt.title(title)
    plt.grid(True)
    plt.xlim(0, 1)
    plt.savefig(f"{DATA_FOLDER}/graphs/pdf-{filename}.png")
    plt.close()


# load data for each scheduler
data = {}
for compareable in compareables:
    filename = f"{DATA_FOLDER}{compareable}/results.json"
    with open(filename, 'r') as json_file:
        data[compareable] = json.load(json_file)




####################################
# TOTAL TIMES
####################################
compareables = ["RoundRobinSched-dotty+reactors-30", "PrioSchedWeightedAvg-dotty+reactors-30", "IOPrioSched-dotty+reactors-30", "None-dotty+reactors-30"]

# make box plot for total times
total_times = {}
for compareable in compareables:
    total_times[compareable] = data[compareable][BENCHMARK]["times"]

plot_boxplot([total_times[compareable] for compareable in compareables], "Total Time", "Time (ms)", f"total-times-{BENCHMARK}")



####################################
# TOTAL WAIT TIMES / ENQUEUES
####################################
compareables = ["RoundRobinSched-dotty+reactors-30", "PrioSchedWeightedAvg-dotty+reactors-30", "IOPrioSched-dotty+reactors-30"]

# make box plot for total enqueues
total_enqueues = {}
for compareable in compareables:
    total_enqueues[compareable] = data[compareable][BENCHMARK]["total_enqueues"]
    total_enqueues[compareable] = [sum(enqueues) for enqueues in total_enqueues[compareable] if len(enqueues) > 5]

plot_boxplot([total_enqueues[compareable] for compareable in compareables], "Total Enqueues", "Enqueues", f"total-enqueues-{BENCHMARK}")

# make box plot for total wait times
total_wait_times = {}
for compareable in compareables:
    total_wait_times[compareable] = data[compareable][BENCHMARK]["total_wait_times"]
    total_wait_times[compareable] = [sum(wait_times) for wait_times in total_wait_times[compareable] if len(wait_times) > 5]

plot_boxplot([total_wait_times[compareable] for compareable in compareables], "Total Wait Time", "Time (ms)", f"total-wait-times-{BENCHMARK}")

####################################
# TOTAL PRIO ENQUEUES / WAIT TIMES
####################################
compareables = ["PrioSchedWeightedAvg-dotty+reactors-30", "IOPrioSched-dotty+reactors-30"]

# make box plot for % prio enqueues
total_prio_enqueues = {}
for compareable in compareables:
    total_prio_enqueues[compareable] = data[compareable][BENCHMARK]["total_prio_enqueues"]
    total_prio_enqueues[compareable] = [sum(enqueues) for enqueues in total_prio_enqueues[compareable] if len(enqueues) > 5]

percentage_prio_enqueues = [[prio_enqueue/total_enqueue for prio_enqueue, total_enqueue in zip(total_prio_enqueues[compareable], total_enqueues[compareable])] for compareable in compareables]
plot_boxplot(percentage_prio_enqueues, "Total Prio Enqueues (%)", "Enqueues", f"total-prio-enqueues-{BENCHMARK}")

# make box plot for % prio wait times
total_prio_wait_times = {}
for compareable in compareables:
    total_prio_wait_times[compareable] = data[compareable][BENCHMARK]["total_prio_wait_time"]
    total_prio_wait_times[compareable] = [sum(wait_times) for wait_times in total_prio_wait_times[compareable] if len(wait_times) > 5]

percentage_prio_wait_times = [[prio_wait/total_wait for prio_wait, total_wait in zip(total_prio_wait_times[compareable], total_wait_times[compareable])] for compareable in compareables]
plot_boxplot(percentage_prio_wait_times, "Total Prio Wait Time (%)", "Time (ms)", f"total-prio-wait-times-{BENCHMARK}")








# # make pdf for slice usage
# benchmark = "PrioSchedWeightedAvg-dotty+reactors-30"
# slice_usage = data[benchmark][BENCHMARK]["slice_usage"]
# slice_usage = [item for sublist in slice_usage for item in sublist]
# plot_pdf([slice_usage], "Slice Usage", "Slice Usage", f"slice-usage-{BENCHMARK}")