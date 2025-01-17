import os
import json
import matplotlib.pyplot as plt
import seaborn as sns

# LIST OF WHAT WE WANT:
# box plot total times
# pdf slice usage
# box plot total wait times

# ALL COMPAREABLES

# run this one for the first comparison
# compareables = ["all-15-SampleScheduler", "all-15-SampleScheduler---fifo", "all-15-None"]

# compareables = ["all-15-PrioSchedWeightedAvg---slice_time_prio=20000000", "all-15-SampleScheduler", "all-15-SampleScheduler---fifo", "all-15-PrioSchedWeightedAvgNoStarvation---slice_time_prio=20000000", "all-15-IOPrioSched---slice_time_prio=20000000", "all-15-RoundRobinSched---slice_time_prio=20000000"]
compareables = ["all-15-PrioSchedWeightedAvg---slice_time_prio=20000000", "all-15-SampleScheduler", "all-15-SampleScheduler---fifo", "all-15-PrioSchedWeightedAvgNoStarvation---slice_time_prio=20000000", "all-15-IOPrioSched---slice_time_prio=20000000"]
# compareables = ["all-15-PrioSchedWeightedAvg---slice_time_prio=20000000", "all-15-PrioSchedWeightedAvgNoStarvation---slice_time_prio=20000000", "all-15-IOPrioSched---slice_time_prio=20000000", "all-15-RoundRobinSched---slice_time_prio=20000000"]

value_mapping= {
    "all-15-PrioSchedWeightedAvg---slice_time_prio=20000000": "PrioSchedWeightedAvg",
    "all-15-PrioSchedWeightedAvgNoStarvation---slice_time_prio=20000000": "PrioSchedWeightedAvgNoStarvation",
    "all-15-IOPrioSched---slice_time_prio=20000000": "IOPrioSched",
    "all-15-RoundRobinSched---slice_time_prio=20000000": "RoundRobinSched",
    "all-15-SampleScheduler---fifo": "SampleSchedulerFIFO",
    "all-15-SampleScheduler": "SampleScheduler",
    "all-15-None": "EEVDF"
}

invalid_compareables = []

BENCHMARK = "renaissance"
DATA_FOLDER = "zdata/"
# Ensure the necessary folders exist
os.makedirs(f"{DATA_FOLDER}/graphs", exist_ok=True)
os.makedirs(f"{DATA_FOLDER}/graphs/renaissance", exist_ok=True)

def plot_boxplot(data, title, ylabel, filename, xticks=None, xlabel=None):
    plt.figure(figsize=(10, 6))
    plt.boxplot(data)
    if xlabel is None:
        plt.xlabel('Scheduler')
    else:
        plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    if xticks is None:
        plt.xticks(range(1, len(valid_compareables) + 1), [value_mapping[c] for c in valid_compareables], rotation=70)
    else:
        plt.xticks(range(1, len(xticks) + 1), xticks, rotation=70)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{DATA_FOLDER}/graphs/{"renaissance/" if BENCHMARK == "renaissance" else ""}boxplot-{filename}.png")
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
    try:
        with open(filename, 'r') as json_file:
            data[compareable] = json.load(json_file)
    except:
        invalid_compareables.append(compareable)
        print(f"Failed to load {filename}")

# Filter out invalid compareables
valid_compareables = [c for c in compareables if c not in invalid_compareables]
# filter out first 10 of dotty
# for compareable in valid_compareables:
#     data[compareable][BENCHMARK]["times"] = data[compareable][BENCHMARK]["times"][15:]
#     data[compareable][BENCHMARK]["total_wait_times"] = data[compareable][BENCHMARK]["total_wait_times"][15:]
#     data[compareable][BENCHMARK]["total_enqueues"] = data[compareable][BENCHMARK]["total_enqueues"][15:]
#     data[compareable][BENCHMARK]["total_prio_wait_time"] = data[compareable][BENCHMARK]["total_prio_wait_time"][15:]
#     data[compareable][BENCHMARK]["total_prio_enqueues"] = data[compareable][BENCHMARK]["total_prio_enqueues"][15:]
#     data[compareable][BENCHMARK]["total_normal_wait_time"] = data[compareable][BENCHMARK]["total_normal_wait_time"][15:]
#     data[compareable][BENCHMARK]["total_normal_enqueues"] = data[compareable][BENCHMARK]["total_normal_enqueues"][15:]

####################################
# TOTAL TIMES
####################################
# make box plot for total times
total_times = {}
for compareable in valid_compareables:
    if BENCHMARK == "renaissance":
        total_times[compareable] = data[compareable]["times"][2:]
    else:
        total_times[compareable] = data[compareable][BENCHMARK]["times"]

# Remove outliers above 75%
import numpy as np
for compareable in valid_compareables:
    q75 = np.percentile(total_times[compareable], 75)
    total_times[compareable] = [time for time in total_times[compareable] if time <= q75]

plot_boxplot([total_times[compareable] for compareable in valid_compareables], "Total Time", "Time (ms)", f"total-times-{BENCHMARK}")

if BENCHMARK == "renaissance":
    for key in data[valid_compareables[0]].keys():
        if key != "times":
            key_data = {compareable: data[compareable][key]["times"][1:] for compareable in valid_compareables}
            for compareable in valid_compareables:
                q75 = np.percentile(key_data[compareable], 75)
                key_data[compareable] = [time for time in key_data[compareable] if time <= q75]
            plot_boxplot([key_data[compareable] for compareable in valid_compareables], f"{key.replace('_', ' ').title()}", key.replace('_', ' ').title(), f"{key}-{BENCHMARK}")
    exit()

####################################
# TOTAL WAIT TIMES / ENQUEUES
####################################
# make box plot for total enqueues
total_enqueues = {}
for compareable in valid_compareables:
    total_enqueues[compareable] = data[compareable][BENCHMARK]["total_enqueues"]
    total_enqueues[compareable] = [sum(enqueues) for enqueues in total_enqueues[compareable] if len(enqueues) > FILTER_FIRST_N]

plot_boxplot([total_enqueues[compareable] for compareable in valid_compareables], "Total Enqueues", "Enqueues", f"total-enqueues-{BENCHMARK}")

# make box plot for total wait times
total_wait_times = {}
for compareable in valid_compareables:
    total_wait_times[compareable] = data[compareable][BENCHMARK]["total_wait_times"]
    total_wait_times[compareable] = [sum(wait_times) for wait_times in total_wait_times[compareable] if len(wait_times) > FILTER_FIRST_N]

plot_boxplot([total_wait_times[compareable] for compareable in valid_compareables], "Total Wait Time", "Time (ms)", f"total-wait-times-{BENCHMARK}")

####################################
# TOTAL PRIO ENQUEUES / WAIT TIMES
####################################
prio_compareables = [c for c in valid_compareables if "Prio" in c]

# make box plot for % prio enqueues
total_prio_enqueues = {}
for compareable in prio_compareables:
    total_prio_enqueues[compareable] = data[compareable][BENCHMARK]["total_prio_enqueues"]
    total_prio_enqueues[compareable] = [sum(enqueues) for enqueues in total_prio_enqueues[compareable] if len(enqueues) > FILTER_FIRST_N]

percentage_prio_enqueues = [[prio_enqueue/total_enqueue for prio_enqueue, total_enqueue in zip(total_prio_enqueues[compareable], total_enqueues[compareable])] for compareable in prio_compareables]
plot_boxplot(percentage_prio_enqueues, "Total Prio Enqueues (%)", "Enqueues", f"total-prio-enqueues-{BENCHMARK}")

# make box plot for % prio wait times
total_prio_wait_times = {}
for compareable in prio_compareables:
    total_prio_wait_times[compareable] = data[compareable][BENCHMARK]["total_prio_wait_time"]
    total_prio_wait_times[compareable] = [sum(wait_times) for wait_times in total_prio_wait_times[compareable] if len(wait_times) > FILTER_FIRST_N]

percentage_prio_wait_times = [[prio_wait/total_wait for prio_wait, total_wait in zip(total_prio_wait_times[compareable], total_wait_times[compareable])] for compareable in compareables]
# plot_boxplot(percentage_prio_wait_times, "Total Prio Wait Time (%)", "Time (ms)", f"total-prio-wait-times-{BENCHMARK}")

# make box plot for normal and prio wait time
total_prio_wait_times = {}
total_normal_wait_times = {}
for compareable in compareables:
    total_prio_wait_times[compareable] = data[compareable][BENCHMARK]["total_prio_wait_time"]
    total_prio_wait_times[compareable] = [sum(wait_times) for wait_times in total_prio_wait_times[compareable] if len(wait_times) > FILTER_FIRST_N]
    total_normal_wait_times[compareable] = data[compareable][BENCHMARK]["total_normal_wait_time"]
    total_normal_wait_times[compareable] = [sum(wait_times) for wait_times in total_normal_wait_times[compareable] if len(wait_times) > FILTER_FIRST_N]

xticks = map(lambda x: x.split("-")[0], compareables)
xticks = [label for name in xticks for label in (name + " normal queue", name + " prio queue")]
values = []
for comparable in compareables:
    values.append(total_normal_wait_times[comparable])
    values.append(total_prio_wait_times[comparable])
plot_boxplot(values, "Total Wait Time per queue", "Time in queue (ms)", f"queue-wait-times-{BENCHMARK}", xlabel="Queue", xticks=xticks)

plot_boxplot