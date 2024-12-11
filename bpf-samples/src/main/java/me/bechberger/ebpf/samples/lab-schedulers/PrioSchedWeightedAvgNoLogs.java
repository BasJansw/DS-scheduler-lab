package me.bechberger.ebpf.samples;

import me.bechberger.ebpf.annotations.AlwaysInline;
import me.bechberger.ebpf.annotations.bpf.BPF;
import me.bechberger.ebpf.annotations.bpf.BPFMapDefinition;
import me.bechberger.ebpf.annotations.bpf.Property;
import me.bechberger.ebpf.annotations.bpf.BPFFunction;
import me.bechberger.ebpf.annotations.Type;
import me.bechberger.ebpf.bpf.BPFProgram;
import me.bechberger.ebpf.bpf.GlobalVariable;
import me.bechberger.ebpf.bpf.Scheduler;
import me.bechberger.ebpf.type.Ptr;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import me.bechberger.ebpf.bpf.map.BPFLRUHashMap;
import me.bechberger.ebpf.annotations.Unsigned;

import me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;

import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_dsq_id_flags.SCX_DSQ_LOCAL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_public_consts.SCX_SLICE_DFL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.*;
import static me.bechberger.ebpf.runtime.helpers.BPFHelpers.bpf_ktime_get_ns;

@BPF(license = "GPL")
@Property(name = "sched_name", value = "priority_weighted_avg_scheduler")
public abstract class PrioSchedWeightedAvgNoLogs extends BPFProgram implements Scheduler, Runnable {
    @Option(names = "--slice_time")
    long slice_time_setting = 20000000;
    final GlobalVariable<@Unsigned Long> slice_time = new GlobalVariable<>(0L);

    @Option(names = "--slice_time_prio")
    long slice_time_prio_setting = 20000000;
    final GlobalVariable<@Unsigned Long> slice_time_prio = new GlobalVariable<>(0L);

    @Option(names = "--prio_slice_usage_percentage")
    int prio_slice_usage_percentage_setting = 5;
    final GlobalVariable<@Unsigned Integer> prio_slice_usage_percentage = new GlobalVariable<>(0);

    @Option(names = "--weighted_avg_mult")
    double weight_avg_mult_setting = 0.99;
    final GlobalVariable<@Unsigned Long> weight_avg_mult = new GlobalVariable<>(0L);
    final GlobalVariable<@Unsigned Long> FIXED_POINT_MULT = new GlobalVariable<>(1000000L);

    @Option(names = "--initial_usage_percentage")
    double initial_usage_percentage_setting = 1;
    final GlobalVariable<@Unsigned Long> initial_usage = new GlobalVariable<>(0L);

    static final long RR_DSQ_ID = 0;
    static final long IO_PRIO_DSQ_ID = 1;

    @BPFMapDefinition(maxEntries = 100000)
    BPFLRUHashMap<@Unsigned Integer, @Unsigned Long> enqueue_time;

    @BPFMapDefinition(maxEntries = 100000)
    BPFLRUHashMap<@Unsigned Integer, @Unsigned Long> slice_usage;

    @BPFFunction
    @AlwaysInline
    public long sliceUsagePercentage(int pid) {
        var used_t = slice_usage.bpf_get(pid);
        if (used_t == null) {
            return 100;
        }
        return (used_t.val() * 100) / slice_time.get();
    }

    @Override
    public int init() {
        int r1 = scx_bpf_create_dsq(IO_PRIO_DSQ_ID, -1);
        int r2 = scx_bpf_create_dsq(RR_DSQ_ID, -1);
        if (r1 == 1 || r2 == 1) {
            return 1;
        }
        return 0;
    }

    @Override
    public int selectCPU(Ptr<task_struct> p, int prev_cpu, long wake_flags) {
        boolean is_idle = false;
        int cpu = scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, Ptr.of(is_idle));
        if (is_idle) {
            long time = bpf_ktime_get_ns();
            enqueue_time.put(Integer.valueOf(p.val().pid), time);
            scx_bpf_dispatch(p, SCX_DSQ_LOCAL.value(), slice_time.get(), 0);
        }
        return cpu;
    }

    @Override
    public void enqueue(Ptr<task_struct> p, long enq_flags) {
        if (sliceUsagePercentage(p.val().pid) < prio_slice_usage_percentage.get()) {
            scx_bpf_dispatch(p, IO_PRIO_DSQ_ID, slice_time_prio.get(), enq_flags);
        } else {
            scx_bpf_dispatch(p, RR_DSQ_ID, slice_time.get(), enq_flags);
        }
        long time = bpf_ktime_get_ns();
        enqueue_time.put(Integer.valueOf(p.val().pid), time);
    }

    @Override
    public void dispatch(int cpu, Ptr<task_struct> prev) {
        if (scx_bpf_dsq_nr_queued(IO_PRIO_DSQ_ID) >= 1) {
            scx_bpf_consume(IO_PRIO_DSQ_ID);
        } else {
            scx_bpf_consume(RR_DSQ_ID);
        }
    }

    @Override
    public void running(Ptr<task_struct> p) {
        long t = bpf_ktime_get_ns();
        var lookupResult = enqueue_time.bpf_get(Integer.valueOf(p.val().pid));
        if (lookupResult == null) {
            return;
        }
        long enqueueTimeValue = lookupResult.val();
        long wait_time = t - enqueueTimeValue;
    }

    @Override
    public void stopping(Ptr<task_struct> p, boolean runnable) {
        long usedTime = slice_time.get() - p.val().scx.slice;
        long prevAvg = initial_usage.get();
        var prevAvgPtr = slice_usage.bpf_get(p.val().pid);
        if (prevAvgPtr != null) {
            prevAvg = prevAvgPtr.val();
        }
        long c = weight_avg_mult.get();
        long weightedAvg = (prevAvg * c + (FIXED_POINT_MULT.get() - c) * usedTime) / FIXED_POINT_MULT.get();
        slice_usage.put(Integer.valueOf(p.val().pid), weightedAvg);
    }

    void setSettings() {
        slice_time.set(slice_time_setting);
        slice_time_prio.set(slice_time_prio_setting);
        prio_slice_usage_percentage.set(prio_slice_usage_percentage_setting);
        weight_avg_mult.set((long) (weight_avg_mult_setting * FIXED_POINT_MULT.get()));
        initial_usage.set((long) (slice_time_setting * initial_usage_percentage_setting));
    }

    public void run() {
        attachScheduler();
        setSettings();
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
        }
    }

    public static void main(String[] args) {
        try (var program = BPFProgram.load(PrioSchedWeightedAvgNoLogs.class)) {
            new CommandLine(program).execute(args);
        }
    }
}
