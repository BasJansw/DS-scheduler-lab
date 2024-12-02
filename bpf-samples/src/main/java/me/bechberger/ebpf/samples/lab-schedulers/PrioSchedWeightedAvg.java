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
import me.bechberger.ebpf.bpf.map.BPFHashMap;
import me.bechberger.ebpf.bpf.map.BPFLRUHashMap;
import me.bechberger.ebpf.annotations.Unsigned;

import me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;
import me.bechberger.ebpf.runtime.misc.used_address;

import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_dsq_id_flags.SCX_DSQ_LOCAL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_public_consts.SCX_SLICE_DFL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.*;
import static me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;
import static me.bechberger.ebpf.runtime.helpers.BPFHelpers.bpf_ktime_get_ns;

@BPF(license = "GPL")
@Property(name = "sched_name", value = "priority_weighted_avg_scheduler")
public abstract class PrioSchedWeightedAvg extends BPFProgram implements Scheduler, Runnable {
    @Option(names = "--verbose")
    boolean verbose = false;

    @Option(names = "--slice_time")
    // Default is 20 milion (ns)
    long slice_time_setting = 20000000;
    final GlobalVariable<@Unsigned Long> slice_time = new GlobalVariable<>(0L);

    // The time slice used in the priority queue
    @Option(names = "--slice_time_prio")
    // Default is 20 milion (ns)
    long slice_time_prio_setting = 20000000;
    final GlobalVariable<@Unsigned Long> slice_time_prio = new GlobalVariable<>(0L);


    // Threshold of how much of the time slice the process consumes to be put in priority queue
    @Option(names = "--prio_slice_usage_percentage")
    int prio_slice_usage_percentage_setting = 5;
    final GlobalVariable<@Unsigned Integer> prio_slice_usage_percentage = new GlobalVariable<>(0);

    // The constant c from the formula: avg_usage_{t+1} = avg_usage_t * c + (1-c) * usage_ts
    @Option(names = "--weighted_avg_mult")
    double weight_avg_mult_setting = 0.99;
    final GlobalVariable<@Unsigned Long> weight_avg_mult = new GlobalVariable<>(0L);
    // used to multiply the weight to represent it as a non floating point number
    final GlobalVariable<@Unsigned Long> FIXED_POINT_MULT = new GlobalVariable<>(1000000L);
    // static final long FIXED_POINT_MULT = 1000000;

    // The avg_usage assumed for t=0
    @Option(names = "--initial_usage_percentage")
    double initial_usage_percentage_setting = 1;
    final GlobalVariable<@Unsigned Long> initial_usage = new GlobalVariable<>(0L);

    // The queue where all runnable processes are stored
    static final long RR_DSQ_ID=0;
    static final long IO_PRIO_DSQ_ID=1;


    final GlobalVariable<@Unsigned Long> total_wait_time = new GlobalVariable<>(0L);
    final GlobalVariable<@Unsigned Long> num_enqueues = new GlobalVariable<>(0L);


    final GlobalVariable<@Unsigned Long> total_normal_queue_wait_time = new GlobalVariable<>(0L);
    final GlobalVariable<@Unsigned Long> num_normal_enqueues = new GlobalVariable<>(0L);

    final GlobalVariable<@Unsigned Long> total_prio_queue_wait_time = new GlobalVariable<>(0L);
    final GlobalVariable<@Unsigned Long> num_prio_enqueues = new GlobalVariable<>(0L);


    @BPFMapDefinition(maxEntries = 100000)
    BPFLRUHashMap<@Unsigned Integer, @Unsigned Long> enqueue_time;

    @BPFMapDefinition(maxEntries = 100000)
    BPFLRUHashMap<@Unsigned Integer, @Unsigned Long> slice_usage;


    @BPFFunction
    @AlwaysInline
    public long sliceUsagePercentage(int pid){
        var used_t =  slice_usage.bpf_get(pid);
        if (used_t == null){
            // no known value so return the worst possible.
            return 100;
        }
        return (used_t.val() * 100) / slice_time.get();
    }

    @Override
    public int init() {
        int r1 = scx_bpf_create_dsq(IO_PRIO_DSQ_ID, -1);
        int r2 = scx_bpf_create_dsq(RR_DSQ_ID, -1);
        if (r1 == 1 || r2 == 1){
            return 1;
        }
        return 0;
    }

    @Override
    public int selectCPU(Ptr<task_struct> p, int prev_cpu, long wake_flags) {
        boolean is_idle = false;
        int cpu = scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, Ptr.of(is_idle));
        if (is_idle) {
            // We skip the enqueue call
            // sends p to the local queue of the cpu and uses the default time slice value
            long time = bpf_ktime_get_ns();
            enqueue_time.put(Integer.valueOf(p.val().pid), time);
            scx_bpf_dispatch(p, SCX_DSQ_LOCAL.value(), slice_time.get(),0);
        }
        return cpu;
    }

    @Override
    public void enqueue(Ptr<task_struct> p, long enq_flags) {
        if (sliceUsagePercentage(p.val().pid) < prio_slice_usage_percentage.get()){
            scx_bpf_dispatch(p, IO_PRIO_DSQ_ID, slice_time_prio.get(), enq_flags);
        } else {
            // No CPU was ready so we put p in our waiting queue
            scx_bpf_dispatch(p, RR_DSQ_ID, slice_time.get(), enq_flags);
        }
        
        // record t_enqueue
        long time = bpf_ktime_get_ns();
        enqueue_time.put(Integer.valueOf(p.val().pid), time);
    
    }

    @Override
    public void dispatch(int cpu, Ptr<task_struct> prev) {
        // cpu and prev unused, prev could be used to track the task that just came of the cpu. cpu is likely implicitly used inside the consume
        // Place the first task into the local DSQ of the cpu

        if (scx_bpf_dsq_nr_queued(IO_PRIO_DSQ_ID) >= 1){
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

        if (sliceUsagePercentage(p.val().pid) < prio_slice_usage_percentage.get()) {
            total_prio_queue_wait_time.set(total_prio_queue_wait_time.get() + wait_time);
            num_prio_enqueues.set(num_prio_enqueues.get() + 1);
        }else {
            total_normal_queue_wait_time.set(total_normal_queue_wait_time.get() + wait_time);
            num_normal_enqueues.set(num_normal_enqueues.get() + 1);
        }

        total_wait_time.set(total_wait_time.get() + wait_time);
        num_enqueues.set(num_enqueues.get() + 1);
    }

    @Override
    public void stopping(Ptr<task_struct> p, boolean runnable) {
        long usedTime = slice_time.get() - p.val().scx.slice;
        long prevAvg = initial_usage.get();

        var prevAvgPtr = slice_usage.bpf_get(p.val().pid);
        if (prevAvgPtr != null){
            prevAvg = prevAvgPtr.val();
        }

        long c = weight_avg_mult.get();
        // long numerator = prevAvg * c + (FIXED_POINT_MULT.get() - c) * usedTime;
    
        // if (numerator < 0) {
        //     numerator = numerator & 0xFFFFFFFFFFFFFFFFL; // Convert to unsigned equivalent.
        // }
    
        // long weightedAvg = numerator / FIXED_POINT_MULT.get(); // Perform division manually.
        long weightedAvg = (prevAvg * c + (FIXED_POINT_MULT.get() - c) * usedTime) / FIXED_POINT_MULT.get();
        slice_usage.put(Integer.valueOf(p.val().pid), weightedAvg);
    }

    int step = 0;
    void printStats(){
        System.out.println("step: " + step);
        step ++;
        System.out.println("total_wait_time: " + total_wait_time.get());
        System.out.println("total_enqueues: " + num_enqueues.get());
        System.out.println("total_prio_wait_time: " + total_prio_queue_wait_time.get());
        System.out.println("total_prio_enqueues: " + num_prio_enqueues.get());
        System.out.println("total_normal_wait_time: " + total_normal_queue_wait_time.get());
        System.out.println("total_normal_enqueues: " + num_normal_enqueues.get());
    }

    void resetStats(){
        total_wait_time.set(0L);
        num_enqueues.set(0L);

        total_prio_queue_wait_time.set(0L);
        num_prio_enqueues.set(0L);
        
        total_normal_queue_wait_time.set(0L);
        num_normal_enqueues.set(0L);
    }

    void statsLoop() {
        try {
            while (true) {
                Thread.sleep(100);
                printStats();
                resetStats();
            }
        } catch (InterruptedException e) {
        }
    }

    void setSettings(){
        slice_time.set(slice_time_setting);
        slice_time_prio.set(slice_time_prio_setting);
        prio_slice_usage_percentage.set(prio_slice_usage_percentage_setting);
        weight_avg_mult.set((long) (weight_avg_mult_setting * FIXED_POINT_MULT.get()));
        initial_usage.set((long) (slice_time_setting * initial_usage_percentage_setting));
    }

    public void run() {
        attachScheduler();
        setSettings();
        if (verbose) {
            statsLoop();
        } else {
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
            }
        }
    }

    public static void main(String[] args) {
        try (var program = BPFProgram.load(IOPrioSched.class)) {
            new CommandLine(program).execute(args);
        }
    }

}