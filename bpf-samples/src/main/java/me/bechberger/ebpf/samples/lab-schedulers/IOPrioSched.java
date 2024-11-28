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

import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_dsq_id_flags.SCX_DSQ_LOCAL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_public_consts.SCX_SLICE_DFL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.*;
import static me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;
import static me.bechberger.ebpf.runtime.helpers.BPFHelpers.bpf_ktime_get_ns;

@BPF(license = "GPL")
@Property(name = "sched_name", value = "io_priority_scheduler")
public abstract class IOPrioSched extends BPFProgram implements Scheduler, Runnable {
    @Option(names = "--verbose")
    boolean verbose = false;

    // The queue where all runnable processes are stored
    static final long RR_DSQ_ID=0;
    static final long IO_PRIO_DSQ_ID=1;

    // We don't use SCX_SLICE_DFL (our time slice in ns)
    // static final long TIME_SLICE=100000000;
    static final long TIME_SLICE=10000000;

    // The fraction of the slice that a task is allowed to use to be classified as "IO heavy"
    static final double IO_SLICE_USAGE_PERCENTAGE = 5;


    final GlobalVariable<@Unsigned Long> total_wait_time = new GlobalVariable<>(0L);

    final GlobalVariable<@Unsigned Long> num_enqueues = new GlobalVariable<>(0L);


    final GlobalVariable<@Unsigned Long> slice_usage_last = new GlobalVariable<>(0L);
    final GlobalVariable<Boolean> last_queue = new GlobalVariable<>(false);

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
        return (used_t.val() * 100) / TIME_SLICE;
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
            scx_bpf_dispatch(p, SCX_DSQ_LOCAL.value(), TIME_SLICE,0);
        }
        return cpu;
    }

    @Override
    public void enqueue(Ptr<task_struct> p, long enq_flags) {
        if (sliceUsagePercentage(p.val().pid) < IO_SLICE_USAGE_PERCENTAGE){
            scx_bpf_dispatch(p, IO_PRIO_DSQ_ID, TIME_SLICE, enq_flags);
        } else {
            // No CPU was ready so we put p in our waiting queue
            scx_bpf_dispatch(p, RR_DSQ_ID, TIME_SLICE, enq_flags);
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
            last_queue.set(true);
            scx_bpf_consume(IO_PRIO_DSQ_ID);
        } else {
            last_queue.set(false);
            scx_bpf_consume(RR_DSQ_ID);
        }
        
    }

    @Override
    public void running(Ptr<task_struct> p) {
        long t = bpf_ktime_get_ns();
        var lookupResult = enqueue_time.bpf_get(Integer.valueOf(p.val().pid));
        if (lookupResult != null) {
            long enqueueTimeValue = lookupResult.val();
            long wait_time = t - enqueueTimeValue;
            total_wait_time.set(total_wait_time.get() + wait_time);
            num_enqueues.set(num_enqueues.get() + 1);
        }
        return;
    }

    @Override
    public void stopping(Ptr<task_struct> p, boolean runnable) {
        long usedTime = TIME_SLICE - p.val().scx.slice;
        slice_usage.put(Integer.valueOf(p.val().pid), usedTime);
        slice_usage_last.set(usedTime);
    }

    void printStats(){
        System.out.println("Average wait time: ");
        System.out.println(total_wait_time.get()/num_enqueues.get());
        double sliceUsage = (double) slice_usage_last.get() / TIME_SLICE;
        System.out.printf("Slice usage: %d\n", slice_usage_last.get());
        System.out.printf("Slice usage: %.2f\n", sliceUsage);
        System.out.printf("queue: %b", last_queue.get());
    }

    void statsLoop() {
        try {
            while (true) {
                System.out.println("Stats:\n");
                Thread.sleep(1000);
                printStats();
                total_wait_time.set(0L);
                num_enqueues.set(0L);
                
            }
        } catch (InterruptedException e) {
        }
    }

    public void run() {
        attachScheduler();
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