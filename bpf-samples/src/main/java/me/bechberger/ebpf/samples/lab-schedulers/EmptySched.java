package me.bechberger.ebpf.samples;

import me.bechberger.ebpf.annotations.bpf.BPF;
import me.bechberger.ebpf.annotations.bpf.BPFMapDefinition;
import me.bechberger.ebpf.annotations.bpf.Property;
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
@Property(name = "sched_name", value = "empty_sched")
public abstract class EmptySched extends BPFProgram implements Scheduler, Runnable {
    @Option(names = "--verbose")
    boolean verbose = false;

    // The queue where all runnable processes are stored
    static final long RR_DSQ_ID=0;

    // We don't use SCX_SLICE_DFL (our time slice in ns)
    // static final long TIME_SLICE=100000000;
    static final long TIME_SLICE=10000;


    final GlobalVariable<@Unsigned Long> total_wait_time = new GlobalVariable<>(0L);

    final GlobalVariable<@Unsigned Long> num_enqueues = new GlobalVariable<>(0L);

    @BPFMapDefinition(maxEntries = 100000)
    BPFLRUHashMap<@Unsigned Integer, @Unsigned Long> enqueue_time;


    @Override
    public int init() {
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
        // // No CPU was ready so we put p in our waiting queue
        // scx_bpf_dispatch(p, RR_DSQ_ID, TIME_SLICE, enq_flags);
        
        // record t_enqueue
        long time = bpf_ktime_get_ns();
        enqueue_time.put(Integer.valueOf(p.val().pid), time);
    
    }

    @Override
    public void dispatch(int cpu, Ptr<task_struct> prev) {
        // // cpu and prev unused, prev could be used to track the task that just came of the cpu. cpu is likely implicitly used inside the consume
        // // Place the first task into the local DSQ of the cpu
        // scx_bpf_consume(RR_DSQ_ID);
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
    }

    void printStats(){
        System.out.println("Average wait time: ");
        System.out.println(total_wait_time.get()/num_enqueues.get());
    }

    void statsLoop() {
        try {
            while (true) {
                System.out.println("Stats:");
                Thread.sleep(10000);
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
        try (var program = BPFProgram.load(RoundRobinSched.class)) {
            new CommandLine(program).execute(args);
        }
    }

}