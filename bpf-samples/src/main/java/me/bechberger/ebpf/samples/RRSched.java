package me.bechberger.ebpf.samples;

import me.bechberger.ebpf.annotations.Unsigned;
import me.bechberger.ebpf.annotations.bpf.BPF;
import me.bechberger.ebpf.annotations.bpf.BPFMapDefinition;
import me.bechberger.ebpf.bpf.BPFProgram;
import me.bechberger.ebpf.bpf.Scheduler;
import me.bechberger.ebpf.type.Ptr;
import picocli.CommandLine;
import me.bechberger.ebpf.bpf.map.BPFHashMap;
import me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;

import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_dsq_id_flags.SCX_DSQ_LOCAL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_public_consts.SCX_SLICE_DFL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.*;
import static me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;

import static me.bechberger.ebpf.runtime.helpers.BPFHelpers.bpf_get_smp_processor_id;

@BPF(license = "GPL")
public abstract class RRSched extends BPFProgram implements Scheduler {

    private static final int MAX_CPUS = 64; // Adjust based on your system
    private static final int MAX_PROCESSES = 10000;

    // Per-CPU round-robin index to track the next task
    @BPFMapDefinition(maxEntries = MAX_CPUS)
    BPFHashMap<Integer, @Unsigned Integer> perCpuNextTaskIndex;

    // Array map to emulate a global process queue
    @BPFMapDefinition(maxEntries = MAX_PROCESSES)
    BPFHashMap<@Unsigned Integer, @Unsigned Integer> processQueue;

    // Global variable to track the current size of the queue
    @BPFMapDefinition(maxEntries = 1)
    BPFHashMap<Integer, @Unsigned Integer> queueSize;

    @Override
    public int selectCPU(Ptr<task_struct> p, int prev_cpu, long wake_flags) {
        // Default CPU selection logic
        return scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, null);
    }

    @Override
    public void enqueue(Ptr<task_struct> p, long enq_flags) {
        // Add the process PID to the global process queue
        Integer size = queueSize.bpf_get(0).val();
        if (size == null) {
            size = 0;
        }

        processQueue.put(size, p.val().pid);
        queueSize.put(0, size + 1);
    }

    @Override
    public void dispatch(int cpu, Ptr<task_struct> prev) {
        // Integer nextIndex = perCpuNextTaskIndex.bpf_get(cpu).val();
        // if (nextIndex == null) {
        //     nextIndex = 0;
        // }

        // Integer size = queueSize.bpf_get(0).val();
        // if (size == null || size == 0) {
        //     return; // No tasks to dispatch
        // }

        // // Get the PID at the current index in the queue
        // var pid = processQueue.bpf_get(nextIndex);
        // if (pid != null) {
        //     scx_bpf_dispatch_pid(pid, SCX_DSQ_LOCAL.value(), SCX_SLICE_DFL.value(), 0);
        // }

        // // Update the per-CPU next task index in a round-robin fashion
        // nextIndex = (nextIndex + 1) % size;
        // perCpuNextTaskIndex.put(cpu, nextIndex);
        scx_bpf_consume(SHARED_DSQ_ID);
    }

    @Override
    public void running(Ptr<task_struct> p) {
        // No additional logic needed for running tasks in this scheduler
    }

    @Override
    public void stopping(Ptr<task_struct> p, boolean runnable) {
        // Place runnable tasks back in the queue
        if (runnable) {
            enqueue(p, 0);
        }
    }

    @Override
    public void enable(Ptr<task_struct> p) {
        // Initialize the process's dispatch parameters
        p.val().scx.dsq_vtime = 0;
    }

    @Override
    public int init() {
        // Initialize the queue size
        queueSize.put(0, 0);

        // Initialize per-CPU task indices
        for (int i = 0; i < MAX_CPUS; i++) {
            perCpuNextTaskIndex.put(i, 0);
        }

        return 0;
    }

    public static void main(String[] args) {
        try (var program = BPFProgram.load(RRSched.class)) {
            new CommandLine(program).execute(args);
        }
    }
}
