package me.bechberger.ebpf.samples;

import me.bechberger.ebpf.annotations.Unsigned;
import me.bechberger.ebpf.annotations.bpf.BPF;
import me.bechberger.ebpf.annotations.bpf.BPFMapDefinition;
import me.bechberger.ebpf.bpf.BPFProgram;
import me.bechberger.ebpf.bpf.Scheduler;
import me.bechberger.ebpf.type.Ptr;
import picocli.CommandLine;
import me.bechberger.ebpf.bpf.map.BPFHashMap;
import me.bechberger.ebpf.bpf.map.BPFLRUHashMap;
import me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;

import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_dsq_id_flags.SCX_DSQ_LOCAL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.scx_public_consts.SCX_SLICE_DFL;
import static me.bechberger.ebpf.runtime.ScxDefinitions.*;
import static me.bechberger.ebpf.runtime.TaskDefinitions.task_struct;

import static me.bechberger.ebpf.runtime.helpers.BPFHelpers.bpf_get_smp_processor_id;

@BPF(license = "GPL")
public abstract class RoundRobinSched extends BPFProgram implements Scheduler {

    // The queue where all runnable processes are stored
    static final long RR_DSQ_ID=0;

    // We don't use SCX_SLICE_DFL (our time slice in ns)
    // static final long TIME_SLICE=100000000;
    static final long TIME_SLICE=10000;

    // @BPFMapDefinition(maxEntries = 100000)
    // BPFLRUHashMap<@unsigned Integer, @unsigned double> wait_time;

    @Override
    public int init() {
        return scx_bpf_create_dsq(RR_DSQ_ID, -1);
    }

    @Override
    public int selectCPU(Ptr<task_struct> p, int prev_cpu, long wake_flags) {
        boolean is_idle = false;
        int cpu = scx_bpf_select_cpu_dfl(p, prev_cpu, wake_flags, Ptr.of(is_idle));
        if (is_idle) {
            // We skip the enqueue call
            // sends p to the local queue of the cpu and uses the default time slice value
            scx_bpf_dispatch(p, SCX_DSQ_LOCAL.value(), TIME_SLICE,0);
        }
        return cpu;
    }

    @Override
    public void enqueue(Ptr<task_struct> p, long enq_flags) {
        // No CPU was ready so we put p in our waiting queue
        scx_bpf_dispatch(p, RR_DSQ_ID, SCX_SLICE_DFL.value(), enq_flags);
    
    }

    @Override
    public void dispatch(int cpu, Ptr<task_struct> prev) {
        // cpu and prev unused, prev could be used to track the task that just came of the cpu. cpu is likely implicitly used inside the consume
        // Place the first task into the local DSQ of the cpu
        scx_bpf_consume(RR_DSQ_ID);
    }

    @Override
    public void running(Ptr<task_struct> p) {
        // p.value()
        return;
    }

    public static void main(String[] args) {
        try (var program = BPFProgram.load(SampleScheduler.class)) {
            new CommandLine(program).execute(args);
        }
    }

}