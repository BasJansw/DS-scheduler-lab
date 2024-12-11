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
public abstract class FIFOScheduler extends BPFProgram 
                                    implements Scheduler {

    /**
     * A custom scheduling queue
     */
    static final long SHARED_DSQ_ID = 0;
    
    @Override
    public int init() {
        // init the scheduling queue
        return scx_bpf_create_dsq(SHARED_DSQ_ID, -1);
    }

    @Override
    public int selectCPU(Ptr<task_struct> p, int prev_cpu, 
                         long wake_flags) {
        boolean is_idle = false;
        // let sched-ext select the best CPU
        int cpu = scx_bpf_select_cpu_dfl(p, prev_cpu, 
                                         wake_flags, 
                                         Ptr.of(is_idle));
        if (is_idle) {
            // directly dispatch to the CPU if it is idle
            scx_bpf_dispatch(p, SCX_DSQ_LOCAL.value(), 
                             SCX_SLICE_DFL.value(),0);
        }
        return cpu;
    }

    @Override
    public void enqueue(Ptr<task_struct> p, long enq_flags) {
        // directly dispatch to the selected CPU's local queue
        scx_bpf_dispatch(p, SHARED_DSQ_ID, 
                         SCX_SLICE_DFL.value(), enq_flags);
    }
    
    // ...

    public static void main(String[] args) {
        try (var program = 
             BPFProgram.load(FIFOScheduler.class)) {
            // ...
        }
    }
}