package me.bechberger.ebpf.bpf;

import me.bechberger.ebpf.annotations.bpf.*;
import me.bechberger.ebpf.annotations.bpf.Properties;
import me.bechberger.ebpf.bpf.map.*;
import me.bechberger.ebpf.bpf.map.BPFRingBuffer.BPFRingBufferError;
import me.bechberger.ebpf.bpf.processor.Processor;
import me.bechberger.ebpf.bpf.raw.*;
import me.bechberger.ebpf.shared.KernelFeatures;
import me.bechberger.ebpf.shared.LibC;
import me.bechberger.ebpf.type.BPFType;
import me.bechberger.ebpf.shared.PanamaUtil;
import me.bechberger.ebpf.shared.PanamaUtil.HandlerWithErrno;
import me.bechberger.ebpf.shared.TraceLog;
import me.bechberger.ebpf.shared.TraceLog.TraceFields;
import me.bechberger.ebpf.type.BPFType.BPFStructType;
import me.bechberger.ebpf.type.BPFType.BPFUnionType;
import me.bechberger.ebpf.type.Union;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static me.bechberger.ebpf.NameUtil.toConstantCase;
import static me.bechberger.ebpf.bpf.raw.Lib.*;

/**
 * Base class for bpf programs.
 * <p></p>
 * {@snippet :
 *     @BPF
 *     public static abstract class HelloWorldProgram extends BPFProgram {
 *
 *         static final String EBPF_PROGRAM = """
 *                 #include "vmlinux.h"
 *                 #include <bpf/bpf_helpers.h>
 *                 #include <bpf/bpf_tracing.h>
 *
 *                 SEC ("kprobe/do_sys_openat2") int kprobe__do_sys_openat2 (struct pt_regs *ctx){
 *                     bpf_printk("Hello, World from BPF and more!");
 *                     return 0;
 *                 }
 *
 *                 char _license[] SEC ("license") = "GPL";
 *                 """;
 *     }
 *
 *     public static void main(String[] args) {
 *         try (HelloWorldProgram program = new HelloWorldProgramImpl()) {
 *             program.autoAttachProgram(program.getProgramByName("kprobe__do_sys_openat2"));
 *             program.tracePrintLoop();
 *         }
 *     }
 *}
 */
public abstract class BPFProgram implements AutoCloseable {

    static {
        LibraryLoader.load();
        // set rlimit to allow for more maps
        LibC.setRlimitMemlockToInfinity();
    }

    /**
     * Thrown whenever the whole bpf program could not be loaded
     */
    public static class BPFLoadError extends BPFError {

        public BPFLoadError(String message) {
            super(message);
        }
    }

    /** Get the class generated by the annotation processor for a giving subclass of {@link BPFProgram} */
    @SuppressWarnings("unchecked")
    public static <T, S extends T> Class<S> getImplClass(Class<T> clazz) {
        try {
            var implName = Processor.classToImplName(clazz);
            return (Class<S>) Class.forName(implName.fullyQualifiedClassName());
        } catch (ClassNotFoundException e) {
            throw new BPFError("Implementation class not found, you probably forgot to annotate the " + clazz.getSimpleName() + " class with @BPF", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the C code for the given BPFProgram subclass
     */
    public static <T extends BPFProgram> String getCode(Class<T> clazz) {
        try {
            return getImplClass(clazz).getMethod("getCodeStatic").invoke(null).toString();
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads the implementation class of the given abstract BPFProgram subclass
     * <p>
     * Example: {@snippet :
     *    HelloWorld program = BPFProgram.load(HelloWorld.class);
     *}
     *
     * @param clazz abstract BPFProgram subclass
     * @param <T>   the abstract BPFProgram subclass
     * @param <S>   the implementation class
     * @return instance of the implementation class, created using the default constructor
     */
    public static <T extends BPFProgram, S extends T> S load(Class<T> clazz) {
        try {
            KernelFeatures.checkRequirements("Loading BPF program", clazz);
            var program = BPFProgram.<T, S>getImplClass(clazz).getConstructor().newInstance();
            program.initGlobals();
            return program;
        } catch (BPFError e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The eBPF object, struct bpf_object *ebpf_object
     */
    private final MemorySegment ebpf_object;

    /**
     * Link to an attached program
     */
    public record BPFLink(MemorySegment segment) {}

    private final Set<BPFLink> attachedPrograms = new HashSet<>();
    private final Set<Integer> openedFDs = new HashSet<>();

    private final Set<BPFMap> attachedMaps = new HashSet<>();

    private final Set<MemorySegment> attachedStructOps = new HashSet<>();

    record AttachedXDPIfIndex(int ifindex, int flags) {}

    private final Set<AttachedXDPIfIndex> attachedXDPIfIndexes = new HashSet<>();

    record AttachedTCIfIndex(ProgramHandle handle, int ifindex, boolean ingress, int priority) {}
    private final Set<AttachedTCIfIndex> attachedTCIfIndices = new HashSet<>();

    private volatile boolean closed = false;

    /**
     * Load the eBPF program from the byte code
     * <p>
     * You have to call {@link #initGlobals()} to initialize the global variables
     */
    public BPFProgram() {
        this.ebpf_object = loadProgram();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    protected void initGlobals() {
    }

    public <T> BPFType<T> getTypeForClass(Class<T> innerType) {
        return getTypeForImplClass(getClass(), innerType);
    }

    public static <T> BPFType<T> getTypeForClass(Class<?> outer, Class<T> inner) {
        return getTypeForImplClass(getImplClass(outer), inner);
    }

    public <T> BPFStructType<T> getStructTypeForClass(Class<T> innerType) {
        return (BPFStructType<T>) getTypeForImplClass(getClass(), innerType);
    }

    public static <T> BPFStructType<T> getStructTypeForClass(Class<?> outer, Class<T> inner) {
        return (BPFStructType<T>) getTypeForImplClass(getImplClass(outer), inner);
    }

    public <T extends Union> BPFUnionType<T> getUnionTypeForClass(Class<T> innerType) {
        return (BPFUnionType<T>) getTypeForImplClass(getClass(), innerType);
    }

    public static <T extends Union> BPFUnionType<T> getUnionTypeForClass(Class<?> outer, Class<T> inner) {
        return (BPFUnionType<T>) getTypeForImplClass(getImplClass(outer), inner);
    }

    private static <T> BPFType<T> getTypeForImplClass(Class<?> outerImpl, Class<T> inner) {
        try {
            return getTypeForImplClass(outerImpl, inner, true);
        } catch (Exception e) {
            return getTypeForImplClass(outerImpl, inner, false);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> BPFType<T> getTypeForImplClass(Class<?> outerImpl, Class<T> inner, boolean canonical) {
        String fieldName = toConstantCase(canonical ? inner.getCanonicalName() : inner.getSimpleName())
                .replace(".", "__");
        try {
            return (BPFType<T>) outerImpl.getDeclaredField(fieldName).get(null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static final HandlerWithErrno<Integer> BPF_OBJECT__LOAD =
            new HandlerWithErrno<>("bpf_object__load",
                    FunctionDescriptor.of(JAVA_INT, PanamaUtil.POINTER));

    private static final HandlerWithErrno<MemorySegment> BPF_OBJECT__OPEN_FILE =
            new HandlerWithErrno<>("bpf_object__open_file",
                    FunctionDescriptor.of(PanamaUtil.POINTER, PanamaUtil.POINTER, PanamaUtil.POINTER));
    /**
     * Load the eBPF program from the byte code
     *
     * @return the eBPF object
     * @throws BPFLoadError if the whole program could not be loaded
     */
    private MemorySegment loadProgram() {
        Path objFile = getTmpObjectFile();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment fileName = arena.allocateFrom(objFile.toString());

            var ebpf_object = BPF_OBJECT__OPEN_FILE.call(fileName, MemorySegment.NULL);
            if (ebpf_object.result() == MemorySegment.NULL) {
                throw new BPFLoadError("Failed to open eBPF file: " + Util.errnoString(ebpf_object.err()));
            }

            var ret = BPF_OBJECT__LOAD.call(ebpf_object.result());
            if (ret.hasError() && ret.result() != 0) {
                throw new BPFLoadError("Failed to load eBPF object: " + Util.errnoString(ret.err()));
            }
            return ebpf_object.result();
        }
    }

    /**
     * Get the names of all functions that represent auto-attachable programs
     * and are defined in C, see {@link #autoAttachPrograms()}.
     * @return the names of the auto-attachable programs
     */
    public abstract List<String> getAutoAttachablePrograms();

    /**
     * All auto-attachable programs, defined in C and in Java.
     * <p>
     * Also includes all methods that are annotated with {@link BPFFunction} and
     * @return names of all auto-attachable programs
     */
    public List<String> getAllAutoAttachablePrograms() {
        // get all methods that are annotated with BPFFunction and where autoAttach is true
        var names = new ArrayList<>(getAutoAttachablePrograms());
        var programClass = getClass().getSuperclass();
        var erroneous = new ArrayList<String>();
        for (var method : programClass.getDeclaredMethods()) {
            var annotation = findParentAnnotation(programClass, method, BPFFunction.class);
            if (annotation != null && annotation.autoAttach()) {
                var baseSection = annotation.section().split("/")[0];
                if (!BPFFunction.autoAttachableSections.contains(baseSection)) {
                    erroneous.add(method.getName() + " with section " + annotation.section());
                    continue;
                }
                names.add(annotation.name().isEmpty() ? method.getName() : annotation.name());
            }
        }
        if (!erroneous.isEmpty()) {
            throw new BPFError("Auto-attachable sections are: " + BPFFunction.autoAttachableSections +
                    ", but the following methods have invalid sections: " + erroneous);
        }
        return names;
    }

    public void attachLSMHooks() {
        for (var method : getClass().getSuperclass().getDeclaredMethods()) {
            var annotation = findParentAnnotation(getClass().getSuperclass(), method, BPFFunction.class);
            if (annotation != null && annotation.section().startsWith("lsm/")) {
                attachLSMHook(getProgramByName(getBPFFunctionName(method)));
            }
        }
    }

    private static final HandlerWithErrno<MemorySegment> BPF_PROGRAM__ATTACH_LSM =
            new HandlerWithErrno<>("bpf_program__attach_lsm",
                    FunctionDescriptor.of(PanamaUtil.POINTER, PanamaUtil.POINTER));

    public void attachLSMHook(ProgramHandle prog) {
        var ret = BPF_PROGRAM__ATTACH_LSM.call(prog.prog());
        if (ret.result() == MemorySegment.NULL) {
            throw new BPFAttachError(prog.name, ret.err());
        }
        var link = new BPFLink(ret.result());
        if (link.segment.address() == 0) {
            throw new BPFAttachError(prog.name, ret.err());
        }
        attachedPrograms.add(link);
    }

    private <T extends Annotation> @Nullable T findParentAnnotation(Class<?> programClass, Method method, Class<T> annotationClass) {
        var annotation = method.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }
        for (var iface : programClass.getInterfaces()) {
            try {
                var ifaceMethod = iface.getMethod(method.getName(), method.getParameterTypes());
                annotation = ifaceMethod.getAnnotation(annotationClass);
                if (annotation != null) {
                    return annotation;
                } else {
                    return findParentAnnotation(iface, ifaceMethod, annotationClass);
                }
            } catch (NoSuchMethodException ignored) {
                // Method not found in this interface, ignore and continue
            }
        }
        return null;
    }

    /**
     * Get the names of all Java defined functions that represent auto-attachable programs of the passed class.
     * <p>
     * All methods attached with {@link BPFFunction} and {@link BPFFunction#autoAttach()} is true.
     */
    public static List<String> getAutoAttachableBPFPrograms(Class<? extends BPFProgram> clazz) {
        return Arrays.stream(clazz.getMethods()).map(m -> {
                    var ann = getAnnotationOfSelfOrOverriden(m, BPFFunction.class);
                    if (ann != null && ann.autoAttach()) {
                        return ann.name().isEmpty() ? m.getName() : ann.name();
                    }
                    return null;
                })
                .filter(Objects::nonNull).toList();
    }

    private static <T extends Annotation> @Nullable T getAnnotationOfSelfOrOverriden(Method method, Class<T> annotationClass) {
        // Get annotations of the current method
        T annotation = method.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }

        // Get annotations from methods in interfaces
        Class<?> declaringClass = method.getDeclaringClass();
        for (Class<?> iface : declaringClass.getInterfaces()) {
            try {
                Method ifaceMethod = iface.getMethod(method.getName(), method.getParameterTypes());
                T ifaceAnnotation = ifaceMethod.getAnnotation(annotationClass);
                if (ifaceAnnotation != null) {
                    return ifaceAnnotation;
                }
            } catch (NoSuchMethodException ignored) {
                // Method not found in this interface, ignore and continue
            }
        }

        return null;
    }

    private static String getBPFFunctionName(Method method) {
        var annotation = getAnnotationOfSelfOrOverriden(method, BPFFunction.class);
        if (annotation == null || annotation.name().isEmpty()) {
            return method.getName();
        }
        return annotation.name();
    }

    /**
     * Get the byte code of the bpf program.
     *
     * @return the byte code
     */
    public abstract byte[] getByteCode();

    /**
     * Get the code of the bpf program.
     *
     * @return the C code
     */
    public abstract String getCode();

    private Path getTmpObjectFile() {
        try {
            Path tmp = Files.createTempFile("bpf", ".o");
            tmp.toFile().deleteOnExit();
            try (var os = Files.newOutputStream(tmp)) {
                os.write(getByteCode());
            }
            return tmp;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A handle to an ebpf program (an entry point function in the eBPF program)
     */
    public record ProgramHandle(String name, MemorySegment prog) {
    }

    public static class BPFProgramNotFound extends BPFError {
        public BPFProgramNotFound(String name) {
            super("Program not found: " + name);
        }
    }

    /**
     * Get a program handle by name
     *
     * @param name the name of the program, or null, if the program cannot be found
     * @return the program handle
     * @throws BPFProgramNotFound if the program cannot be found
     */
    public ProgramHandle getProgramByName(String name) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment prog = Lib.bpf_object__find_program_by_name(this.ebpf_object, arena.allocateFrom(name));
            if (prog == MemorySegment.NULL || prog.address() == 0) {
                throw new BPFProgramNotFound(name);
            }
            return new ProgramHandle(name, prog);
        }
    }

    /**
     * Thrown when attaching a specific program / entry function fails
     */
    public static class BPFAttachError extends BPFError {

        private final int error;

        public BPFAttachError(String name, int errorCode) {
            super("Failed to attach " + name, errorCode);
            this.error = errorCode;
        }

        public BPFAttachError(String name, String message) {
            super("Failed to attach " + name + ":" + message);
            this.error = 0;
        }

        public int getErrorCode() {
            return error;
        }
    }

    private static final HandlerWithErrno<MemorySegment> BPF_PROGRAM__ATTACH =
            new HandlerWithErrno<>("bpf_program__attach",
                    FunctionDescriptor.of(PanamaUtil.POINTER, PanamaUtil.POINTER));

    /**
     * Attach the program by the automatically detected program type, attach type, and extra paremeters, where
     * applicable.
     *
     * @param prog program to attach
     * @throws BPFAttachError when attaching fails
     */
    public BPFLink autoAttachProgram(ProgramHandle prog) {
        var ret = BPF_PROGRAM__ATTACH.call(prog.prog());
        if (ret.result() == MemorySegment.NULL) {
            throw new BPFAttachError(prog.name, ret.err());
        }
        var link = new BPFLink(ret.result());
        if (link.segment.address() == 0) {
            throw new BPFAttachError(prog.name, ret.err());
        }
        attachedPrograms.add(link);
        return link;
    }

    /**
     * Attach the program by the automatically detected program type, attach type, and extra paremeters, where
     * applicable.
     *
     * @param name name of the program to attach
     * @throws BPFAttachError when attaching fails
     */
    public BPFLink autoAttachProgram(String name) {
        return autoAttachProgram(getProgramByName(name));
    }

    /**
     * Attach all programs by the automatically detected program type, attach type, and extra paremeters, where
     * applicable.
     * <p>
     * Auto-attaches all programs that are prefixed by "SEC(...)" in the eBPF program.
     * It works with
     * <li>
     *     <ul>fentry, fexit of syscalls: e.g. <code>SEC("fentry/do_unlinkat")
     * int BPF_PROG(do_unlinkat, int dfd, struct filename *name)</code>, <code>SEC("fexit/do_unlinkat")
     * int BPF_PROG(do_unlinkat_exit, int dfd, struct filename *name, long ret)</code></ul>
     * </li>
     * See <a href="https://man7.org/linux/man-pages/man2/syscalls.2.html">syscalls(2)</a> for a list of syscalls.
     * @return
     */
    public BPFProgram autoAttachPrograms() {
        for (var name : getAllAutoAttachablePrograms()) {
            autoAttachProgram(name);
        }
        return this;
    }

    public void xdpAttach(ProgramHandle prog, List<Integer> ifindex) {
        for (var index : ifindex) {
            xdpAttach(prog, index);
        }
    }

    public void xdpAttach(ProgramHandle prog, int ifindex) {
        int fd = Lib.bpf_program__fd(prog.prog());
        int flags = NetworkUtil.XDP_FLAGS_UPDATE_IF_NOEXIST;
        int err = Lib.bpf_xdp_attach(ifindex, fd, flags, MemorySegment.NULL);
        if (err > 0) {
            throw new BPFAttachError(prog.name, err);
        }
        attachedXDPIfIndexes.add(new AttachedXDPIfIndex(ifindex, flags));
    }

    public void tcAttach(ProgramHandle prog, List<Integer> ifindex, boolean ingress) {
        for (var index : ifindex) {
            tcAttach(prog, index, ingress);
        }
    }

    public void tcAttach(ProgramHandle prog, int ifindex, boolean ingress) {
        var tcIfIndex = new AttachedTCIfIndex(prog, ifindex, ingress, 0);
        try (var arena = Arena.ofConfined()) {
            MemorySegment hook = allocateTCHookObject(arena, tcIfIndex);
            // run tc qdisc del dev $DEVICE clsact on the command line
            // to remove the clsact qdisc if it exists
            try {
                Runtime.getRuntime().exec(new String[]{"tc", "qdisc", "del", "dev", NetworkUtil.getNetworkInterfaceName(ifindex), "clsact"}).waitFor();
            } catch (IOException | InterruptedException e) {
            }
            hook = allocateTCHookObject(arena, tcIfIndex);
            MemorySegment opts = allocateTCOptsObject(arena, tcIfIndex);
            int err = Lib.bpf_tc_hook_create(hook);
            if (err > 0) {
                throw new BPFAttachError(prog.name, err);
            }
            err = Lib.bpf_tc_attach(hook, opts);
            if (err > 0) {
                throw new BPFAttachError(prog.name, err);
            }
            attachedTCIfIndices.add(tcIfIndex);
        }
    }

    /**
     * Find the cgroup path by name
     * @param cgroupName the name of the cgroup
     * @return the path to the cgroup, or null if the cgroup could not be found
     */
    public static @Nullable Path findCGroupPath(String cgroupName) {
        try {
            return Files.list(Path.of("/sys/fs/cgroup")).filter(p -> p.getFileName().toString().equals(cgroupName)).findFirst().orElse(null);
        } catch (IOException e) {
            return null;
        }
    }

    private static final HandlerWithErrno<MemorySegment> BPF_PROGRAM__ATTACH_CGROUP =
            new HandlerWithErrno<>("bpf_program__attach_cgroup",
                    FunctionDescriptor.of(PanamaUtil.POINTER, PanamaUtil.POINTER, JAVA_INT));

    private void cgroupAttachInternal(ProgramHandle handle, String cgroupName) {
        var cgroupPath = findCGroupPath(cgroupName);
        if (cgroupPath == null) {
            throw new BPFAttachError(handle.name, "Cgroup not found: " + cgroupName);
        }
        PanamaUtil.ResultAndErr<Integer> fileFD = LibC.open(cgroupPath, O_RDONLY());
        if (fileFD.err() > 0) {
            throw new BPFAttachError(handle.name, fileFD.err());
        }
        openedFDs.add(fileFD.result());

        var resultAndErr = BPF_PROGRAM__ATTACH_CGROUP.call(handle.prog, fileFD.result());
        if (resultAndErr.err() > 0) {
            throw new BPFAttachError(handle.name, resultAndErr.err());
        }
        if (resultAndErr.result() == MemorySegment.NULL) {
            throw new BPFAttachError(handle.name, resultAndErr.err());
        }
        attachedPrograms.add(new BPFLink(resultAndErr.result()));
    }

    public void cgroupAttach(ProgramHandle handle, String cgroupName) {
        // tries it four times before giving up
        for (int i = 0; i < 3; i++) {
            try {
                cgroupAttachInternal(handle, cgroupName);
                return;
            } catch (BPFAttachError e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interruptedException) {
                    throw new BPFAttachError(handle.name, "Cgroup not found: " + cgroupName);
                }
            }
        }
        cgroupAttachInternal(handle, cgroupName);
    }

    private MemorySegment allocateTCHookObject(Arena arena, AttachedTCIfIndex tcIfIndex) {
        MemorySegment hook = arena.allocate(bpf_tc_hook.sizeof());
        hook.fill((byte) 0);
        bpf_tc_hook.sz(hook, bpf_tc_hook.sizeof());
        bpf_tc_hook.ifindex(hook, tcIfIndex.ifindex);
        bpf_tc_hook.attach_point(hook, tcIfIndex.ingress ? BPF_TC_INGRESS() : BPF_TC_EGRESS());
        bpf_tc_hook.parent(hook, 0);
        return hook;
    }

    private MemorySegment allocateTCOptsObject(Arena arena, AttachedTCIfIndex tcIfIndex) {
        MemorySegment opts = arena.allocate(bpf_tc_opts.sizeof());
        var progFd = Lib.bpf_program__fd(tcIfIndex.handle.prog());
        if (progFd <= 0) {
            throw new BPFAttachError(tcIfIndex.handle.name, -progFd);
        }
        opts.fill((byte) 0);
        bpf_tc_opts.sz(opts, bpf_tc_opts.sizeof());
        bpf_tc_opts.handle(opts, 1);
        bpf_tc_opts.prog_fd(opts, progFd);
        bpf_tc_opts.prog_id(opts, 0);
        bpf_tc_opts.priority(opts, tcIfIndex.priority);
        return opts;
    }

    private void tcDetach(AttachedTCIfIndex tcIfIndex) {
        try (var arena = Arena.ofConfined()) {
            MemorySegment hook = allocateTCHookObject(arena, tcIfIndex);
            MemorySegment opts = allocateTCOptsObject(arena, tcIfIndex);
            /*bpf_tc_opts.prog_fd(opts, 0);
            bpf_tc_opts.prog_id(opts, 0);*/
            int err = Lib.bpf_tc_detach(hook, opts);
            if (err > 0) {
                throw new BPFError("Detaching " + tcIfIndex.handle.name, err);
            }
        }
    }

    public void detachProgram(BPFLink link) {
        if (!attachedPrograms.contains(link)) {
            throw new IllegalArgumentException("Program not attached");
        }
        if (link.segment.address() == 0) {
            throw new IllegalArgumentException("Improper link");
        }
        Lib.bpf_link__destroy(link.segment);
        attachedPrograms.remove(link);
    }

    /**
     * Close the program and remove it
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (var structOps : attachedStructOps) {
            Lib.bpf_link__destroy(structOps);
        }
        for (var prog : new HashSet<>(attachedPrograms)) {
            detachProgram(prog);
        }
        for (var ifindex : new HashSet<>(attachedXDPIfIndexes)) {
            Lib.bpf_xdp_detach(ifindex.ifindex, ifindex.flags, MemorySegment.NULL);
        }
        for (var tcIfIndex : new HashSet<>(attachedTCIfIndices)) {
            tcDetach(tcIfIndex);
        }
        for (var map : new HashSet<>(attachedMaps)) {
            map.close();
        }
        Lib.bpf_object__close(this.ebpf_object);
        openedFDs.forEach(LibC::close);
    }

    /**
     * Print the kernel debug trace pipe
     * <p>
     *
     * @see TraceLog#printLoop()
     */
    public void tracePrintLoop() {
        TraceLog.getInstance().printLoop();
    }

    /**
     * Print the kernel debug trace pipe, cleaning the output
     * <p>
     * @see TraceLog#printLoop(boolean)
     */
    public void tracePrintLoopCleaned() {
        TraceLog.getInstance().printLoop(true);
    }

    /**
     * Read from the kernel debug trace pipe and print on stdout.
     *
     * @param format optional function to format the output
     *               <p>
     *               Example
     *               {@snippet *tracePrintLoop(f->f.format("pid {1}, msg = {5}"));
     *}
     */
    public void tracePrintLoop(Function<TraceFields, @Nullable String> format) {
        TraceLog.getInstance().printLoop(format);
    }

    public String readTraceLine() {
        return TraceLog.getInstance().readLine();
    }

    public TraceFields readTraceFields() {
        return TraceLog.getInstance().readFields();
    }


    /**
     * Thrown when a map could not be found
     */
    public static class BPFMapNotFoundError extends BPFError {
        public BPFMapNotFoundError(String name) {
            super("Map not found: " + name);
        }
    }

    /**
     * Get a map descriptor by name
     *
     * @param name the name of the map
     * @return the map descriptor
     * @throws BPFMapNotFoundError if the map cannot be found
     */
    public FileDescriptor getMapDescriptorByName(String name) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment map = Lib.bpf_object__find_map_by_name(this.ebpf_object, arena.allocateFrom(name));
            if (map == MemorySegment.NULL || map.address() == 0) {
                throw new BPFMapNotFoundError(name);
            }
            return new FileDescriptor(name, map, Lib.bpf_map__fd(map));
        }
    }

    public <T extends BPFMap> T recordMap(T map) {
        attachedMaps.add(map);
        return map;
    }

    /**
     * Get a map by name
     *
     * @param name       the name of the map
     * @param mapCreator function to create the map
     * @param <M>        the type of the map
     * @return the map
     * @throws BPFMapNotFoundError       if the map cannot be found
     * @throws BPFMap.BPFMapTypeMismatch if the type of the map does not match the expected type
     */
    public <M extends BPFMap> M getMapByName(String name, Function<FileDescriptor, M> mapCreator) {
        return recordMap(mapCreator.apply(getMapDescriptorByName(name)));
    }

    /**
     * Get a ring buffer by name
     * <p>
     * Keep in mind to regularly call {@link BPFRingBuffer#consumeAndThrow()} to consume the events
     *
     * @param name      the name of the ring buffer
     * @param eventType type of the event
     * @param callback  callback that is called when a new event is received
     * @param <E>       the type of the event
     * @return the ring buffer
     * @throws BPFMapNotFoundError              if the ring buffer cannot be found
     * @throws BPFMap.BPFMapTypeMismatch        if the type of the ring buffer does not match the expected type
     * @throws BPFRingBuffer.BPFRingBufferError if the ring buffer could not be created
     */
    public <E> BPFRingBuffer<E> getRingBufferByName(String name, BPFType<E> eventType,
                                                    BPFRingBuffer.EventCallback<E> callback) {
        return recordMap(getMapByName(name, fd -> new BPFRingBuffer<>(fd, eventType, callback)));
    }

    public <K, V> BPFHashMap<K, V> getHashMapByName(String name, BPFType<K> keyType,
                                                    BPFType<V> valueType) {
        var fd = getMapDescriptorByName(name);
        MapTypeId type = BPFMap.getInfo(fd).type();
        return recordMap(new BPFHashMap<>(fd, type == MapTypeId.LRU_HASH, keyType, valueType));
    }

    private static final HandlerWithErrno<MemorySegment> BPF_MAP__ATTACH_STRUCT_OPS =
            new HandlerWithErrno<>("bpf_map__attach_struct_ops",
                    FunctionDescriptor.of(PanamaUtil.POINTER, PanamaUtil.POINTER));

    public void attachStructOps(String name) {
        var opsDescriptor = getMapDescriptorByName(name);
        if (opsDescriptor == null) {
            throw new BPFMapNotFoundError("Could not find struct ops " + name);
        }

        var res = BPF_MAP__ATTACH_STRUCT_OPS.call(opsDescriptor.map());
        if (res.result() == MemorySegment.NULL && res.hasError()) {
            throw new BPFAttachError("Failed to attach struct ops " + name, res.err());
        }
        attachedStructOps.add(res.result());
    }

    /**
     * Polls data from all ring buffers and consumes if available.
     *
     * @throws BPFRingBufferError if calling the consume method failed,
     *         or if any errors were caught in the call-back of any ring buffer
     */
    public void consumeAndThrow() {
        for (var map : attachedMaps) {
            if (map instanceof BPFRingBuffer) {
                ((BPFRingBuffer<?>)map).consumeAndThrow();
            }
        }
    }

    private @Nullable String getDefaultPropertyValue(String name) {
        ArrayDeque<Class<?>> queue = new ArrayDeque<>(List.of(getClass()));
        while (!queue.isEmpty()) {
            var clazz = queue.poll();
            var annotation = clazz.getAnnotation(PropertyDefinitions.class);
            if (annotation != null) {
                for (var prop : annotation.value()) {
                    if (prop.name().equals(name)) {
                        return prop.defaultValue();
                    }
                }
            }
            var prop = clazz.getAnnotation(PropertyDefinition.class);
            if (prop != null && prop.name().equals(name)) {
                return prop.defaultValue();
            }
            queue.addAll(Arrays.asList(clazz.getInterfaces()));
        }
        return null;
    }

    public @Nullable String getPropertyValue(String name) {
        ArrayDeque<Class<?>> queue = new ArrayDeque<>(List.of(getClass()));
        while (!queue.isEmpty()) {
            var clazz = queue.poll();
            var annotation = clazz.getAnnotation(Properties.class);
            if (annotation != null) {
                for (var prop : annotation.value()) {
                    if (prop.name().equals(name)) {
                        return prop.value();
                    }
                }
            }
            var prop = clazz.getAnnotation(Property.class);
            if (prop != null && prop.name().equals(name)) {
                return prop.value();
            }
            queue.addAll(Arrays.asList(clazz.getInterfaces()));
            if (clazz.getSuperclass() != null) {
                queue.add(clazz.getSuperclass());
            }
        }
        return getDefaultPropertyValue(name);
    }

    public static class BTF {

        private final MemorySegment bpfObject;
        private Map<Integer, BTFType> types = new HashMap<>();

        BTF(MemorySegment bpfObject) {
            this.bpfObject = bpfObject;
        }

        public enum Kind {
            UNKN(0),
            INT(1),
            PTR(2),
            ARRAY(3),
            STRUCT(4),
            UNION(5),
            ENUM(6),
            FWD(7),
            TYPEDEF(8),
            VOLATILE(9),
            CONST(10),
            RESTRICT(11),
            FUNC(12),
            FUNC_PROTO(13),
            VAR(14),
            DATASEC(15),
            FLOAT(16),
            DECL_TAG(17),
            TYPE_TAG(18),
            ENUM64(19);

            private final int value;

            Kind(int value) {
                this.value = value;
            }

            public int value() {
                return value;
            }

            public static Kind fromValue(int value) {
                return values()[value];
            }
        }

        public static class BTFType {

            private static int kind(int info) {
                return (info >> 24) & 0xff;
            }

            private static int vlen(int info) {
                return info & 0xffff;
            }

            private final BTF btf;
            private final MemorySegment typeObj;
            private final Kind kind;
            private final String name;

            public BTFType(BTF btf, MemorySegment typeObj) {
                this.btf = btf;
                this.typeObj = typeObj;
                this.kind = Kind.fromValue(kind(btf_type.info(typeObj)));
                this.name = PanamaUtil.toString(Lib.btf__name_by_offset(btf.bpfObject, btf_type.name_off(typeObj)));
            }

            Kind kind() {
                return kind;
            }

            String name() {
                return name;
            }

            int memberCount() {
                return vlen(btf_type.info(typeObj));
            }

            record VariableSectionInfo(BTFType type, int offset, int size) {
                String name() {
                    return type.name;
                }
            }

            List<VariableSectionInfo> getVariableSectionInfos() {
                // in c code:
                // btf_var_secinfo *ptr = (struct btf_var_secinfo *)(type + 1);
                // assume that btf_var_secinfo and btf_type are 4 byte aligned
                var infos = typeObj.address() + btf_type.sizeof();
                return IntStream.range(0, memberCount()).mapToObj(i -> {
                    var elem = MemorySegment.ofAddress(infos + i * btf_var_secinfo.sizeof()).reinterpret(btf_var_secinfo.sizeof());
                    return new VariableSectionInfo(btf.getTypeById(btf_var_secinfo.type(elem)), btf_var_secinfo.offset(elem), btf_var_secinfo.size(elem));
                }).toList();
            }
        }

        int findIdByName(String name) {
            try (Arena arena = Arena.ofConfined()) {
                int id = Lib.btf__find_by_name(bpfObject, arena.allocateFrom(name));
                if (id < 0) {
                    throw new BPFError("Failed to find BTF by name: " + name);
                }
                return id;
            }
        }

        BTFType getTypeById(int id) {
            return types.computeIfAbsent(id, i -> {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = Lib.btf__type_by_id(bpfObject, id);
                    if (segment == MemorySegment.NULL) {
                        throw new BPFError("Failed to get BTF type by id: " + id);
                    }
                    return new BTFType(this, segment);
                }
            });
        }

        BTFType findTypeByName(String name) {
            return getTypeById(findIdByName(name));
        }
    }

    private BTF btf = null;

    public BTF getBTF() {
        if (btf == null) {
            var ret = Lib.bpf_object__btf(this.ebpf_object);
            if (Lib.libbpf_get_error(ret) != 0) {
                throw new BPFError("Failed to get BTF");
            }
            btf = new BTF(ret);
        }
        return btf;
    }
}