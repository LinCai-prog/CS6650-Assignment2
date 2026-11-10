/** Preserve the exact workload when its size is not divisible by thread count. */
public final class RequestAllocation {
    private RequestAllocation() {}
    public static int requestsForWorker(int events, int threads, int index) {
        if (events < 0 || threads < 1 || index < 0 || index >= threads) throw new IllegalArgumentException("invalid workload partition");
        return events / threads + (index < events % threads ? 1 : 0);
    }
}
