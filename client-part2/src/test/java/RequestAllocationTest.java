public final class RequestAllocationTest {
    public static void main(String[] args) {
        for (int threads : new int[] {64, 128, 256, 512}) {
            for (int events : new int[] {0, 1, 100, 32000, 468000, 500000}) {
                int sum = 0, min = Integer.MAX_VALUE, max = 0;
                for (int i = 0; i < threads; i++) {
                    int n = RequestAllocation.requestsForWorker(events, threads, i);
                    sum += n; min = Math.min(min, n); max = Math.max(max, n);
                }
                assert sum == events : "wrong request count";
                assert max - min <= 1 : "unbalanced workers";
            }
        }
        System.out.println("Request allocation checks passed for 64/128/256/512 threads");
    }
}
