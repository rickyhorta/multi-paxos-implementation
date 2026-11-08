package didameetings.util;

import io.grpc.stub.StreamObserver;

/**
 * CollectorStreamObserver wraps a GenericResponseCollector and ensures
 * responses are only counted once. It distinguishes between:
 *   - onNext: actual reply received (counted as response),
 *   - onError: RPC failed (counted as no response),
 *   - onCompleted: only counts as no response if no reply ever arrived.
 *
 * This avoids double-counting and ensures bogus acceptors don’t trick
 * the proposer into moving on without a quorum.
 */
public class CollectorStreamObserver<T> implements StreamObserver<T> {

    private final GenericResponseCollector<T> collector;
    private boolean received = false;

    public CollectorStreamObserver(GenericResponseCollector<T> collector) {
        this.collector = collector;
    }

    @Override
    public void onNext(T value) {
        // System.out.println("CollectorStreamObserver received a response: " + value.toString());
        if (!received) {
            collector.addResponse(value);
            return;
        }
    }

    @Override
    public void onError(Throwable t) {
    }

    @Override
    public void onCompleted() {
        received = true;
        return;
    }
}
