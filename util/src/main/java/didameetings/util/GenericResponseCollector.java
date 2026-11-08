package didameetings.util;

import java.util.ArrayList;

public class GenericResponseCollector<T>  {
    private GenericResponseProcessor     processor;
    private ArrayList<T>                 collected_responses;
    private int                          received;
    private int                          pending;
    private boolean                      done;

    public GenericResponseCollector(ArrayList<T> responses, int maxresponses) {
        this.processor           = null;
        this.collected_responses = responses;
        this.received            = 0;
        this.done                = false;
    }

    public GenericResponseCollector(ArrayList<T> responses, int maxresponses, GenericResponseProcessor p) {
        this.processor           = p;
        this.collected_responses = responses;
        this.received            = 0;
        this.done                = false;
    }

    public synchronized void addResponse(T resp) {
        if (this.done) {
            return; // already finished — ignore late responses
        }

        collected_responses.add(resp);
        this.received++;

        // If a processor exists, let it evaluate the new response
        if (this.processor != null) {
            // The processor's onNext() method will return true if a final decision has been made
            this.processor.onNext(this.collected_responses, resp);
            this.done = this.processor.onCompleted(this.collected_responses);
        }
        notifyAll();
}
 
    public synchronized void waitForQuorum(int quorum) {
        // System.out.println("Waiting for quorum of " + quorum + " responses (so far received " + this.received + " with " + this.pending + " pending)");
        // Continue waiting until the processor indicates a final decision (either success or failure)
        while (this.received < quorum) {
            try {   
                wait();
                // System.out.println("Woke up, checking quorum: received " + this.received + " out of " + quorum);
            }
            catch (InterruptedException e) {
            }
        }
        return;
    }

    public synchronized void waitUntilDone() {
        // System.out.println("Waiting until done (so far received " + this.received + " with " + this.pending + " pending)");
        // Continue waiting until the processor indicates a final decision (either success or failure)
        while (!this.done) {
            try {   
                wait();
                // System.out.println("Woke up, checking done: done = " + this.done);
            }
            catch (InterruptedException e) {
            }
        }
        return;
    }
}
