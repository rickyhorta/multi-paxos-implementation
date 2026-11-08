package didameetings.util;

import java.util.ArrayList;

import didameetings.DidaMeetingsPaxos;
import didameetings.configs.ConfigurationScheduler;

public class PhaseOneBogusProcessor extends GenericResponseProcessor<DidaMeetingsPaxos.PhaseOneReply>  {
    private final ConfigurationScheduler scheduler;

    private boolean accepted;      // final decision: did Phase 1 succeed?
    private int value;             // value tied to the highest valballot seen
    private int valballot;         // highest ballot of an accepted value seen
    private int maxballot;         // highest ballot reported by any acceptor
    private final int low_ballot;
    private final int high_ballot;
    private boolean done;

    public PhaseOneBogusProcessor(ConfigurationScheduler s, int l, int h) {
        System.out.println("Phase 1 processor constructor with low_ballot =" + l + " high_ballot = " + h);
        this.scheduler   = s;
        this.accepted    = false;
        this.value       = -1;
        this.valballot   = -1;
        this.high_ballot = h;
        this.done       = false;
        this.low_ballot  = l;
        //this.maxballot   = -1;
        //this.responses   = 0;
        //this.promiseCount = 0;
    }

    public boolean getAccepted() {
        return this.accepted;
    }

    public int getValue() {
        return this.value;
    }

    public int getValballot() {
        return this.valballot;
    }

    public int getMaxballot() {
        return this.maxballot;
    }

    public boolean isDone() {
        return this.done;
    }

    public synchronized boolean onNext(ArrayList<DidaMeetingsPaxos.PhaseOneReply> all_responses,
        DidaMeetingsPaxos.PhaseOneReply last_response) {
    
        if (last_response.getMaxballot() > this.high_ballot) {
            this.done = true;
            this.accepted = false;
            return true;
        }

        int current_valballot = -1;
        int current_value = -1;
        int promise_count = 0;
        
        for (DidaMeetingsPaxos.PhaseOneReply response : all_responses) {
            if (response.getAccepted()) {
                promise_count++;
                if (response.getValballot() > current_valballot) {
                    current_valballot = response.getValballot();
                    current_value = response.getValue();
                }
            }
        }

        this.valballot = current_valballot;
        this.value = current_value;

        if (promise_count >= this.scheduler.quorum(this.high_ballot)) {
            this.accepted = true;
            this.done = true;
        } else {
            this.accepted = false;
            this.done = false;
        }
        
        return this.done;
}
    
    public synchronized boolean onCompleted(ArrayList<DidaMeetingsPaxos.PhaseOneReply> all_responses) {
        return this.done;
    }
}
