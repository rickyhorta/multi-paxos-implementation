package didameetings.server;

import java.util.ArrayList;
import java.util.List;

import didameetings.DidaMeetingsPaxos;
import didameetings.configs.ConfigurationScheduler;
import didameetings.util.GenericResponseProcessor;

/**
 * Processor used by the leader when doing a Multi-Paxos "phase 1".
 *
 * Responsibilities:
 * - track highest maxBallot reported by acceptors (for abortion check)
 * - store all successful Promise responses (log entries and ballots)
 * - count "promises" (acceptors that accepted this ballot)
 * - decide (accepted==true) when we have quorum promises
 *
 * NOTE: This class is a PURE COLLECTOR. It must NOT modify serverState.paxos_log.
 * Value Forcing (log merging) must be done by the main leader loop AFTER the quorum is reached.
 */
public class MultiPhaseOneProcessor extends GenericResponseProcessor<DidaMeetingsPaxos.MultiPhaseOneReply> {

    // Removed direct reference to serverState (except for initialization)
    private final ConfigurationScheduler scheduler;
    private final int currentBallot; // B_prep of this leader attempt

    private boolean accepted = false;
    private int maxBallotSeen = -1;
    private boolean done = false;
    
    // Stores only the successful Promise responses for later analysis (Value Forcing).
    // Note: We use the full response object, which contains all the log entries.
    private final List<DidaMeetingsPaxos.MultiPhaseOneReply> successfulPromises = new ArrayList<>();

    public MultiPhaseOneProcessor(DidaMeetingsServerState serverState, ConfigurationScheduler scheduler, int ballot) {
        this.scheduler = scheduler;
        this.currentBallot = ballot;
    }

    public boolean getAccepted() {
        return this.accepted;
    }

    public int getMaxBallotSeen() {
        return this.maxBallotSeen;
    }

    /**
     * Getter for the collected, successful promises.
     * The main leader loop will use this data to perform Value Forcing.
     */
    public List<DidaMeetingsPaxos.MultiPhaseOneReply> getSuccessfulPromises() {
        return this.successfulPromises;
    }

    /**
     * Called each time a MultiPhaseOneResponse arrives.
     * We only check for quorum and abort conditions here; we DO NOT modify the local paxos_log.
     */
    @Override
    public synchronized boolean onNext(ArrayList<DidaMeetingsPaxos.MultiPhaseOneReply> all_responses,
                                       DidaMeetingsPaxos.MultiPhaseOneReply last_response) {

        if (last_response.getMaxballot() > this.maxBallotSeen) {
            this.maxBallotSeen = last_response.getMaxballot();
            
            if (this.maxBallotSeen > this.currentBallot) {
                System.out.println("Aborting Multi-phase1: acceptor reported higher ballot " + this.maxBallotSeen + " than current " + this.currentBallot);
                this.accepted = false;
                this.done = true;
                return true; // stop early
            }
        }
        
        // 2. If the promise was granted, store the response
        if (last_response.getAccepted()){
            // We store the entire response object, which contains the log list (getLogList())
            this.successfulPromises.add(last_response);
        }
        else {
            System.out.println("ENTERED THIS ELSE WITH: " + last_response.toString());
            this.accepted = false;
            this.done = true;
            return true; // stop early
        }

        // 3. Quorum check
        int promiseCount = this.successfulPromises.size();
        
        int quorum = this.scheduler.quorum(this.currentBallot);
        if (promiseCount >= quorum) {
            this.accepted = true;
            this.done = true;
            return true;
        }

        // Not enough yet; keep waiting
        this.accepted = false;
        this.done = false;
        return false;
    }

    /**
     * Called when no more responses will arrive (or pending==0).
     * Final check: if we reached quorum earlier we have accepted=true,
     * otherwise accepted=false.
     */
    @Override
    public synchronized boolean onCompleted(ArrayList<DidaMeetingsPaxos.MultiPhaseOneReply> all_responses) {
        // if not already decided by onNext, compute final promise count
        if (!this.done) {
            // Recalculate based on the collected promises if we haven't already decided
            int promiseCount = this.successfulPromises.size(); 
            
            int quorum = this.scheduler.quorum(this.currentBallot);
            if (promiseCount >= quorum) {
                this.accepted = true;
                this.done = true;
            } 
            else {
                System.out.println("ON COMPLETED: Still not enough promises: " + promiseCount + " out of " + quorum);
                this.accepted = false;
                this.done = false;
            }
        }
        return this.done;
    }
}
