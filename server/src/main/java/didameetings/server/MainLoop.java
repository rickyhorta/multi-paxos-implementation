package didameetings.server;

import java.util.*;

import didameetings.DidaMeetingsMain;
import didameetings.DidaMeetingsPaxos;
import didameetings.DidaMeetingsPaxosServiceGrpc;

import didameetings.util.GenericResponseCollector;
import didameetings.util.CollectorStreamObserver;
import didameetings.util.PhaseTwoResponseProcessor;

import didameetings.configs.ConfigurationScheduler;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class MainLoop implements Runnable  {
    DidaMeetingsServerState server_state;

    private boolean has_work;
    private int next_log_entry;
    private List<Integer> all_participants;
    private int n_participants;
    private String[] targets;
    private ManagedChannel[] channels;
    private DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[] async_stubs;
    
    
    public MainLoop(DidaMeetingsServerState state) {
        this.server_state   = state;
        this.has_work       = false;
        this.next_log_entry = -1;
    }

    public void run() {
        while (true) {
            this.next_log_entry++;
            this.processEntry(this.next_log_entry);
        }
    }
      
    public synchronized void wakeup() {
        this.has_work = true;
        notify();    
    }
   
 
    public synchronized void processEntry(int entry_number) {

        synchronized(server_state) {
            while(server_state.isFrozen()) {
                try {
                    server_state.wait();
                } catch (InterruptedException e) {}
            }

            if (server_state.isSlowed()) {
                try {        
                    int sleepTime = 3000;
                    System.out.println("Sleeping on App Request for " + sleepTime + "ms...");
                    Thread.sleep(sleepTime);
                    System.out.println("Processing...");
                } catch (InterruptedException e) {}        
            }
            
        }

        PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);

        while (next_entry.decided == false) {
            RequestRecord request_record   = this.server_state.req_history.getFirstPending();
            int           ballot           = this.server_state.getCurrentBallot ();
            int           completed_ballot = this.server_state.getCompletedBallot ();

            List<Integer> acceptors        = this.server_state.scheduler.acceptors (ballot);
            int           quorum           = this.server_state.scheduler.quorum (ballot);
            int           n_acceptors      = acceptors.size();
            
            if ((ballot > -1) && (request_record != null) && (this.server_state.scheduler.leader(ballot)==this.server_state.my_id)) {
                
                System.out.println("I am the leader for request with id " + request_record.getId());
                boolean ballot_aborted   = false;
                int phase_two_value      = request_record.getId();
                System.out.println("Value to propose = " + phase_two_value);
                
                // check if we already performed a successful multi-phase1 for this ballot
                boolean already_multi_prepared = (this.server_state.getLeaderBallot() == ballot) &&
                                                 (this.server_state.scheduler.leader(ballot) == this.server_state.my_id);

                if (!already_multi_prepared) {
                    System.out.println("Going to run multi-paxos phase 1 for ballot " + ballot);

                    DidaMeetingsPaxos.MultiPhaseOneRequest.Builder mp1b = DidaMeetingsPaxos.MultiPhaseOneRequest.newBuilder();
                    mp1b.setRequestballot(ballot);
                    mp1b.setInstance(entry_number);
                    DidaMeetingsPaxos.MultiPhaseOneRequest mp1req = mp1b.build();

                    ArrayList<DidaMeetingsPaxos.MultiPhaseOneReply> mp1_responses = new ArrayList<DidaMeetingsPaxos.MultiPhaseOneReply>();
                    MultiPhaseOneProcessor mp1_processor = new MultiPhaseOneProcessor(this.server_state, this.server_state.scheduler, ballot);
                    GenericResponseCollector<DidaMeetingsPaxos.MultiPhaseOneReply> mp1_collector =
                            new GenericResponseCollector<DidaMeetingsPaxos.MultiPhaseOneReply>(mp1_responses, n_acceptors, mp1_processor);

                    for (int i = 0; i < n_acceptors; i++) {
                        CollectorStreamObserver<DidaMeetingsPaxos.MultiPhaseOneReply> mp1_observer = new CollectorStreamObserver<DidaMeetingsPaxos.MultiPhaseOneReply>(mp1_collector);
                        this.server_state.async_stubs[acceptors.get(i)].multiphaseone(mp1req, mp1_observer);
                    }

                    synchronized(this) {
                        System.out.println("Waiting for quorum...");
                        mp1_collector.waitUntilDone();
                    }
                    System.out.println("Returned from wait for multiphase1 with " + mp1_responses.size() + " responses\n");
                    System.out.println("Responses:\n" + mp1_responses);

                    if (mp1_processor.getAccepted() == false) {
                        System.out.println("Multi-phase1 aborted for ballot " + ballot);
                        ballot_aborted = true;
                        int maxballot = mp1_processor.getMaxBallotSeen();
                        if (maxballot > this.server_state.getCurrentBallot())
                            this.server_state.setCurrentBallot(maxballot);
                        this.server_state.clearLeaderBallot();
                    } else {
                        // --- START VALUE FORCING (PAXOS SAFETY) ---
                        this.server_state.setLeaderBallot(ballot);
                        System.out.println("Multi-phase1 succeeded for ballot " + ballot);
                        
                        // Scratchpad map: stores the {Instance ID -> Highest Accepted Log Entry} found so far across the quorum.
                        Map<Integer, DidaMeetingsPaxos.PaxosLog> forcedProposals = new HashMap<>();

                        // 1. Iterate over all successful promises
                        List<DidaMeetingsPaxos.MultiPhaseOneReply> promises = mp1_processor.getSuccessfulPromises();
                        
                        for (DidaMeetingsPaxos.MultiPhaseOneReply promise : promises) {
                            // 2. Iterate over all historical log entries reported by this acceptor
                            for (DidaMeetingsPaxos.PaxosLog acceptedEntry : promise.getLogList()) {
                                
                                int instanceId = acceptedEntry.getInstance();
                                int promisedBallot = acceptedEntry.getValballot();
                                System.out.println("\n\nExamining accepted entry: instance=" + instanceId + ", value=" + acceptedEntry.getValue() + ", valballot=" + promisedBallot + "\n");
                                
                                DidaMeetingsPaxos.PaxosLog currentMax = forcedProposals.get(instanceId);
                                
                                // CORE RULE: Only update if the current promise has a higher accepted ballot (B_acc)
                                if (currentMax == null || promisedBallot > currentMax.getValballot()) {
                                    forcedProposals.put(instanceId, acceptedEntry);
                                }
                            }
                        }

                        // 3. Update the Leader's temporary log (leaderLog) with the forced proposals.
                        // This log now represents the history the leader is forced to respect.
                        for (Map.Entry<Integer, DidaMeetingsPaxos.PaxosLog> entry : forcedProposals.entrySet()) {
                            DidaMeetingsPaxos.PaxosLog forcedLog = entry.getValue();
                            PaxosInstance leaderEntry = this.server_state.getLeaderLog().testAndSetEntry(forcedLog.getInstance());
                            
                            // Copy the accepted value and its ballot into the leader's working log
                            leaderEntry.command_id = forcedLog.getValue();
                            leaderEntry.write_ballot = forcedLog.getValballot();
                            // CRITICAL FIX: DO NOT transfer decided status from Phase 1 reply. 
                            // The decision status is set by Phase 2 completion, not by the acceptor's log report.
                            // leaderEntry.decided = forcedLog.getDecided(); // <--- REMOVED
                        }
                        
                        System.out.println("Value Forcing complete. Leader log initialized with " + forcedProposals.size() + " entries.");
                        // --- END VALUE FORCING ---
                    }
                } else {
                    System.out.println("Skipping multi-phase1 because leaderBallot == " + ballot + " and I'm still leader");
                }

                if (ballot_aborted == false) {
                    System.out.println("Going to run phase 2");

                    int loop_start = 0;
                    int loop_end;

                    if (already_multi_prepared) {
                        // P1 was skipped (optimization): We only propose for the current new request slot.
                        loop_start = entry_number;
                        loop_end = entry_number;
                        System.out.println("Phase 1 optimization active. Only proposing for current slot " + entry_number);
                    } else {
                        // P1 was executed: We must catch up all historical forced values in leaderLog,
                        // up to the highest slot seen or the current entry_number.
                        loop_end = Math.max(entry_number, Math.max(0, this.server_state.getLeaderLog().length() - 1));
                        System.out.println("Phase 1 ran. Catching up slots from 0 to " + loop_end);
                    }

                    // estimate an upper bound: entry_number and leaderLog.length()-1
                    for (int slot = loop_start; slot <= loop_end; slot++) {

                        // detect loss of leadership while catching up
                        if (this.server_state.scheduler.leader(ballot) != this.server_state.my_id) {
                            System.out.println("ABORT: No longer leader for ballot " + ballot + " — stopping");
                            this.server_state.clearLeaderBallot();
                            ballot_aborted = true;
                            break;
                        }
                        
                        // Check Leader Log (leaderLog) for forced value
                        PaxosInstance leaderInst = this.server_state.getLeaderLog().getEntry(slot);

                        Integer valueToPropose = null;

                        // Check if the Leader is FORCED to propose a value (from the quorum history)
                        if (leaderInst != null && leaderInst.write_ballot != -1 && leaderInst.command_id != 0) {
                            // If forced, propose the highest-balleted value found in Phase 1
                            valueToPropose = leaderInst.command_id;
                            
                        // Otherwise, if this is the target slot, propose the new request ID
                        } else if (slot == entry_number) {
                            valueToPropose = phase_two_value;
                        } else {
                            continue; // Skip this slot
                        }

                        System.out.println("Proposing Phase2 for instance " + slot + " value: " + valueToPropose);

                        DidaMeetingsPaxos.PhaseTwoRequest.Builder phase_two_request = DidaMeetingsPaxos.PhaseTwoRequest.newBuilder();
                        phase_two_request.setInstance(slot);
                        phase_two_request.setRequestballot(ballot);
                        phase_two_request.setValue(valueToPropose);

                        PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);
                        ArrayList<DidaMeetingsPaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaMeetingsPaxos.PhaseTwoReply>();
                        GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply> phase_two_collector =
                                new GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);

                        for (int i = 0; i < n_acceptors  ; i++) {
                            CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply> phase_two_observer = new CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_collector);
                            this.server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request.build(), phase_two_observer);
                        }

                        // System.out.println("Waiting for responses...");
                        phase_two_collector.waitUntilDone();

                        if (phase_two_processor.getAccepted() == false) {
                            ballot_aborted = true;
                            int mb = phase_two_processor.getMaxballot();
                            if (mb > this.server_state.getCurrentBallot())
                                this.server_state.setCurrentBallot(mb);
                            this.server_state.clearLeaderBallot();
                            System.out.println("Phase2 aborted for instance " + slot);
                            break;
                        } else {                            
                            PaxosInstance leaderDecidedInst = this.server_state.getLeaderLog().testAndSetEntry(slot);
                            leaderDecidedInst.command_id = valueToPropose;
                            leaderDecidedInst.decided = true;
                            
                            this.server_state.setCompletedBallot(ballot);
                            System.out.println("Instance " + slot + " decided with value " + valueToPropose);
                        }
                    }
                }

                if (ballot_aborted == false) {
                    PaxosInstance decidedEntry = this.server_state.paxos_log.getEntry(entry_number);
                    if (decidedEntry != null && decidedEntry.decided) {
                        next_entry.command_id = decidedEntry.command_id;
                        next_entry.decided = true;
                    }
                }
            } // end if leader

            if (next_entry.decided == false) {
                System.out.println("Entry not decided: waiting");
                this.has_work = false;
                while (this.has_work == false) {
                    try {
                        wait ();
                    }
                    catch (InterruptedException e) {
                    }
                }
            }
            
        } // end while not decided


        System.out.println("Log entry with number " + this.next_log_entry + " has been decided with command id = "+ next_entry.command_id + " and valballot = " + next_entry.accept_ballot);
        RequestRecord request_record = this.server_state.req_history.getIfPending(next_entry.command_id);
        while (request_record == null) {
            System.out.println("Record not available!");
            try {
                wait ();
            }
            catch (InterruptedException e) {
            }
            request_record = this.server_state.req_history.getIfPending(next_entry.command_id);
        }

        DidaMeetingsCommand  command = request_record.getRequest();
        boolean result = false;
        
        DidaMeetingsAction  action = command.getAction();

        switch (action) {
        case OPEN:
            //System.out.println("It is an open request with id = " + command.getMeetingId() + " and max = " + this.server_state.max_participants);
            result = this.server_state.meeting_manager.open(command.getMeetingId(), this.server_state.max_participants);
            break;
        case ADD:
            result = this.server_state.meeting_manager.addAndClose(command.getMeetingId(), command.getParticipantId());
            break;
        case TOPIC:
            result = this.server_state.meeting_manager.setTopic(command.getMeetingId(), command.getParticipantId(), command.getTopicId());
            break;
        case CLOSE:
            result = this.server_state.meeting_manager.close(command.getMeetingId());
            break;
        case DUMP:
            this.server_state.meeting_manager.dump();
            result = true;
            break;
        default:
            result = false;
            System.err.println("*** Unknown command ****");
            break;
        }

        System.out.println("Setting response for command with id = " + next_entry.command_id + " with result = " + result);
        request_record.setResponse(result);
        this.server_state.req_history.moveToProcessed(request_record.getId());
    }
}
