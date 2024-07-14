package com.app.algorithms;

import com.app.SharedMemoryProtobuf.EldTrust;
import com.app.SharedMemoryProtobuf.Message;
import com.app.SharedMemoryProtobuf.ProcessId;
import com.app.system.SharedMemorySystem;

import java.util.*;

public class EventualLeaderDetector implements Algorithm {

    public static final String NAME = "eld";

    private final Set<ProcessId> suspected = new HashSet<>();
    private ProcessId leader = null;
    private SharedMemorySystem sharedMemorySystem;

    public EventualLeaderDetector(final SharedMemorySystem sharedMemorySystem) {
//        distributedSystem.registerAbstraction(new EventuallyPerfectFailureDetector(AbstractionUtils.buildChildAbstractionId(abstractionId, EventuallyPerfectFailureDetector.NAME), distributedSystem));
        this.sharedMemorySystem = sharedMemorySystem;
    }

    @Override
    public boolean handleMessage(final Message message) {
        return switch (message.getType()) {
            case EPFD_SUSPECT -> handleEpfdSuspect(message);
            case EPFD_RESTORE -> handleEpfdRestore(message);
            default -> false;
        };
    }

    private boolean handleEpfdSuspect(final Message message) {
        suspected.add(message.getEpfdSuspect().getProcess());
        updateLeader(message);
        return true;
    }

    private boolean handleEpfdRestore(final Message message) {
        suspected.remove(message.getEpfdSuspect().getProcess());
        updateLeader(message);
        return true;
    }

    private void updateLeader(Message message) {
        final Optional<ProcessId> candidateLeaderOptional = difference(sharedMemorySystem.getProcessList(), suspected).stream().max(Comparator.comparingInt(ProcessId::getRank));
        if (candidateLeaderOptional.isPresent()) {
            final ProcessId candidateLeader = candidateLeaderOptional.get();
            if (!candidateLeader.equals(leader)) {
                leader = candidateLeader;

                final EldTrust eldTrust = EldTrust.newBuilder()
                        .setProcess(leader)
                        .build();

                final Message eldTrustWrapper = Message.newBuilder()
                        .setType(Message.Type.ELD_TRUST)
                        .setEldTrust(eldTrust)
                        .setSystemId(sharedMemorySystem.getSystemId())
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId())) // TODO might have to add another one
                        .build();

                sharedMemorySystem.trigger(eldTrustWrapper);
            }
        }
    }

    private static <T> Set<T> difference(final Collection<T> first, final Collection<T> second) {
        final Set<T> firstCopy = new HashSet<>(first);
        firstCopy.removeAll(second);
        return firstCopy;
    }
}