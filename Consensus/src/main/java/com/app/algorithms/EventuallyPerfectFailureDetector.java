package com.app.algorithms;

import com.app.SharedMemoryProtobuf.*;
import com.app.system.SharedMemorySystem;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EventuallyPerfectFailureDetector implements Algorithm {

    public static final String NAME = "epfd";

    private static final int DELTA = 1000;

    private final Set<ProcessId> alive = new HashSet<>();
    private final Set<ProcessId> suspected = new HashSet<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private int delay = DELTA;

    private String abstractionId;
    private SharedMemorySystem sharedMemorySystem;

    public EventuallyPerfectFailureDetector(final String abstractionId, final SharedMemorySystem sharedMemorySystem) {
        this.sharedMemorySystem = sharedMemorySystem;
//        distributedSystem.registerAbstraction(new PerfectLink(AbstractionUtils.buildChildAbstractionId(abstractionId, PerfectLink.NAME), distributedSystem));
        alive.addAll(sharedMemorySystem.getProcessList());
        this.abstractionId = abstractionId;
        startTimer();
    }

    private void startTimer() {
        scheduledExecutorService.schedule(() -> {
            final EpfdTimeout epfdTimeout = EpfdTimeout.newBuilder().build();

            final Message epfdTimeoutWrapper = Message.newBuilder()
                .setType(Message.Type.EPFD_TIMEOUT)
                .setEpfdTimeout(epfdTimeout)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

            sharedMemorySystem.trigger(epfdTimeoutWrapper);
        }, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean handleMessage(Message message) {
        return switch (message.getType()) {
            case EPFD_TIMEOUT -> handleEpfdTimeout();
            case PL_DELIVER -> handlePlDeliver(message);
            default -> false;
        };
    }

    private boolean handlePlDeliver(final Message message) {
        return switch (message.getPlDeliver().getMessage().getType()) {
            case EPFD_INTERNAL_HEARTBEAT_REQUEST -> handleEpfdInternalHeartbeatRequest(message);
            case EPFD_INTERNAL_HEARTBEAT_REPLY -> handleEpfdInternalHeartbeatReply(message);
            default -> false;
        };
    }

    private boolean handleEpfdTimeout() {
        if (!intersection(alive, suspected).isEmpty()) {
            delay += DELTA;
        }

        for (final ProcessId processId : sharedMemorySystem.getProcessList()) {
            if (!alive.contains(processId) && !suspected.contains(processId)) {
                suspected.add(processId);

                final EpfdSuspect epfdSuspect = EpfdSuspect.newBuilder()
                    .setProcess(processId)
                    .build();

                final Message epfdSuspectWrapper = Message.newBuilder()
                    .setType(Message.Type.EPFD_SUSPECT)
                    .setEpfdSuspect(epfdSuspect)
                    .setSystemId(sharedMemorySystem.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(Utils.removeLastPartFromAbstractionId(abstractionId)) //TODO remove last part of abstraction id if it doesn't work
                    .build();

                sharedMemorySystem.trigger(epfdSuspectWrapper);
            } else if (alive.contains(processId) && suspected.contains(processId)) {
                suspected.remove(processId);

                final EpfdRestore epfdRestore = EpfdRestore.newBuilder()
                    .setProcess(processId)
                    .build();

                final Message epfdRestoreWrapper = Message.newBuilder()
                    .setType(Message.Type.EPFD_RESTORE)
                    .setEpfdRestore(epfdRestore)
                    .setSystemId(sharedMemorySystem.getSystemId())
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(Utils.removeLastPartFromAbstractionId(abstractionId))
                    .build();

                sharedMemorySystem.trigger(epfdRestoreWrapper);
            }

            final EpfdInternalHeartbeatRequest epfdInternalHeartbeatRequest = EpfdInternalHeartbeatRequest.newBuilder()
                .build();

            final Message epfdInternalHeartbeatRequestWrapper = Message.newBuilder()
                .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                .setEpfdInternalHeartbeatRequest(epfdInternalHeartbeatRequest)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId)
                .build();

            final PlSend plSend = PlSend.newBuilder()
                .setMessage(epfdInternalHeartbeatRequestWrapper)
                .setDestination(processId)
                .build();

            final Message plSendWrapper = Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(abstractionId)
                .setToAbstractionId(abstractionId + ".pl")
                .build();

            sharedMemorySystem.trigger(plSendWrapper);
        }

        alive.clear();
        startTimer();

        return true;
    }

    private boolean handleEpfdInternalHeartbeatRequest(final Message message) {
        final EpfdInternalHeartbeatReply epfdInternalHeartbeatReply = EpfdInternalHeartbeatReply.newBuilder().build();

        final Message epfdInternalHeartbeatReplyWrapper = Message.newBuilder()
            .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
            .setEpfdInternalHeartbeatReply(epfdInternalHeartbeatReply)
            .setSystemId(sharedMemorySystem.getSystemId())
            .setFromAbstractionId(abstractionId)
            .setToAbstractionId(abstractionId)
            .build();

        final PlSend plSend = PlSend.newBuilder()
            .setMessage(epfdInternalHeartbeatReplyWrapper)
            .setDestination(message.getPlDeliver().getSender())
            .build();

        final Message plSendWrapper = Message.newBuilder()
            .setType(Message.Type.PL_SEND)
            .setPlSend(plSend)
            .setSystemId(sharedMemorySystem.getSystemId())
            .setFromAbstractionId(abstractionId)
            .setToAbstractionId(abstractionId + ".pl")
            .build();

        sharedMemorySystem.trigger(plSendWrapper);

        return true;
    }

    private boolean handleEpfdInternalHeartbeatReply(final Message message) {
        alive.add(message.getPlDeliver().getSender());
        return true;
    }

    private static <T> Set<T> intersection(final Collection<T> first, final Collection<T> second) {
        final Set<T> intersection = new HashSet<>(first);
        intersection.retainAll(second);
        return intersection;
    }

    @Override
    public String toString() {
        return "EventuallyPerfectFailureDetector{" +
            "abstractionId='" + abstractionId + '\'' +
            '}';
    }

}