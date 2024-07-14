package com.app.algorithms;

import com.app.SharedMemoryProtobuf.*;
import com.app.system.SharedMemorySystem;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class EpochConsensus implements Algorithm {

    public static final String NAME = "ep";

    private EpState epState;
    private final int epochTimestamp;
    private Value tempValue = Value.newBuilder().setDefined(false).build();
    private final Map<ProcessId, EpState> states = new HashMap<>();
    private int accepted = 0;

    private SharedMemorySystem sharedMemorySystem;
    private boolean halted = false;

    public EpochConsensus(final SharedMemorySystem sharedMemorySystem, final EpState epState, final int epochTimestamp) {
//        distributedSystem.registerAbstraction(new PerfectLink(AbstractionUtils.buildChildAbstractionId(abstractionId, PerfectLink.NAME), distributedSystem));
//        distributedSystem.registerAbstraction(new BestEffortBroadcast(AbstractionUtils.buildChildAbstractionId(abstractionId, BestEffortBroadcast.NAME), distributedSystem));
        this.epochTimestamp = epochTimestamp;
        this.epState = epState;
        this.sharedMemorySystem = sharedMemorySystem;
    }

    @Override
    public boolean handleMessage(final Message message) {
        if (halted) {
            return false;
        }

        return switch (message.getType()) {
            case EP_PROPOSE -> handleEpPropose(message);
            case EP_ABORT -> handleEpAbort(message);
            case BEB_DELIVER -> handleBebDeliver(message);
            case PL_DELIVER -> handlePlDeliver(message);
            default -> false;
        };
    }

    private boolean handleBebDeliver(final Message message) {
        return switch (message.getBebDeliver().getMessage().getType()) {
            case EP_INTERNAL_READ -> handleEpInternalRead(message);
            case EP_INTERNAL_WRITE -> handleEpInternalWrite(message);
            case EP_INTERNAL_DECIDED -> handleEpInternalDecided(message);
            default -> false;
        };
    }

    private boolean handlePlDeliver(final Message message) {
        return switch (message.getPlDeliver().getMessage().getType()) {
            case EP_INTERNAL_STATE -> handleEpInternalState(message);
            case EP_INTERNAL_ACCEPT -> handleEpInternalAccept(message);
            default -> false;
        };
    }

    private boolean handleEpPropose(final Message message) {
        tempValue = message.getEpPropose().getValue();

        final EpInternalRead epInternalRead = EpInternalRead.newBuilder().build();

        final Message epInternalReadWrapper = Message.newBuilder()
                .setType(Message.Type.EP_INTERNAL_READ)
                .setEpInternalRead(epInternalRead)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .build();

        final BebBroadcast bebBroadcast = BebBroadcast.newBuilder()
                .setMessage(epInternalReadWrapper)
                .build();

        final Message bebBroadcastWrapper = Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId() + ".beb")
                .build();

        sharedMemorySystem.trigger(bebBroadcastWrapper);

        return true;
    }

    private boolean handleEpInternalRead(final Message message) {
        final EpInternalState epInternalState = EpInternalState.newBuilder()
                .setValueTimestamp(epState.valueTimestamp)
                .setValue(epState.value)
                .build();

        final Message epInternalStateWrapper = Message.newBuilder()
                .setType(Message.Type.EP_INTERNAL_STATE)
                .setEpInternalState(epInternalState)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .build();

        final PlSend plSend = PlSend.newBuilder()
                .setMessage(epInternalStateWrapper)
                .setDestination(message.getBebDeliver().getSender())
                .build();

        final Message plSendWrapper = Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId( message.getToAbstractionId() + ".pl")
                .build();

        sharedMemorySystem.trigger(plSendWrapper);

        return true;
    }

    private boolean handleEpInternalWrite(final Message message) {
        final BebDeliver bebDeliver = message.getBebDeliver();
        final EpInternalWrite epInternalWrite = bebDeliver.getMessage().getEpInternalWrite();

        epState = new EpState(epochTimestamp, epInternalWrite.getValue());

        final EpInternalAccept epInternalAccept = EpInternalAccept.newBuilder()
                .build();

        final Message epInternalAcceptWrapper = Message.newBuilder()
                .setType(Message.Type.EP_INTERNAL_ACCEPT)
                .setEpInternalAccept(epInternalAccept)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .build();

        final PlSend plSend = PlSend.newBuilder()
                .setMessage(epInternalAcceptWrapper)
                .setDestination(bebDeliver.getSender())
                .build();

        final Message plSendWrapper = Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId() +  ".pl")
                .build();

        sharedMemorySystem.trigger(plSendWrapper);

        return true;
    }

    private boolean handleEpInternalDecided(final Message message) {
        final BebDeliver bebDeliver = message.getBebDeliver();

        final EpDecide epDecide = EpDecide.newBuilder()
                .setEts(epochTimestamp)
                .setValue(bebDeliver.getMessage().getEpInternalDecided().getValue())
                .build();

        final Message epDecideWrapper = Message.newBuilder()
                .setType(Message.Type.EP_DECIDE)
                .setEpDecide(epDecide)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId()))
                .build();

        sharedMemorySystem.trigger(epDecideWrapper);

        return true;
    }

    private boolean handleEpInternalState(final Message message) {
        final PlDeliver plDeliver = message.getPlDeliver();
        final EpInternalState epInternalState = plDeliver.getMessage().getEpInternalState();

        states.put(plDeliver.getSender(), new EpState(epInternalState.getValueTimestamp(), epInternalState.getValue()));

        if (states.size() > sharedMemorySystem.getProcessList().size() / 2) {
            final EpState highestEpState = findHighestEpState();
            if (highestEpState != null && highestEpState.value.getDefined()) {
                tempValue = highestEpState.value;
            }

            states.clear();

            final EpInternalWrite epInternalWrite = EpInternalWrite.newBuilder()
                    .setValue(tempValue)
                    .build();

            final Message epInternalWriteWrapper = Message.newBuilder()
                    .setType(Message.Type.EP_INTERNAL_WRITE)
                    .setEpInternalWrite(epInternalWrite)
                    .setSystemId(sharedMemorySystem.getSystemId())
                    .setFromAbstractionId(message.getToAbstractionId())
                    .setToAbstractionId(message.getToAbstractionId())
                    .build();

            final BebBroadcast bebBroadcast = BebBroadcast.newBuilder()
                    .setMessage(epInternalWriteWrapper)
                    .build();

            final Message bebBroadcastWrapper = Message.newBuilder()
                    .setType(Message.Type.BEB_BROADCAST)
                    .setBebBroadcast(bebBroadcast)
                    .setSystemId(sharedMemorySystem.getSystemId())
                    .setFromAbstractionId(message.getToAbstractionId())
                    .setToAbstractionId(message.getToAbstractionId()  + ".beb"  )
                    .build();

            sharedMemorySystem.trigger(bebBroadcastWrapper);
        }

        return true;
    }

    private boolean handleEpInternalAccept(Message message) {
        ++accepted;

        if (accepted > sharedMemorySystem.getProcessList().size() / 2) {
            accepted = 0;

            final EpInternalDecided epInternalDecided = EpInternalDecided.newBuilder()
                    .setValue(tempValue)
                    .build();

            final Message epInternalDecidedWrapper = Message.newBuilder()
                    .setType(Message.Type.EP_INTERNAL_DECIDED)
                    .setEpInternalDecided(epInternalDecided)
                    .setSystemId(sharedMemorySystem.getSystemId())
                    .setFromAbstractionId(message.getToAbstractionId())
                    .setToAbstractionId(message.getToAbstractionId())
                    .build();

            final BebBroadcast bebBroadcast = BebBroadcast.newBuilder()
                    .setMessage(epInternalDecidedWrapper)
                    .build();

            final Message bebBroadcastWrapper = Message.newBuilder()
                    .setType(Message.Type.BEB_BROADCAST)
                    .setBebBroadcast(bebBroadcast)
                    .setSystemId(sharedMemorySystem.getSystemId())
                    .setFromAbstractionId(message.getToAbstractionId())
                    .setToAbstractionId(message.getToAbstractionId() + ".beb")
                    .build();

            sharedMemorySystem.trigger(bebBroadcastWrapper);
        }

        return true;
    }

    private boolean handleEpAbort(Message message) {
        final EpAborted epAborted = EpAborted.newBuilder()
                .setEts(epochTimestamp)
                .setValueTimestamp(epState.valueTimestamp)
                .setValue(epState.value)
                .build();

        final Message epAbortedWrapper = Message.newBuilder()
                .setType(Message.Type.EP_ABORTED)
                .setEpAborted(epAborted)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId()))
                .build();

        sharedMemorySystem.trigger(epAbortedWrapper);

        halted = true;

        return true;
    }

    private EpState findHighestEpState() {
        return states.values().stream().max(Comparator.comparingInt(s -> s.valueTimestamp)).orElse(null);
    }

    public static class EpState {

        private final int valueTimestamp;
        private final Value value;

        public EpState(final int valueTimestamp, final Value value) {
            this.valueTimestamp = valueTimestamp;
            this.value = value;
        }

    }

}