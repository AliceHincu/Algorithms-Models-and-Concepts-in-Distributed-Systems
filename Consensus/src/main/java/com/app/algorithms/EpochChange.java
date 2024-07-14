package com.app.algorithms;

import com.app.system.SharedMemorySystem;

import java.util.Comparator;

import static com.app.SharedMemoryProtobuf.*;

public class EpochChange implements Algorithm {

    public static final String NAME = "ec";

    private ProcessId trusted;
    private int lastTimestamp = 0;
    private int timestamp;

    private SharedMemorySystem sharedMemorySystem;

    public EpochChange(final SharedMemorySystem sharedMemorySystem) {
//        distributedSystem.registerAbstraction(new PerfectLink(AbstractionUtils.buildChildAbstractionId(abstractionId, PerfectLink.NAME), distributedSystem));
//        distributedSystem.registerAbstraction(new BestEffortBroadcast(AbstractionUtils.buildChildAbstractionId(abstractionId, BestEffortBroadcast.NAME), distributedSystem));
//        distributedSystem.registerAbstraction(new EventualLeaderDetector(AbstractionUtils.buildChildAbstractionId(abstractionId, EventualLeaderDetector.NAME), distributedSystem));
        trusted = sharedMemorySystem.getProcessList().stream().max(Comparator.comparingInt(ProcessId::getRank)).orElseThrow();
        timestamp = sharedMemorySystem.getCurrentProcess().getRank();
        this.sharedMemorySystem = sharedMemorySystem;
    }

    @Override
    public boolean handleMessage(final Message message) {
        return switch (message.getType()) {
            case ELD_TRUST -> handleEldTrust(message);
            case BEB_DELIVER -> handleBebDeliver(message);
            case PL_DELIVER -> handlePlDeliver(message);
            default -> false;
        };
    }

    private boolean handleBebDeliver(final Message message) {
        return switch (message.getBebDeliver().getMessage().getType()) {
            case EC_INTERNAL_NEW_EPOCH -> handleEcInternalNewEpoch(message);
            default -> false;
        };
    }

    private boolean handlePlDeliver(final Message message) {
        return switch (message.getPlDeliver().getMessage().getType()) {
            case EC_INTERNAL_NACK -> handleEcInternalNack(message);
            default -> false;
        };
    }

    private boolean handleEldTrust(final Message message) {
        trusted = message.getEldTrust().getProcess();
        return triggerNewEpoch(message);
    }

    private boolean handleEcInternalNewEpoch(final Message message) {
        final BebDeliver bebDeliver = message.getBebDeliver();
        final ProcessId bebDeliverSender = bebDeliver.getSender();
        final int newTimestamp = bebDeliver.getMessage().getEcInternalNewEpoch().getTimestamp();

        if (bebDeliverSender.equals(trusted) && newTimestamp > lastTimestamp) {
            lastTimestamp = newTimestamp;

            final EcStartEpoch ecStartEpoch = EcStartEpoch.newBuilder()
                .setNewLeader(bebDeliverSender)
                .setNewTimestamp(newTimestamp)
                .build();

            final Message ecStartEpochWrapper = Message.newBuilder()
                .setType(Message.Type.EC_START_EPOCH)
                .setEcStartEpoch(ecStartEpoch)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId()))
                .build();

            sharedMemorySystem.trigger(ecStartEpochWrapper);
        } else {
            final EcInternalNack ecInternalNack = EcInternalNack.newBuilder()
                .build();

            final Message ecInternalNackWrapper = Message.newBuilder()
                .setType(Message.Type.EC_INTERNAL_NACK)
                .setEcInternalNack(ecInternalNack)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .build();

            final PlSend plSend = PlSend.newBuilder()
                .setMessage(ecInternalNackWrapper)
                .setDestination(bebDeliverSender)
                .build();

            final Message plSendWrapper = Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(plSend)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId() + ".pl")
                .build();

            sharedMemorySystem.trigger(plSendWrapper);
        }

        return true;
    }

    private boolean handleEcInternalNack(Message message) {
        return triggerNewEpoch(message);
    }

    private boolean triggerNewEpoch(Message message) {
        if (trusted.equals(sharedMemorySystem.getCurrentProcess())) {
            timestamp += sharedMemorySystem.getProcessList().size();

            final EcInternalNewEpoch ecInternalNewEpoch = EcInternalNewEpoch.newBuilder()
                .setTimestamp(timestamp)
                .build();

            final Message ecInternalNewEpochWrapper = Message.newBuilder()
                .setType(Message.Type.EC_INTERNAL_NEW_EPOCH)
                .setEcInternalNewEpoch(ecInternalNewEpoch)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .build();

            final BebBroadcast bebBroadcast = BebBroadcast.newBuilder()
                .setMessage(ecInternalNewEpochWrapper)
                .build();

            final Message bebBroadcastWrapper = Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(bebBroadcast)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId() +  ".beb")
                .build();

            sharedMemorySystem.trigger(bebBroadcastWrapper);
        }

        return true;
    }

}