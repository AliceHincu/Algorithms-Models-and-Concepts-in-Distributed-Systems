package com.app.algorithms;

import com.app.SharedMemoryProtobuf.*;
import com.app.system.NetworkManager;
import com.app.system.SharedMemorySystem;

import static com.app.SharedMemoryProtobuf.Message.Type.BEB_DELIVER;
import static com.app.SharedMemoryProtobuf.Message.Type.PL_DELIVER;

public class PerfectLink implements Algorithm {
    private final SharedMemorySystem sharedMemorySystem;

    public PerfectLink(SharedMemorySystem sharedMemorySystem) {
        this.sharedMemorySystem = sharedMemorySystem;
    }

    @Override
    public boolean handleMessage(Message message) {
        switch (message.getType()) {
            case PL_SEND -> {
                onPlSend(message);
                return true;
            }
            case PL_DELIVER -> {
                switch (message.getPlDeliver().getMessage().getType()) {
                    case APP_BROADCAST -> {
                        onPlDeliverContainingAppValue(message);
                        return true;
                    }
                    case EP_INTERNAL_DECIDED, EP_INTERNAL_WRITE, EP_INTERNAL_READ, EC_INTERNAL_NEW_EPOCH, NNAR_INTERNAL_READ,
                            NNAR_INTERNAL_WRITE-> {
                        onPlDeliverFromBeb(message);
                        return true;
                    }
                    case NNAR_INTERNAL_VALUE, NNAR_INTERNAL_ACK, EPFD_INTERNAL_HEARTBEAT_REQUEST, EPFD_INTERNAL_HEARTBEAT_REPLY,
                            EC_INTERNAL_NACK, EP_INTERNAL_STATE, EP_INTERNAL_ACCEPT -> {
                        onPlDeliverFromPlSend(message);
                        return true;
                    }
                }

                return false;
            }
        }

        return false;
    }

    private void onPlSend(Message message) {
        int nodePort = sharedMemorySystem.getProcessListeningPort();
        ProcessId destinationProcess = message.getPlSend().getDestination();
        NetworkManager.sendMessage(message, destinationProcess.getHost(), destinationProcess.getPort(), nodePort);
    }

    private void onPlDeliverContainingAppValue(Message message) {
        PlDeliver plDeliver = message.getPlDeliver();

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setType(BEB_DELIVER)
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId()))
                        .setBebDeliver(
                                BebDeliver.newBuilder()
                                        .setSender(plDeliver.getSender())
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(plDeliver.getMessage().getFromAbstractionId())
                                                        .setToAbstractionId("app")
                                                        .setAppValue(plDeliver.getMessage().getAppValue())
                                                        .build())
                                        .build())
                        .build());
    }

    private void onPlDeliverFromPlSend(Message message) {
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setType(PL_DELIVER)
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId()))
                        .setPlDeliver(message.getPlDeliver())
                        .build());
    }

    private void onPlDeliverFromBeb(Message message) {
        PlDeliver plDeliver = message.getPlDeliver();
        String toAbstractionIdToBeb = Utils.removeLastPartFromAbstractionId(message.getToAbstractionId());
        String toAbstractionIdToEp = Utils.removeLastPartFromAbstractionId(toAbstractionIdToBeb);

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setType(BEB_DELIVER)
                        .setFromAbstractionId(toAbstractionIdToBeb)
                        .setToAbstractionId(toAbstractionIdToEp)
                        .setBebDeliver(
                                BebDeliver.newBuilder()
                                        .setSender(plDeliver.getSender())
                                        .setMessage(plDeliver.getMessage())
                                        .build())
                        .build());
    }
}
