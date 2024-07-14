package com.app.algorithms;

import com.app.SharedMemoryProtobuf.*;
import com.app.system.SharedMemorySystem;

import static com.app.SharedMemoryProtobuf.Message.Type.BEB_DELIVER;
import static com.app.SharedMemoryProtobuf.Message.Type.PL_SEND;

public class BestEffortBroadcast implements Algorithm {
    private final SharedMemorySystem sharedMemorySystem;

    public BestEffortBroadcast(SharedMemorySystem sharedMemorySystem) {
        this.sharedMemorySystem = sharedMemorySystem;
    }

    @Override
    public boolean handleMessage(Message message) {
        switch (message.getType()) {
            case BEB_BROADCAST -> {
                onBebBroadcast(message);
                return true;
            }
            case PL_DELIVER -> {
                onPlDeliver(message);
                return true;
            }
        }

        return false;
    }

    private void onBebBroadcast(Message message) {
        for (ProcessId processId : sharedMemorySystem.getProcessList()) {
            sharedMemorySystem.trigger(
                    Message.newBuilder()
                            .setType(PL_SEND)
                            .setFromAbstractionId(message.getToAbstractionId())
                            .setToAbstractionId(message.getToAbstractionId() + ".pl")
                            .setPlSend(
                                    PlSend.newBuilder()
                                            .setDestination(processId)
                                            .setMessage(message.getBebBroadcast().getMessage())
                                            .build())
                            .build());
        }
    }

    private void onPlDeliver(Message message) {
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setType(BEB_DELIVER)
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId()))
                        .setBebDeliver(
                                BebDeliver.newBuilder()
                                        .setSender(message.getPlDeliver().getSender())
                                        .setMessage(message.getPlDeliver().getMessage())
                                        .build())
                        .build());
    }
}