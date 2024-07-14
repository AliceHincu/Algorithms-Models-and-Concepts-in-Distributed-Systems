package com.app.algorithms;

import com.app.SharedMemoryProtobuf;
import com.app.SharedMemoryProtobuf.*;
import com.app.system.SharedMemorySystem;

import java.util.HashMap;
import java.util.Map;

import static com.app.SharedMemoryProtobuf.Message.Type.*;


public class ReadImposeWriteConsultMajority implements Algorithm {
    private int timeStamp;
    private int writerRank;
    private int readId;
    private int acks;
    private Value val;
    private Value writeVal;
    private SharedMemoryProtobuf.Value readVal;
    private boolean reading;
    private final Map<Integer, NnarInternalValue> readList = new HashMap<>();
    private final SharedMemorySystem sharedMemorySystem;

    public ReadImposeWriteConsultMajority(SharedMemorySystem sharedMemorySystem) {
        this.sharedMemorySystem = sharedMemorySystem;
        timeStamp = writerRank = acks = readId = 0;
        val = writeVal = readVal = Utils.getUndefinedValue();
        reading = false;
    }

    @Override
    public boolean handleMessage(Message message) {
        switch (message.getType()) {
            case NNAR_READ -> {
                onNnarRead(message);
                return true;
            }
            case NNAR_WRITE -> {
                onNnarWrite(message);
                return true;
            }
            case BEB_DELIVER -> {
                switch (message.getBebDeliver().getMessage().getType()) {
                    case NNAR_INTERNAL_READ -> onBebDeliverContainingNnarInternalRead(message);
                    case NNAR_INTERNAL_WRITE -> onBebDeliverContainingNnarInternalWrite(message);
                }

                return true;
            }
            case PL_DELIVER -> {
                PlDeliver plDeliver = message.getPlDeliver();

                switch (plDeliver.getMessage().getType()) {
                    case NNAR_INTERNAL_VALUE:
                        if (plDeliver.getMessage().getNnarInternalValue().getReadId() == readId) {
                            onPlDeliverContainingNnarInternalValue(message);
                        } else {
                            return false;
                        }

                        break;
                    case NNAR_INTERNAL_ACK:
                        if (plDeliver.getMessage().getNnarInternalAck().getReadId() == readId) {
                            onPlDeliverContainingNnarInternalAck(message);
                        } else {
                            return false;
                        }

                        break;
                }

                return true;
            }
        }

        return false;
    }

    private void onNnarRead(Message message) {
        readId++;
        acks = 0;
        reading = true;

        readList.clear();
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".beb")
                        .setType(BEB_BROADCAST)
                        .setBebBroadcast(
                                BebBroadcast.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(message.getToAbstractionId())
                                                        .setToAbstractionId(message.getToAbstractionId())
                                                        .setType(NNAR_INTERNAL_READ)
                                                        .setNnarInternalRead(
                                                                NnarInternalRead.newBuilder()
                                                                        .setReadId(readId)
                                                                        .build())
                                                        .build())
                                        .build())
                        .build()
        );
    }

    private void onNnarWrite(Message message) {
        readId++;
        writeVal = message.getNnarWrite().getValue();
        acks = 0;
        readList.clear();

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".beb")
                        .setType(BEB_BROADCAST)
                        .setBebBroadcast(
                                BebBroadcast.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(message.getToAbstractionId())
                                                        .setToAbstractionId(message.getToAbstractionId())
                                                        .setType(NNAR_INTERNAL_READ)
                                                        .setNnarInternalRead(
                                                                NnarInternalRead.newBuilder()
                                                                        .setReadId(readId)
                                                                        .build())
                                                        .build())
                                        .build())
                        .build()
        );
    }

    private void onBebDeliverContainingNnarInternalRead(Message message) {
        BebDeliver bebDeliver = message.getBebDeliver();
        int readId = bebDeliver.getMessage().getNnarInternalRead().getReadId();

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".pl")
                        .setType(PL_SEND)
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setDestination(bebDeliver.getSender())
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(message.getToAbstractionId())
                                                        .setToAbstractionId(message.getToAbstractionId())
                                                        .setType(NNAR_INTERNAL_VALUE)
                                                        .setNnarInternalValue(
                                                                NnarInternalValue.newBuilder()
                                                                        .setReadId(readId)
                                                                        .setTimestamp(timeStamp)
                                                                        .setWriterRank(writerRank)
                                                                        .setValue(val)
                                                                        .build())
                                                        .build())
                                        .build()
                        )
                        .build()
        );
    }

    private void onBebDeliverContainingNnarInternalWrite(Message message) {
        BebDeliver bebDeliver = message.getBebDeliver();
        NnarInternalWrite nnarInternalWrite = bebDeliver.getMessage().getNnarInternalWrite();
        int readId = nnarInternalWrite.getReadId();
        int newTimeStamp = nnarInternalWrite.getTimestamp();
        int newWriterRank = nnarInternalWrite.getWriterRank();
        Value newVal = nnarInternalWrite.getValue();

        if (newTimeStamp > timeStamp || (newTimeStamp == timeStamp && newWriterRank > writerRank)) {
            timeStamp = newTimeStamp;
            writerRank = newWriterRank;
            val = newVal;
        }

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".pl")
                        .setType(PL_SEND)
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setDestination(bebDeliver.getSender())
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(message.getToAbstractionId())
                                                        .setToAbstractionId(message.getToAbstractionId())
                                                        .setType(NNAR_INTERNAL_ACK)
                                                        .setNnarInternalAck(
                                                                NnarInternalAck.newBuilder()
                                                                        .setReadId(readId)
                                                                        .build())
                                                        .build()
                                        )
                                        .build()
                        )
                        .build()
        );
    }

    private void onPlDeliverContainingNnarInternalValue(Message message) {
        PlDeliver plDeliver = message.getPlDeliver();
        NnarInternalValue nnarInternalValue = plDeliver.getMessage().getNnarInternalValue();
        int readId = nnarInternalValue.getReadId();
        int newTimeStamp = nnarInternalValue.getTimestamp();
        int newWriterRank = nnarInternalValue.getWriterRank();
        Value newVal = nnarInternalValue.getValue();

        int rank = plDeliver.getSender().getRank();
        int senderPort = plDeliver.getSender().getPort();

        readList.put(senderPort, NnarInternalValue.newBuilder()
                .setReadId(readId)
                .setTimestamp(newTimeStamp)
                .setWriterRank(newWriterRank)
                .setValue(newVal)
                .build());

        if (readList.size() > sharedMemorySystem.getProcessList().size() / 2) {
            int maxTimestamp = -1;
            int maxWriterRank = -1;

            for (NnarInternalValue internalValue : readList.values()) {
                if (internalValue.getTimestamp() > maxTimestamp ||
                        (internalValue.getTimestamp() == maxTimestamp && internalValue.getWriterRank() > maxWriterRank) ||
                        (internalValue.getTimestamp() == maxTimestamp && internalValue.getWriterRank() == maxWriterRank && internalValue.getValue().getV() > readVal.getV())) {
                    readVal = internalValue.getValue();
                    maxTimestamp = internalValue.getTimestamp();
                    maxWriterRank = internalValue.getWriterRank();
                }
            }

            readList.clear();

            if (reading) {
                sharedMemorySystem.trigger(
                        Message.newBuilder()
                                .setFromAbstractionId(message.getToAbstractionId())
                                .setToAbstractionId(message.getToAbstractionId() + ".beb")
                                .setType(BEB_BROADCAST)
                                .setBebBroadcast(
                                        BebBroadcast.newBuilder()
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setFromAbstractionId(message.getToAbstractionId())
                                                                .setToAbstractionId(message.getToAbstractionId())
                                                                .setType(NNAR_INTERNAL_WRITE)
                                                                .setNnarInternalWrite(
                                                                        NnarInternalWrite.newBuilder()
                                                                                .setReadId(this.readId)
                                                                                .setTimestamp(maxTimestamp)
                                                                                .setWriterRank(maxWriterRank)
                                                                                .setValue(readVal)
                                                                                .build())
                                                                .build())
                                                .build())
                                .build());
            } else {
                sharedMemorySystem.trigger(
                        Message.newBuilder()
                                .setFromAbstractionId(message.getToAbstractionId())
                                .setToAbstractionId(message.getToAbstractionId() + ".beb")
                                .setType(BEB_BROADCAST)
                                .setBebBroadcast(
                                        BebBroadcast.newBuilder()
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setFromAbstractionId(message.getToAbstractionId())
                                                                .setToAbstractionId(message.getToAbstractionId())
                                                                .setType(NNAR_INTERNAL_WRITE)
                                                                .setNnarInternalWrite(
                                                                        NnarInternalWrite.newBuilder()
                                                                                .setReadId(this.readId)
                                                                                .setTimestamp(maxTimestamp + 1)
                                                                                .setWriterRank(rank)
                                                                                .setValue(writeVal)
                                                                                .build())
                                                                .build()
                                                )
                                                .build()
                                )
                                .build()
                );
            }
        }
    }

    private void onPlDeliverContainingNnarInternalAck(Message message) {
        acks++;

        if (acks > sharedMemorySystem.getProcessList().size() / 2) {
            acks = 0;

            if (reading) {
                reading = false;

                sharedMemorySystem.trigger(
                        Message.newBuilder()
                                .setFromAbstractionId(message.getToAbstractionId())
                                .setToAbstractionId("app")
                                .setType(NNAR_READ_RETURN)
                                .setNnarReadReturn(
                                        NnarReadReturn.newBuilder()
                                                .setValue(readVal)
                                                .build())
                                .build()
                );
            } else {
                sharedMemorySystem.trigger(
                        Message.newBuilder()
                                .setFromAbstractionId(message.getToAbstractionId())
                                .setToAbstractionId("app")
                                .setType(NNAR_WRITE_RETURN)
                                .setNnarWriteReturn(
                                        NnarWriteReturn.newBuilder()
                                                .build())
                                .build()
                );
            }
        }
    }
}
