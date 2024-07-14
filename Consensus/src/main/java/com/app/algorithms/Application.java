package com.app.algorithms;


import com.app.SharedMemoryProtobuf.*;
import com.app.system.SharedMemorySystem;

import static com.app.SharedMemoryProtobuf.Message.Type.*;
import static com.app.algorithms.Utils.getRegisterFromNnar;

public class Application implements Algorithm {
    private final SharedMemorySystem sharedMemorySystem;

    public Application(SharedMemorySystem sharedMemorySystem) {
        this.sharedMemorySystem = sharedMemorySystem;
    }

    @Override
    public boolean handleMessage(Message message) {
        switch (message.getType()) {
            case APP_BROADCAST -> {
                onAppBroadcast(message);
                return true;
            }
            case APP_VALUE -> {
                onAppValue(message);
                return true;
            }
            case APP_WRITE -> {
                onAppWrite(message);
                return true;
            }
            case APP_READ -> {
                onAppRead(message);
                return true;
            }
            case APP_PROPOSE -> {
                handleAppPropose(message);
                return true;
            }
            case PL_DELIVER -> {
                switch (message.getPlDeliver().getMessage().getType()) {
                    case APP_BROADCAST -> {
                        onPlDeliverContainingAppBroadcast(message);
                        return true;
                    }
                    case APP_WRITE -> {
                        onPlDeliverContainingAppWrite(message);
                        return true;
                    }
                    case APP_READ -> {
                        onPlDeliverContainingAppRead(message);
                        return true;
                    }
                }

                return false;
            }
            case BEB_DELIVER -> {
                onBebDeliver(message);
                return true;
            }
            case NNAR_WRITE_RETURN -> {
                onNnarWriteReturn(message);
                return true;
            }
            case NNAR_READ_RETURN -> {
                onNnarReadReturn(message);
                return true;
            }
            case UC_DECIDE -> {
                handleUcDecide(message);
                return true;
            }
        }

        return false;
    }

    private void handleUcDecide(final Message message) {
        final UcDecide ucDecide = message.getUcDecide();

        final AppDecide appDecide = AppDecide.newBuilder()
                .setValue(ucDecide.getValue())
                .build();

        final Message appDecideWrapper = Message.newBuilder()
                .setType(Message.Type.APP_DECIDE)
                .setAppDecide(appDecide)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId("hub")
                .build();

        final PlSend plSend = PlSend.newBuilder()
                .setMessage(appDecideWrapper)
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

    private void onAppBroadcast(Message message) {
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId() + ".pl")
                        .setToAbstractionId("app")
                        .setType(PL_DELIVER)
                        .setPlDeliver(
                                PlDeliver.newBuilder()
                                        .setMessage(message)
                                        .build())
                        .build());
    }

    private void onAppValue(Message message) {
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId() + ".beb.pl")
                        .setToAbstractionId(message.getToAbstractionId() + ".beb")
                        .setType(PL_DELIVER)
                        .setPlDeliver(
                                PlDeliver.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(message.getToAbstractionId())
                                                        .setToAbstractionId(message.getToAbstractionId())
                                                        .setAppValue(message.getAppValue())
                                                        .build())
                                        .build())
                        .build());
    }

    private void onAppWrite(Message message) {
        AppWrite appWrite = message.getAppWrite();
        String abstractionId = "app.nnar[" + appWrite.getRegister() + "]";
        sharedMemorySystem.addAlgorithm(abstractionId);

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId("app.pl")
                        .setToAbstractionId("app")
                        .setType(PL_DELIVER)
                        .setPlDeliver(
                                PlDeliver.newBuilder()
                                        .setMessage(message)
                                        .build())
                        .build()
        );
    }

    private void onAppRead(Message message) {
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId("app.pl")
                        .setToAbstractionId("app")
                        .setType(PL_DELIVER)
                        .setPlDeliver(
                                PlDeliver.newBuilder()
                                        .setMessage(message)
                                        .build())
                        .build());
    }

    private boolean handleAppPropose(final Message message) {
        final AppPropose appPropose = message.getAppPropose();

        String ucAbstractionId = "app.uc[" + appPropose.getTopic() + "]";

        sharedMemorySystem.addUCAlgorithm(ucAbstractionId);

        final UcPropose ucPropose = UcPropose.newBuilder()
                .setValue(appPropose.getValue())
                .build();

        final Message ucProposeWrapper = Message.newBuilder()
                .setType(Message.Type.UC_PROPOSE)
                .setUcPropose(ucPropose)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(ucAbstractionId)
                .build();

        sharedMemorySystem.trigger(ucProposeWrapper);

        return true;
    }

    private void onPlDeliverContainingAppBroadcast(Message message) {
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".beb")
                        .setType(BEB_BROADCAST)
                        .setBebBroadcast(
                                BebBroadcast.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId("app")
                                                        .setToAbstractionId("app")
                                                        .setType(APP_VALUE)
                                                        .setAppValue(
                                                                AppValue.newBuilder()
                                                                        .setValue(message.getPlDeliver().getMessage().getAppBroadcast().getValue())
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());
    }

    private void onPlDeliverContainingAppWrite(Message message) {
        AppWrite appWrite = message.getPlDeliver().getMessage().getAppWrite();

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".nnar[" + appWrite.getRegister() + "]")
                        .setType(NNAR_WRITE)
                        .setNnarWrite(
                                NnarWrite.newBuilder()
                                        .setValue(appWrite.getValue())
                                        .build())
                        .build());
    }

    private void onPlDeliverContainingAppRead(Message message) {
        AppRead appRead = message.getPlDeliver().getMessage().getAppRead();

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".nnar[" + appRead.getRegister() + "]")
                        .setType(NNAR_READ)
                        .setNnarRead(
                                NnarRead.newBuilder()
                                        .build())
                        .build());
    }

    private void onBebDeliver(Message message) {
        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setType(PL_SEND)
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".pl")
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setDestination(message.getBebDeliver().getSender())
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId("app")
                                                        .setToAbstractionId("hub")
                                                        .setType(APP_VALUE)
                                                        .setAppValue(
                                                                AppValue.newBuilder()
                                                                        .setValue(message.getBebDeliver().getMessage().getAppValue().getValue())
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());
    }

    private void onNnarWriteReturn(Message message) {
        String register = getRegisterFromNnar(message.getFromAbstractionId());

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".pl")
                        .setType(PL_SEND)
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(message.getToAbstractionId())
                                                        .setToAbstractionId("hub")
                                                        .setType(APP_WRITE_RETURN)
                                                        .setAppWriteReturn(
                                                                AppWriteReturn.newBuilder()
                                                                        .setRegister(register)
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());
    }

    private void onNnarReadReturn(Message message) {
        NnarReadReturn nnarReadReturn = message.getNnarReadReturn();
        String register = getRegisterFromNnar(message.getFromAbstractionId());

        sharedMemorySystem.trigger(
                Message.newBuilder()
                        .setFromAbstractionId(message.getToAbstractionId())
                        .setToAbstractionId(message.getToAbstractionId() + ".pl")
                        .setType(PL_SEND)
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setFromAbstractionId(message.getToAbstractionId())
                                                        .setToAbstractionId("hub")
                                                        .setType(APP_READ_RETURN)
                                                        .setAppReadReturn(
                                                                AppReadReturn.newBuilder()
                                                                        .setRegister(register)
                                                                        .setValue(nnarReadReturn.getValue())
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());
    }
}

