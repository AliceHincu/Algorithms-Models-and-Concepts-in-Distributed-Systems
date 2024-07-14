package com.app.algorithms;



import com.app.SharedMemoryProtobuf.*;
import com.app.system.SharedMemorySystem;

import java.util.Comparator;

/**
 * asigură implementarea mecanismului de consens uniform, gestionând propunerile de valori și coordonând deciziile între
 * diferite epoci și lideri ai sistemului.
 */
public class UniformConsensus implements Algorithm {

    public static final String NAME = "uc";

    private Value value = Value.newBuilder().setDefined(false).build();
    private boolean proposed = false;
    private boolean decided = false;

    private int epochTimestamp = 0; // identificator temporal pt ordonarea operatiunilor

    private ProcessId leader; // procesul desemnat sa coordoneze actiunea de propunere a valorilor si colectarea raspunsurilor de la alte procese pt a ajunge la un conses

    private int newTimestamp = 0;
    private ProcessId newLeader = null;

    private final SharedMemorySystem sharedMemorySystem;

    public UniformConsensus(SharedMemorySystem sharedMemorySystem, String abstractionId) {
        leader = sharedMemorySystem.getProcessList().stream().max(Comparator.comparingInt(ProcessId::getRank)).orElseThrow();
        final EpochConsensus.EpState epState0 = new EpochConsensus.EpState(0, Value.newBuilder().setDefined(false).build());

        sharedMemorySystem.addEPAlgorithm(abstractionId + ".ep[" + epochTimestamp + "]", epState0, epochTimestamp);

        this.sharedMemorySystem = sharedMemorySystem;
    }

    @Override
    public boolean handleMessage(final Message message) {
        return switch (message.getType()) {
            case UC_PROPOSE -> handleUcPropose(message);
            case EC_START_EPOCH -> handleEcStartEpoch(message);
            case EP_ABORTED -> handleEpAborted(message);
            case EP_DECIDE -> handleEpDecide(message);
            default -> false;
        };
    }

    /**
     * Initiere propunere pentru consens
     * @param message
     * @return
     */
    private boolean handleUcPropose(final Message message) {
        value = message.getUcPropose().getValue();
        triggerPropose(message);
        return true;
    }

    /**
     * Raspunde la notificarile de incepere a unei noi epoci, resetand starea curenta  si pregatind sistemul pentru
     * noile propuneri sub noul lider
     * @param message
     * @return
     */
    private boolean handleEcStartEpoch(final Message message) {
        final EcStartEpoch ecStartEpoch = message.getEcStartEpoch();

        newTimestamp = ecStartEpoch.getNewTimestamp();
        newLeader = ecStartEpoch.getNewLeader();

        final EpAbort epAbort = EpAbort.newBuilder()
            .build();

        final Message epAbortWrapper = Message.newBuilder()
            .setType(Message.Type.EP_ABORT)
            .setEpAbort(epAbort)
            .setSystemId(sharedMemorySystem.getSystemId())
            .setFromAbstractionId(message.getToAbstractionId())
            .setToAbstractionId(message.getToAbstractionId() + ".ep[" + epochTimestamp + "]")
            .build();

        sharedMemorySystem.trigger(epAbortWrapper);

        return true;
    }

    /**
     * Gestioneaza cazurile in care o epoca e intrerupta (se asigura ca sistemul ramane consistent si pregatit pentru a
     * incerca din nou)
     * @param message
     * @return
     */
    private boolean handleEpAborted(final Message message) {
        final EpAborted epAborted = message.getEpAborted();

        // verifica daca mesajul se refera la epoca curenta
        if (epAborted.getEts() != epochTimestamp) {
            return false;
        }

        epochTimestamp = newTimestamp;
        leader = newLeader;
        proposed = false;

        final EpochConsensus.EpState epState = new EpochConsensus.EpState(epAborted.getValueTimestamp(), epAborted.getValue());

        sharedMemorySystem.addEPAlgorithm(message.getToAbstractionId() + ".ep[" + epochTimestamp + "]", epState, epochTimestamp);

        triggerPropose(message);

        return true;
    }

    /**
     * Primeste deciziile finale privind valoarea consensului si le transmite tuturor proceselor, marcand procesul de
     * consens ca fiind complet
     * @param message
     * @return
     */
    private boolean handleEpDecide(final Message message) {
        final EpDecide epDecide = message.getEpDecide();

        // verifica daca mesajul se refera la epoca curenta
        if (epDecide.getEts() != epochTimestamp) {
            return false;
        }

        if (!decided) {
            decided = true;

            final UcDecide ucDecide = UcDecide.newBuilder()
                .setValue(epDecide.getValue())
                .build();

            final Message ucDecideWrapper = Message.newBuilder()
                .setType(Message.Type.UC_DECIDE)
                .setUcDecide(ucDecide)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(Utils.removeLastPartFromAbstractionId(message.getToAbstractionId()))
                .build();

            sharedMemorySystem.trigger(ucDecideWrapper);
        }

        return true;
    }

    /**
     * Liderul initiaza o propunere de consens daca valoarea a fost definita dar nu a fost inca propusa
     * @param message
     */
    private void triggerPropose(Message message) {
        if (leader.equals(sharedMemorySystem.getCurrentProcess()) && value.getDefined() && !proposed) {
            proposed = true;

            final EpPropose epPropose = EpPropose.newBuilder()
                .setValue(value)
                .build();

            String epochAbstractionId = message.getToAbstractionId() + ".ep[" + epochTimestamp + "]";

            final Message ucProposeWrapper = Message.newBuilder()
                .setType(Message.Type.EP_PROPOSE)
                .setEpPropose(epPropose)
                .setSystemId(sharedMemorySystem.getSystemId())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(epochAbstractionId)
                .build();

            sharedMemorySystem.trigger(ucProposeWrapper);
        }
    }

}