package com.app.system;

import com.app.SharedMemoryProtobuf.Message;
import com.app.SharedMemoryProtobuf.NetworkMessage;
import com.app.SharedMemoryProtobuf.ProcessId;
import com.app.algorithms.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.app.algorithms.Utils.getBaseEpAlgAbstractId;
import static com.app.algorithms.Utils.getBaseNnarAlgNameFromAbstractId;

public class SharedMemorySystem {


    private final List<ProcessId> processList = new CopyOnWriteArrayList<>();
    private final List<Message> messageQueue = new CopyOnWriteArrayList<>();
    private final Map<String, Algorithm> algorithmMap = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private ProcessId currentProcess;

    public String getSystemId() {
        return systemId;
    }

    public ProcessId getCurrentProcess() {
        return currentProcess;
    }

    private final String hubAddress;
    private final String systemId;
    private final int hubListeningPort;
    private final int processListeningPort;

    public SharedMemorySystem(int processListeningPort, String hubAddress, int hubListeningPort, String systemId) {
        this.processListeningPort = processListeningPort;
        this.hubAddress = hubAddress;
        this.hubListeningPort = hubListeningPort;
        this.systemId = systemId;

        this.executorService.execute(this::init);
        addAlgorithm("app");
    }

    public void init() {
        while (true) {
            boolean handled = false;
            int messageIndex = 0;

            while (!messageQueue.isEmpty() && !handled && messageIndex < messageQueue.size()) {
                Message message = messageQueue.get(messageIndex);
                Algorithm alg;

                System.out.println(message);

                if ((alg = algorithmMap.get(message.getToAbstractionId())) != null) {
                    handled = alg.handleMessage(message);
                }

                if (handled) {
                    messageQueue.remove(messageIndex);
                } else if (!algorithmMap.containsKey(message.getFromAbstractionId())) {
                    if (message.getToAbstractionId().startsWith("app.nnar")) {
                        addAlgorithm(getBaseNnarAlgNameFromAbstractId(message.getToAbstractionId()));
                    }
//                    if (message.getType().equals(Message.Type.PL_DELIVER) && message.getPlDeliver().getMessage().getType().equals(Message.Type.EP_INTERNAL_READ)) {
//                        String epAbstractionId = getBaseEpAlgAbstractId(message.getToAbstractionId());
//                        int timeStamp = Integer.parseInt(epAbstractionId.split("\\[")[2].split("]")[0]);
//                        addEPAlgorithm(epAbstractionId, new EpochConsensus.EpState(timeStamp, SharedMemoryProtobuf.Value.newBuilder().setDefined(false).build()), timeStamp);
//                    }
                    messageIndex++;
                } else {
                    messageIndex++;
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void setProcessList(List<ProcessId> processesList) {
        processList.addAll(processesList);

        for (ProcessId processId : processList) {
            if (processListeningPort == processId.getPort()) {
                currentProcess = processId;
                break;
            }
        }
    }

    public List<ProcessId> getProcessList() {
        return processList;
    }

    public int getProcessListeningPort() {
        return processListeningPort;
    }

    public void trigger(Message internalMessage) {
        internalMessage = internalMessage.toBuilder().setSystemId(systemId).build();
        messageQueue.add(internalMessage);
    }

    public ProcessId identifyProcess(NetworkMessage networkMessage) {
        for (ProcessId processId : processList) {
            if (networkMessage.getSenderListeningPort() == processId.getPort()) {
                return processId;
            }
        }

        return null;
    }

    public void addAlgorithm(String abstractionId) {
        if (abstractionId.startsWith("app.nnar")) {
            addCustomAlgorithm(abstractionId);
        } else if ("app".equals(abstractionId)) {
            addApplicationAlgorithm();
        }
    }

    private void addApplicationAlgorithm() {
        algorithmMap.put("app", new Application(this));
        algorithmMap.put("app.beb", new BestEffortBroadcast(this));
        algorithmMap.put("app.pl", new PerfectLink(this));
        algorithmMap.put("app.beb.pl", new PerfectLink(this));
    }

    private void addCustomAlgorithm(String abstractionId) {
        algorithmMap.put(abstractionId, new ReadImposeWriteConsultMajority(this));
        algorithmMap.put(abstractionId + ".beb", new BestEffortBroadcast(this));
        algorithmMap.put(abstractionId + ".beb.pl", new PerfectLink(this));
        algorithmMap.put(abstractionId + ".pl", new PerfectLink(this));
    }

    public void addUCAlgorithm(String abstractionId) {
        if(!algorithmMap.containsKey(abstractionId)) {
            algorithmMap.put(abstractionId, new UniformConsensus(this, abstractionId));
            algorithmMap.put(abstractionId + ".ec", new EpochChange(this));
            algorithmMap.put(abstractionId + ".ec.pl", new PerfectLink(this));
            algorithmMap.put(abstractionId + ".ec.beb", new BestEffortBroadcast(this));
            algorithmMap.put(abstractionId + ".ec.beb.pl", new PerfectLink(this));
            algorithmMap.put(abstractionId + ".ec.eld", new EventualLeaderDetector(this));
            algorithmMap.put(abstractionId + ".ec.eld.epfd", new EventuallyPerfectFailureDetector(abstractionId + ".ec.eld.epfd", this));
            algorithmMap.put(abstractionId + ".ec.eld.epfd.pl", new PerfectLink(this));
        }
    }

    public void addEPAlgorithm(String abstractionId, EpochConsensus.EpState epState, int epochTimeStamp) {
        if(!algorithmMap.containsKey(abstractionId)) {
            algorithmMap.put(abstractionId, new EpochConsensus(this, epState, epochTimeStamp));
            algorithmMap.put(abstractionId + ".pl", new PerfectLink(this));
            algorithmMap.put(abstractionId + ".beb", new BestEffortBroadcast(this));
            algorithmMap.put(abstractionId + ".beb.pl", new PerfectLink(this));
        }
    }

}
