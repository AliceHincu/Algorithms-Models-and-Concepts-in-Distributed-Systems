package com.app.system;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.app.SharedMemoryProtobuf.*;
import static com.app.SharedMemoryProtobuf.Message.Type.*;

public class NetworkManager {
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 5000;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Map<String, SharedMemorySystem> systemIdToSystemMap = new HashMap<>();

    private final String ownerAlias;
    private final String processHost;
    private final String hubAddress;
    private final int processListeningPort;
    private final int hubListeningPort;
    private final int processIndex;

    public NetworkManager(String ownerAlias, int processIndex, String processHost, int processListeningPort, String hubAddress, int hubListeningPort) {
        this.ownerAlias = ownerAlias;
        this.processIndex = processIndex;
        this.processHost = processHost;
        this.processListeningPort = processListeningPort;
        this.hubAddress = hubAddress;
        this.hubListeningPort = hubListeningPort;

        start();
        sendAppRegistration();
    }

    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(processListeningPort);

            executorService.execute(() -> {
                while (true) {
                    try {
                        Socket messageSocket = serverSocket.accept();
                        DataInputStream dataInputStream = new DataInputStream(messageSocket.getInputStream());
                        int length = dataInputStream.readInt();

                        if (length > 0) {
                            byte[] messageBytes = new byte[length];
                            dataInputStream.readFully(messageBytes, 0, messageBytes.length);
                            Message message = Message.parseFrom(messageBytes);
                            placeMessageInQueue(message);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void placeMessageInQueue(Message receivedMessage) {
        NetworkMessage networkMessage = receivedMessage.getNetworkMessage();
        Message internalMessage = networkMessage.getMessage();
        String systemId = receivedMessage.getSystemId();

        if (internalMessage.getType().equals(PROC_DESTROY_SYSTEM)) {
            systemIdToSystemMap.remove(systemId);
        } else if (internalMessage.getType().equals(PROC_INITIALIZE_SYSTEM)) {
            SharedMemorySystem sharedMemorySystem = new SharedMemorySystem(processListeningPort, hubAddress, hubListeningPort, systemId);

            systemIdToSystemMap.put(systemId, sharedMemorySystem);
            sharedMemorySystem.setProcessList(internalMessage.getProcInitializeSystem().getProcessesList());
        } else if (internalMessage.getType().equals(APP_BROADCAST) || internalMessage.getType().equals(APP_VALUE) ||
                internalMessage.getType().equals(APP_WRITE) || internalMessage.getType().equals(APP_READ) || internalMessage.getType().equals(APP_PROPOSE)) {
            SharedMemorySystem sharedMemorySystem = systemIdToSystemMap.get(systemId);

            sharedMemorySystem.trigger(internalMessage);
        } else {
            SharedMemorySystem sharedMemorySystem = systemIdToSystemMap.get(systemId);

            if (sharedMemorySystem != null) {
                ProcessId sender = sharedMemorySystem.identifyProcess(networkMessage);

                if (sender != null) {
                    Message plDeliver = Message.newBuilder()
                            .setType(PL_DELIVER)
                            .setFromAbstractionId(receivedMessage.getFromAbstractionId())
                            .setToAbstractionId(receivedMessage.getToAbstractionId())
                            .setPlDeliver(
                                    PlDeliver.newBuilder()
                                            .setMessage(internalMessage)
                                            .setSender(sender)
                                            .build())
                            .build();

                    sharedMemorySystem.trigger(plDeliver);
                }
            }
        }
    }

    private void sendAppRegistration() {
        Message appRegistration = Message.newBuilder()
                .setType(PROC_REGISTRATION)
                .setProcRegistration(
                        ProcRegistration.newBuilder()
                                .setOwner(ownerAlias)
                                .setIndex(processIndex)
                                .build())
                .build();
        NetworkManager.sendMessage(appRegistration, hubAddress, hubListeningPort, processListeningPort);
    }

    public static void sendMessage(Message message, String destinationIP, int destinationPort, int nodePort) {
        try {
            if (destinationIP.isBlank() && destinationPort == 0) {
                destinationIP = DEFAULT_HOST;
                destinationPort = DEFAULT_PORT;
            }

            Socket socket = new Socket(destinationIP, destinationPort);

            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(sentMessageToBytes(message, nodePort));
            outputStream.flush();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static byte[] sentMessageToBytes(Message message, int nodePort) {
        Message sentMessage = Message.newBuilder()
                .setType(NETWORK_MESSAGE)
                .setNetworkMessage(
                        NetworkMessage.newBuilder()
                                .setMessage(PL_SEND.equals(message.getType()) ? message.getPlSend().getMessage() : message)
                                .setSenderHost(PL_SEND.equals(message.getType()) ? message.getPlSend().getDestination().getHost() : DEFAULT_HOST)
                                .setSenderListeningPort(nodePort)
                                .build())
                .setFromAbstractionId(message.getToAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .setSystemId(message.getSystemId())
                .build();
        byte[] messageBytes = sentMessage.toByteArray();

        return ByteBuffer.allocate(Integer.BYTES + messageBytes.length)
                .putInt(messageBytes.length).put(messageBytes).array();
    }

}
