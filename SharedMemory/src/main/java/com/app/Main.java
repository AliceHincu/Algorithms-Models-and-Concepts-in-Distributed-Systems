package com.app;

import com.app.system.NetworkManager;

public class Main {
    private static final String OWNER_ALIAS = "node";
    private static final String HUB_HOST = "localhost";
    private static final String PROCESS_HOST = "localhost";
    private static final int HUB_PORT = 5000;
    private static final int PROCESS_PORT = 5010;

    public static void main(String[] args) {
        for (int index = 1; index <= 3; index++) {
            new NetworkManager(OWNER_ALIAS, index, PROCESS_HOST,PROCESS_PORT + index, HUB_HOST, HUB_PORT);
        }
    }
}