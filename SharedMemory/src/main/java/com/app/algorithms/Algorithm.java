package com.app.algorithms;

import com.app.SharedMemoryProtobuf.*;

public interface Algorithm {


    boolean handleMessage(Message message);
}
