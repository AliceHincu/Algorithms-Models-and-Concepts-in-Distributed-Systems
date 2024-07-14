package com.app.algorithms;

import com.app.SharedMemoryProtobuf.*;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    public static final Pattern nnarAlgorithmPattern = Pattern.compile("^[^.]*\\.[^.]*\\[.*\\][^.]*");
    public static final Pattern registerPattern = Pattern.compile("^[^\\[\\]]*\\[(.*)].*");

    public static String getBaseNnarAlgNameFromAbstractId(String abstractId) {
        Matcher matcher = nnarAlgorithmPattern.matcher(abstractId);

        if(matcher.find() ) {
            return matcher.group();
        }

        return "";
    }

    public static String getRegisterFromNnar(String abstractId) {
        Matcher matcher = registerPattern.matcher(abstractId);

        if(matcher.find() ) {
            return matcher.group(1);
        }

        return "";
    }

    public static Value getUndefinedValue() {
        return Value.newBuilder()
                .setDefined(false)
                .build();
    }

    public static String removeLastPartFromAbstractionId(String abstractionId) {
        String[] parts = abstractionId.split("\\.");
        parts = Arrays.copyOf(parts, parts.length - 1);

        return String.join(".", parts);
    }
}