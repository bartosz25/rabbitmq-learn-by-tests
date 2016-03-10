package com.waitingforcode.util;

public final class Mapping {

    private Mapping() {
        // prevents init
    }

    public static enum BoolMaps {

        DURABLE,
        EXCLUSIVE,
        AUTO_DELETE,
        AUTO_ACK,
        MANDATORY,
        MULTIPLE,
        REQUEUE,
        GLOBAL_CHANNEL_PREFETCH,
        IF_UNUSED,
        IF_EMPTY,
        INTERNAL,
        NO_LOCAl;

        public boolean not() {
            return false;
        }

        public boolean yes() {
            return true;
        }
    }


    public static enum DeliveryModes {
        NOT_PERSISTENT(1),
        PERSISTENT(2);

        private int value;

        private DeliveryModes(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

}
