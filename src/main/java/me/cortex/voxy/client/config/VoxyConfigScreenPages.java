package me.cortex.voxy.client.config;

// Retained for compatibility references. Option registration for Sodium 0.8+ is handled by VoxySodiumConfigEntryPoint.
public abstract class VoxyConfigScreenPages {
    private VoxyConfigScreenPages() {
    }

    private static final int SUBDIV_IN_MAX = 100;
    private static final double SUBDIV_MIN = 28;
    private static final double SUBDIV_MAX = 256;
    private static final double SUBDIV_CONST = Math.log(SUBDIV_MAX / SUBDIV_MIN) / Math.log(2);

    public static float ln2subDiv(int in) {
        return (float) (SUBDIV_MIN * Math.pow(2, SUBDIV_CONST * ((double) in / SUBDIV_IN_MAX)));
    }

    public static int subDiv2ln(float in) {
        return (int) (((Math.log(((double) in) / SUBDIV_MIN) / Math.log(2)) / SUBDIV_CONST) * SUBDIV_IN_MAX);
    }
}
