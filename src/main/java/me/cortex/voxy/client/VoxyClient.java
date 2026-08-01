package me.cortex.voxy.client;

import me.cortex.voxy.client.core.gl.Capabilities;
import me.cortex.voxy.client.core.model.bakery.BudgetBufferRenderer;
import me.cortex.voxy.client.core.rendering.util.SharedIndexBuffer;
import me.cortex.voxy.common.Logger;
import me.cortex.voxy.commonImpl.VoxyCommon;
import java.util.HashSet;

public class VoxyClient {
    private static final HashSet<String> FREX = new HashSet<>();
    private static boolean initialized = false;

    public static synchronized void initVoxyClient() {
        if (initialized) {
            return;
        }
        initialized = true;

        Capabilities.init(); // Ensure clinit is called

        boolean systemSupported = Capabilities.INSTANCE.compute && Capabilities.INSTANCE.indirectParameters;
        if (systemSupported) {
            SharedIndexBuffer.INSTANCE.id();
            BudgetBufferRenderer.init();

            VoxyCommon.setInstanceFactory(VoxyClientInstance::new);
            try {
                net.caffeinemc.mods.sodium.client.config.ConfigManager.registerConfigEntryPoint(me.cortex.voxy.client.config.VoxySodiumConfigEntryPoint::new, "neovoxy");
            } catch (Throwable t) {
                Logger.warn("Failed to register Sodium config entry point: " + t.getMessage());
            }

            if (!Capabilities.INSTANCE.subgroup) {
                Logger.warn("GPU does not support subgroup operations, expect some performance degradation");
            }
        } else {
            Logger.error("Voxy is unsupported on your system.");
        }
    }

    public static void onClientSetup() {
        // Called from Voxy mod entrypoint during client setup
        // Command registration is handled via NeoForge events in the main mod class
    }

    public static boolean isFrexActive() {
        return !FREX.isEmpty();
    }

    public static int getOcclusionDebugState() {
        return 0;
    }

    public static boolean disableSodiumChunkRender() {
        return getOcclusionDebugState() != 0;
    }
}