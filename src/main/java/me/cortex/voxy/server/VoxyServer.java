package me.cortex.voxy.server;

import me.cortex.voxy.common.Logger;
import me.cortex.voxy.commonImpl.VoxyCommon;
import net.minecraft.server.MinecraftServer;

public class VoxyServer {
    private static boolean initialized = false;

    public static synchronized void initVoxyServer(MinecraftServer server) {
        if (initialized) {
            return;
        }
        Logger.info("Initializing NeoVoxy Server instance for world storage");
        VoxyCommon.setInstanceFactory(() -> new VoxyServerInstance(server));
        if (VoxyCommon.getInstance() != null) {
            VoxyCommon.shutdownInstance();
        }
        VoxyCommon.createInstance();
        initialized = true;
    }

    public static synchronized void onServerStopping() {
        if (initialized) {
            Logger.info("Stopping NeoVoxy Server instance");
            VoxyCommon.shutdownInstance();
            initialized = false;
        }
    }
}
