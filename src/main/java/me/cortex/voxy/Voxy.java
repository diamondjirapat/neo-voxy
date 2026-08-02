package me.cortex.voxy;

import me.cortex.voxy.common.network.VoxyNetworkHandler;
import me.cortex.voxy.commonImpl.VoxyCommon;
import net.neoforged.api.distmarker.Dist;
import net.neoforged.bus.api.IEventBus;
import net.neoforged.fml.ModContainer;
import net.neoforged.fml.common.Mod;
import net.neoforged.fml.event.lifecycle.FMLClientSetupEvent;
import net.neoforged.fml.loading.FMLLoader;
import net.neoforged.neoforge.client.event.RegisterClientCommandsEvent;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.server.ServerAboutToStartEvent;
import net.neoforged.neoforge.event.server.ServerStoppingEvent;

@Mod(Voxy.MODID)
public class Voxy {
    public static final String MODID = "neovoxy";

    public Voxy(IEventBus modEventBus, ModContainer modContainer) {
        // Init common environment
        VoxyCommon.cleanInit(modContainer.getModInfo().getVersion().toString(),
                FMLLoader.getDist() == Dist.DEDICATED_SERVER);

        // Register network handler (both client and server)
        modEventBus.addListener(VoxyNetworkHandler::register);

        // Register client setup event only on client dist
        if (FMLLoader.getDist() == Dist.CLIENT) {
            ClientInitializer.register(modEventBus);
        } else {
            ServerInitializer.register();
        }
    }

    private static class ServerInitializer {
        private static void register() {
            NeoForge.EVENT_BUS.addListener(ServerInitializer::onServerAboutToStart);
            NeoForge.EVENT_BUS.addListener(ServerInitializer::onServerStopping);
        }

        private static void onServerAboutToStart(ServerAboutToStartEvent event) {
            me.cortex.voxy.server.VoxyServer.initVoxyServer(event.getServer());
        }

        private static void onServerStopping(ServerStoppingEvent event) {
            me.cortex.voxy.server.VoxyServer.onServerStopping();
        }
    }

    private static class ClientInitializer {
        private static void register(IEventBus modEventBus) {
            modEventBus.addListener(ClientInitializer::onClientSetup);
            NeoForge.EVENT_BUS.addListener(ClientInitializer::onRegisterClientCommands);
        }

        private static void onClientSetup(FMLClientSetupEvent event) {
            event.enqueueWork(() -> {
                me.cortex.voxy.client.VoxyClient.initVoxyClient();
                me.cortex.voxy.client.VoxyClient.onClientSetup();
            });
        }

        private static void onRegisterClientCommands(RegisterClientCommandsEvent event) {
            if (VoxyCommon.isAvailable()) {
                event.getDispatcher().register(me.cortex.voxy.client.VoxyCommands.register());
            }
        }
    }
}
