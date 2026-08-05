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

        // Register client setup event on client dist, and server initializer on all dists
        if (FMLLoader.getDist() == Dist.CLIENT) {
            ClientInitializer.register(modEventBus);
        }
        ServerInitializer.register();
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
            NeoForge.EVENT_BUS.addListener(ClientInitializer::onCustomizeDebugText);
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

        private static void onCustomizeDebugText(net.neoforged.neoforge.client.event.CustomizeGuiOverlayEvent.DebugText event) {
            var left = event.getLeft();
            if (!VoxyCommon.isAvailable()) {
                left.add(net.minecraft.ChatFormatting.RED + "[NeoVoxy] Disabled");
                return;
            }
            var instance = VoxyCommon.getInstance();
            if (instance == null) {
                left.add(net.minecraft.ChatFormatting.YELLOW + "[NeoVoxy] Inactive");
                return;
            }
            var wr = net.minecraft.client.Minecraft.getInstance().levelRenderer;
            me.cortex.voxy.client.core.VoxyRenderSystem vrs = null;
            if (wr != null) {
                vrs = ((me.cortex.voxy.client.core.IGetVoxyRenderSystem) wr).getVoxyRenderSystem();
            }

            left.add((vrs != null ? net.minecraft.ChatFormatting.GREEN : net.minecraft.ChatFormatting.DARK_GREEN) + "[NeoVoxy] " + VoxyCommon.MOD_VERSION);

            java.util.List<String> instanceLines = new java.util.ArrayList<>();
            instance.addDebug(instanceLines);
            for (String line : instanceLines) {
                left.add(net.minecraft.ChatFormatting.AQUA + " [Instance] " + line);
            }

            if (vrs != null) {
                java.util.List<String> renderLines = new java.util.ArrayList<>();
                vrs.addDebugInfo(renderLines);
                for (String line : renderLines) {
                    left.add(net.minecraft.ChatFormatting.GREEN + " [Render] " + line);
                }
            }
        }
    }
}
