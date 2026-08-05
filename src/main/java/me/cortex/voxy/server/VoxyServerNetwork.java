package me.cortex.voxy.server;

import me.cortex.voxy.common.Logger;
import me.cortex.voxy.common.network.SharedBandwidthLimit;
import me.cortex.voxy.common.network.VoxyNetworkHandler;
import me.cortex.voxy.common.network.VoxyPacketPayload;
import me.cortex.voxy.common.world.service.LodStreamingService;
import me.cortex.voxy.commonImpl.VoxyCommon;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.minecraft.server.level.ServerPlayer;
import net.neoforged.bus.api.SubscribeEvent;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.entity.player.PlayerEvent;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class VoxyServerNetwork {
    private static final SharedBandwidthLimit bandwidthLimit = new SharedBandwidthLimit(() -> 5000); // 5 MB/s global limit
    private static final ConcurrentHashMap<WorldIdentifier, LodStreamingService> streamingServices = new ConcurrentHashMap<>();

    public static void init() {
        VoxyNetworkHandler.setServerMessageHandler(VoxyServerNetwork::handleClientMessage);
        NeoForge.EVENT_BUS.register(VoxyServerNetwork.class);
        Logger.info("Initialized VoxyServerNetwork packet handlers");
    }

    public static void shutdown() {
        for (LodStreamingService service : streamingServices.values()) {
            try {
                service.close();
            } catch (Exception e) {
                Logger.error("Error closing LodStreamingService", e);
            }
        }
        streamingServices.clear();
        VoxyNetworkHandler.clearServerState();
        NeoForge.EVENT_BUS.unregister(VoxyServerNetwork.class);
    }

    @SubscribeEvent
    public static void onPlayerLoggedOut(PlayerEvent.PlayerLoggedOutEvent event) {
        if (event.getEntity() instanceof ServerPlayer player) {
            removePlayerFromStreamingServices(player.getUUID());
            VoxyServer.onPlayerLoggedOut(player);
        }
    }

    @SubscribeEvent
    public static void onPlayerLoggedIn(PlayerEvent.PlayerLoggedInEvent event) {
        if (event.getEntity() instanceof ServerPlayer player) {
            VoxyServer.onPlayerLoggedIn(player);
        }
    }

    @SubscribeEvent
    public static void onPlayerChangedDimension(PlayerEvent.PlayerChangedDimensionEvent event) {
        if (event.getEntity() instanceof ServerPlayer player) {
            removePlayerFromStreamingServices(player.getUUID());
        }
    }

    @SubscribeEvent
    public static void onPlayerClone(PlayerEvent.Clone event) {
        if (event.getEntity() instanceof ServerPlayer player) {
            removePlayerFromStreamingServices(player.getUUID());
        }
    }

    private static void removePlayerFromStreamingServices(UUID playerId) {
        for (var entry : streamingServices.entrySet()) {
            LodStreamingService service = entry.getValue();
            service.onPlayerDisconnect(playerId);
            if (!service.hasActivePlayers()
                    && streamingServices.remove(entry.getKey(), service)) {
                service.close();
            }
        }
        VoxyNetworkHandler.removePlayer(playerId);
    }

    private static void handleClientMessage(ServerPlayer player, VoxyPacketPayload payload) {
        switch (payload.messageType()) {
            case VoxyPacketPayload.MSG_SYNC_REQUEST -> handleSyncRequest(player, payload);
            case VoxyPacketPayload.MSG_CACHE_RESPONSE -> handleCacheResponse(player, payload);
            case VoxyPacketPayload.MSG_RATE_UPDATE -> handleRateUpdate(player, payload);
            case VoxyPacketPayload.MSG_REQUEST_SECTIONS -> handleRequestSections(player, payload);
            case VoxyPacketPayload.MSG_MAPPER_REQUEST -> handleMapperRequest(player, payload);
            case VoxyPacketPayload.MSG_RECENTER_REQUEST -> handleRecenterRequest(player, payload);
        }
    }

    private static LodStreamingService getOrCreateStreamingService(ServerPlayer player) {
        var instance = VoxyCommon.getInstance();
        if (instance == null) return null;

        var worldId = WorldIdentifier.of(player.serverLevel());
        var engine = instance.getOrCreate(worldId);
        if (engine == null) return null;

        return streamingServices.computeIfAbsent(worldId,
                id -> new LodStreamingService(engine, bandwidthLimit, 2000, 64 * 32));
    }

    private static LodStreamingService getStreamingService(ServerPlayer player) {
        return streamingServices.get(WorldIdentifier.of(player.serverLevel()));
    }

    private static void handleSyncRequest(ServerPlayer player, VoxyPacketPayload payload) {
        VoxyNetworkHandler.setPlayerCapable(player.getUUID(), true);
        var service = getOrCreateStreamingService(player);
        if (service != null) {
            int radiusChunks = payload.parseSyncRadiusChunks(16 * 32);
            service.startSyncForPlayer(player, radiusChunks);
        }
    }

    private static void handleCacheResponse(ServerPlayer player, VoxyPacketPayload payload) {
        var service = getStreamingService(player);
        if (service != null) {
            service.handleCacheResponse(player, payload);
        }
    }

    private static void handleRateUpdate(ServerPlayer player, VoxyPacketPayload payload) {
        var service = getStreamingService(player);
        if (service != null) {
            service.handleRateUpdate(player, payload);
        }
    }

    private static void handleRequestSections(ServerPlayer player, VoxyPacketPayload payload) {
        var service = getStreamingService(player);
        if (service != null) {
            service.handleSectionRequest(player, payload);
        }
    }

    private static void handleMapperRequest(ServerPlayer player, VoxyPacketPayload payload) {
        var service = getStreamingService(player);
        if (service != null) {
            service.handleMapperRequest(player, payload);
        }
    }

    private static void handleRecenterRequest(ServerPlayer player, VoxyPacketPayload payload) {
        var service = getStreamingService(player);
        if (service != null) {
            service.handleRecenterRequest(player, payload);
        }
    }
}
