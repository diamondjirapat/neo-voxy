package me.cortex.voxy.server;

import me.cortex.voxy.common.Logger;
import me.cortex.voxy.common.world.service.VoxelIngestService;
import me.cortex.voxy.commonImpl.VoxyCommon;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import me.cortex.voxy.commonImpl.importers.WorldImporter;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.storage.LevelResource;
import net.neoforged.bus.api.SubscribeEvent;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.level.ChunkEvent;
import net.neoforged.neoforge.event.level.LevelEvent;
import net.neoforged.neoforge.event.server.ServerStartedEvent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class VoxyServer {
    private static boolean initialized = false;
    private static volatile boolean autoImportsAllowed = false;
    private static final ConcurrentHashMap<WorldIdentifier, WorldImporter> activeImporters = new ConcurrentHashMap<>();

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
        VoxyCommon.getInstance().setBackgroundWorkRunChecker(() -> autoImportsAllowed);
        VoxyServerNetwork.init();
        NeoForge.EVENT_BUS.register(VoxyServer.class);
        initialized = true;

        autoImportsAllowed = !server.getPlayerList().getPlayers().isEmpty();
        if (!autoImportsAllowed) {
            Logger.info("Automatic region importing is paused until a player connects");
        }

        for (ServerLevel level : server.getAllLevels()) {
            autoImportServerLevel(level);
        }
    }

    @SubscribeEvent
    public static void onServerStarted(ServerStartedEvent event) {
        for (ServerLevel level : event.getServer().getAllLevels()) {
            autoImportServerLevel(level);
        }
    }

    @SubscribeEvent
    public static void onLevelLoad(LevelEvent.Load event) {
        if (event.getLevel() instanceof Level level) {
            autoImportLevel(level);
        }
    }

    public static void autoImportServerLevel(ServerLevel level) {
        autoImportLevel(level);
    }

    public static void autoImportLevel(Level level) {
        if (!autoImportsAllowed) return;

        var instance = VoxyCommon.getInstance();
        if (instance == null) return;

        MinecraftServer server = null;
        if (level instanceof ServerLevel serverLevel) {
            server = serverLevel.getServer();
        }
        if (server == null) return;

        var worldId = WorldIdentifier.of(level);
        var engine = instance.getOrCreate(worldId);
        if (engine == null) return;

        Path rootPath = server.getWorldPath(LevelResource.ROOT);
        Path regionPath;

        if (level.dimension() == Level.OVERWORLD) {
            regionPath = rootPath.resolve("region");
        } else if (level.dimension() == Level.NETHER) {
            regionPath = rootPath.resolve("DIM-1").resolve("region");
        } else if (level.dimension() == Level.END) {
            regionPath = rootPath.resolve("DIM1").resolve("region");
        } else {
            regionPath = rootPath.resolve("dimensions")
                    .resolve(level.dimension().location().getNamespace())
                    .resolve(level.dimension().location().getPath())
                    .resolve("region");
        }

        if (Files.exists(regionPath) && Files.isDirectory(regionPath)) {
            instance.getImportManager().makeAndRunIfNone(engine, () -> {
                Logger.info("Auto-importing region directory for level " + level.dimension().location() + " from " + regionPath);
                WorldImporter importer = new WorldImporter(engine, level, instance.getServiceManager(),
                        () -> autoImportsAllowed && instance.savingServiceRateLimiter.getAsBoolean(), 0);
                importer.importRegionDirectoryAsync(regionPath.toFile());
                return importer;
            });
        }
    }

    public static synchronized void onPlayerLoggedIn(ServerPlayer player) {
        if (!initialized || autoImportsAllowed) return;

        autoImportsAllowed = true;
        Logger.info("Player connected; resuming automatic region importing");
        for (ServerLevel level : player.getServer().getAllLevels()) {
            autoImportServerLevel(level);
        }
    }

    public static synchronized void onPlayerLoggedOut(ServerPlayer player) {
        if (!initialized || !autoImportsAllowed) return;

        boolean anotherPlayerIsOnline = player.getServer().getPlayerList().getPlayers().stream()
                .anyMatch(other -> !other.getUUID().equals(player.getUUID()));
        if (!anotherPlayerIsOnline) {
            autoImportsAllowed = false;
            Logger.info("Last player disconnected; pausing automatic region importing");
        }
    }

    public static synchronized void onServerStopping() {
        if (initialized) {
            Logger.info("Stopping NeoVoxy Server instance");
            autoImportsAllowed = false;
            for (WorldImporter importer : activeImporters.values()) {
                try {
                    importer.shutdown();
                } catch (Exception e) {
                    Logger.error("Error shutting down importer", e);
                }
            }
            activeImporters.clear();
            VoxyServerNetwork.shutdown();
            NeoForge.EVENT_BUS.unregister(VoxyServer.class);
            VoxyCommon.shutdownInstance();
            initialized = false;
        }
    }

    @SubscribeEvent
    public static void onChunkLoad(ChunkEvent.Load event) {
        if (event.getChunk() instanceof LevelChunk chunk && event.getLevel() != null && !event.getLevel().isClientSide()) {
            VoxelIngestService.tryAutoIngestChunk(chunk, 0);
        }
    }
}
