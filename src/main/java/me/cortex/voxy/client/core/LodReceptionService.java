package me.cortex.voxy.client.core;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import me.cortex.voxy.client.network.ClientCongestionControl;
import me.cortex.voxy.client.config.VoxyConfig;
import me.cortex.voxy.common.Logger;
import me.cortex.voxy.common.network.BloomFilter;
import me.cortex.voxy.common.network.IdRemapper;
import me.cortex.voxy.common.network.VoxyNetworkHandler;
import me.cortex.voxy.common.network.VoxyPacketPayload;
import me.cortex.voxy.common.world.SectionSerializer;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldUpdater;
import me.cortex.voxy.common.world.other.Mapper;
import me.cortex.voxy.common.voxelization.VoxelizedSection;
import me.cortex.voxy.common.voxelization.WorldConversionFactory;
import net.minecraft.client.Minecraft;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Client-side service for receiving and processing streamed LOD data.
 * <p>
 * Implements server-driven streaming architecture with client hints:
 * <ul>
 * <li>Receives level-0 source voxel sections pushed by the server</li>
 * <li>Responds to cache queries with bloom filter (to skip sections client
 * has)</li>
 * <li>Deserializes section data and remaps server IDs to client IDs</li>
 * <li>Builds the mip chain and complete LOD hierarchy on the client</li>
 * <li>Triggers render updates via {@code markDirty()}</li>
 * </ul>
 */
public class LodReceptionService implements AutoCloseable {

    private static final long NO_SESSION = Long.MIN_VALUE;
    private static final int CHUNK_HEADER_SIZE = VoxyPacketPayload.SESSION_HEADER_SIZE + 13;
    private static final int CACHE_FILTER_EXPECTED_SECTIONS = 750_000;
    private static final int MAX_PROCESSING_QUEUE = 512;
    private static final int PROCESSING_HIGH_WATER = 384;
    private static final int PROCESSING_LOW_WATER = 128;
    private static final int MAX_REASSEMBLY_SECTIONS = 64;
    private static final int MAX_SERIALIZED_SECTION_BYTES = 2 * 1024 * 1024;
    private static final long RECENTER_COOLDOWN_NANOS = TimeUnit.SECONDS.toNanos(1);

    // ==================== Core Fields ==================== //

    private final WorldEngine worldEngine;
    private final Mapper clientMapper;
    private final IdRemapper idRemapper = new IdRemapper();
    private final ClientCongestionControl congestionControl;

    // Chunk reassembly buffers (sectionId -> partial data)
    private final ConcurrentHashMap<Long, ChunkReassemblyBuffer> reassemblyBuffers = new ConcurrentHashMap<>();

    // Processing thread
    private final ThreadPoolExecutor processingExecutor;
    private final AtomicBoolean isActive = new AtomicBoolean(true);
    private final AtomicInteger pendingSectionJobs = new AtomicInteger();
    private final AtomicBoolean transfersPausedForBackpressure = new AtomicBoolean(false);
    private final Consumer<VoxyPacketPayload> networkHandler;

    // Stats
    private final AtomicInteger sectionsReceived = new AtomicInteger(0);
    private final AtomicInteger sectionsApplied = new AtomicInteger(0);
    private final AtomicInteger stalePacketsDiscarded = new AtomicInteger(0);
    private final AtomicBoolean mapperRefreshRequested = new AtomicBoolean(false);

    // ==================== Tracking State ==================== //

    /** Sections that have been received from the server */
    private final LongSet receivedSections = LongSets.synchronize(new LongOpenHashSet());
    private final ConcurrentHashMap<Long, PendingMapperSection> pendingMapperSections = new ConcurrentHashMap<>();

    /** Whether the mapper has been synced (required for processing) */
    private volatile boolean mapperReady = false;

    /** Whether we've already requested sync */
    private volatile boolean syncRequested = false;

    /** Current server-assigned sync session. */
    private volatile long activeSessionId = NO_SESSION;
    private volatile long transferCompleteSession = NO_SESSION;
    private int lastRequestedCenterCellX = Integer.MIN_VALUE;
    private int lastRequestedCenterCellZ = Integer.MIN_VALUE;
    private long nextRecenterNanos;

    public LodReceptionService(WorldEngine worldEngine, Mapper clientMapper,
            me.cortex.voxy.client.core.model.ModelBakerySubsystem modelBakery) {
        this.worldEngine = worldEngine;
        this.clientMapper = clientMapper;
        this.congestionControl = new ClientCongestionControl(this::onRateUpdate);

        this.processingExecutor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_PROCESSING_QUEUE),
                r -> {
                    Thread t = new Thread(r, "VoxyLodReception");
                    t.setDaemon(true);
                    return t;
                },
                (task, executor) -> {
                    if (executor.isShutdown()) {
                        throw new RejectedExecutionException("LOD reception is shutting down");
                    }
                    try {
                        // Last-resort lossless backpressure. Normally the high-water
                        // pause reaches the server before this bounded queue fills.
                        executor.getQueue().put(task);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RejectedExecutionException(e);
                    }
                });

        // Register client message handler
        this.networkHandler = this::handleServerMessage;
        VoxyNetworkHandler.setClientMessageHandler(this.networkHandler);

        Logger.info("LodReceptionService initialized with client-side LOD generation");
    }

    /**
     * Called every client tick to ensure sync is requested.
     * Should be called from the client tick event.
     */
    public void tick() {
        if (!isActive.get()) {
            return;
        }

        if (!VoxyNetworkHandler.shouldEnableStreaming()) {
            return;
        }

        updateBackpressure();

        // Request sync from server if not done yet (to get mapper)
        if (!syncRequested) {
            syncRequested = true;
            int radiusChunks = getConfiguredRadiusChunks();
            rememberCurrentRenderCell();
            Logger.info("Requesting LOD sync with circular radius " + radiusChunks + " chunks");
            VoxyNetworkHandler.sendToServer(VoxyPacketPayload.syncRequest(radiusChunks));
        }

        updateStreamingCenter();

    }

    /**
     * Voxy's render-distance tracker moves in 512-block (32-chunk) cells. Recenter
     * the server scan at that same boundary so walking and teleporting keep the
     * streamed circle aligned with the visible render radius.
     */
    private void updateStreamingCenter() {
        if (activeSessionId == NO_SESSION) {
            return;
        }
        var player = Minecraft.getInstance().player;
        if (player == null) {
            return;
        }
        int cellX = player.getBlockX() >> 9;
        int cellZ = player.getBlockZ() >> 9;
        if (cellX == lastRequestedCenterCellX && cellZ == lastRequestedCenterCellZ) {
            return;
        }

        long now = System.nanoTime();
        if (now < nextRecenterNanos) {
            return;
        }
        lastRequestedCenterCellX = cellX;
        lastRequestedCenterCellZ = cellZ;
        nextRecenterNanos = now + RECENTER_COOLDOWN_NANOS;
        Logger.info("Player moved to LOD render cell [" + cellX + ", " + cellZ +
                "]; recentering circular stream for session " + activeSessionId);
        VoxyNetworkHandler.sendToServer(VoxyPacketPayload.recenterRequest(activeSessionId));
    }

    private void rememberCurrentRenderCell() {
        var player = Minecraft.getInstance().player;
        if (player != null) {
            lastRequestedCenterCellX = player.getBlockX() >> 9;
            lastRequestedCenterCellZ = player.getBlockZ() >> 9;
        }
    }

    /**
     * Handle messages from the server.
     */
    private void handleServerMessage(VoxyPacketPayload payload) {
        switch (payload.messageType()) {
            case VoxyPacketPayload.MSG_MAPPER_SYNC -> handleMapperSync(payload);
            case VoxyPacketPayload.MSG_LOD_SECTION -> handleSection(payload);
            case VoxyPacketPayload.MSG_LOD_CHUNK -> handleChunk(payload);
            case VoxyPacketPayload.MSG_SYNC_COMPLETE -> handleSyncComplete(payload);
            case VoxyPacketPayload.MSG_CACHE_QUERY -> handleCacheQuery(payload);
        }

        // Update congestion control
        congestionControl.onChunkReceived(payload);
    }

    /**
     * Handle mapper sync from server.
     */
    private void handleMapperSync(VoxyPacketPayload payload) {
        long sessionId = payload.parseSessionId();
        byte[] mapperData = payload.parseSessionData();
        if (sessionId == NO_SESSION || mapperData.length == 0) {
            Logger.warn("Received mapper sync without a valid streaming session");
            return;
        }

        boolean newSession = activeSessionId != sessionId;
        if (newSession) {
            reassemblyBuffers.clear();
            pendingMapperSections.clear();
            receivedSections.clear();
            sectionsReceived.set(0);
            sectionsApplied.set(0);
            stalePacketsDiscarded.set(0);
            mapperReady = false;
            mapperRefreshRequested.set(false);
            transfersPausedForBackpressure.set(false);
            transferCompleteSession = NO_SESSION;
        }

        Logger.info("Received mapper sync from server (" + mapperData.length +
                " bytes, session " + sessionId + ")");
        activeSessionId = sessionId;
        submitControlTask(() -> {
            if (!isActive.get() || sessionId != activeSessionId) {
                return;
            }
            if (newSession) {
                idRemapper.reset();
            }
            idRemapper.buildFromServerData(mapperData, clientMapper);
            if (sessionId != activeSessionId) {
                return;
            }
            mapperReady = true;
            mapperRefreshRequested.set(false);
            retryPendingMapperSections(sessionId);
            reportCompletionIfReady(sessionId);
        });
    }

    /**
     * Handle complete section data.
     */
    private void handleSection(VoxyPacketPayload payload) {
        long sessionId = payload.parseSessionId();
        if (!acceptSession(sessionId)) {
            return;
        }
        sectionsReceived.incrementAndGet();
        byte[] sectionData = payload.parseSessionData();
        submitSectionProcessing(sessionId, sectionData);
    }

    /**
     * Handle chunk of a large section.
     */
    private void handleChunk(VoxyPacketPayload payload) {
        byte[] data = payload.data();
        if (data.length < CHUNK_HEADER_SIZE) {
            Logger.warn("Received chunk with insufficient header");
            return;
        }

        long sessionId = payload.parseSessionId();
        if (!acceptSession(sessionId)) {
            return;
        }

        // Parse chunk header: [sessionId:8][sectionId:8][offset:4][isLast:1][data:N]
        long sectionId = ((data[8] & 0xFFL) << 56) | ((data[9] & 0xFFL) << 48) |
                ((data[10] & 0xFFL) << 40) | ((data[11] & 0xFFL) << 32) |
                ((data[12] & 0xFFL) << 24) | ((data[13] & 0xFFL) << 16) |
                ((data[14] & 0xFFL) << 8) | (data[15] & 0xFFL);
        int offset = ((data[16] & 0xFF) << 24) | ((data[17] & 0xFF) << 16) |
                ((data[18] & 0xFF) << 8) | (data[19] & 0xFF);
        boolean isLast = data[20] != 0;

        // Get or create reassembly buffer
        int chunkLength = data.length - CHUNK_HEADER_SIZE;
        long chunkEnd = (long) offset + chunkLength;
        if (offset < 0 || chunkEnd > MAX_SERIALIZED_SECTION_BYTES) {
            reassemblyBuffers.remove(sectionId);
            Logger.warn("Rejected oversized LOD section transfer " + sectionId +
                    " (end offset " + chunkEnd + ")");
            return;
        }

        ChunkReassemblyBuffer buffer = reassemblyBuffers.get(sectionId);
        if (buffer == null) {
            if (reassemblyBuffers.size() >= MAX_REASSEMBLY_SECTIONS) {
                pauseTransfers("too many section reassembly buffers");
                Logger.warn("Rejected LOD chunk because the reassembly limit was reached");
                return;
            }
            ChunkReassemblyBuffer created = new ChunkReassemblyBuffer();
            ChunkReassemblyBuffer existing = reassemblyBuffers.putIfAbsent(sectionId, created);
            buffer = existing == null ? created : existing;
        }

        // Add chunk data
        buffer.addChunk(offset, data, CHUNK_HEADER_SIZE, chunkLength);

        if (isLast) {
            // Complete! Process the section
            reassemblyBuffers.remove(sectionId);
            sectionsReceived.incrementAndGet();

            byte[] completeData = buffer.assemble();
            if (completeData != null) {
                submitSectionProcessing(sessionId, completeData);
            }
        }
    }

    /**
     * Handle sync complete signal.
     */
    private void handleSyncComplete(VoxyPacketPayload payload) {
        long sessionId = payload.parseSessionId();
        if (!acceptSession(sessionId)) {
            return;
        }

        Logger.info("Source transfer complete for session " + sessionId +
                "; waiting for client LOD build queue");
        transferCompleteSession = sessionId;

        // All source-section jobs submitted before this marker must finish before
        // reporting completion. Otherwise a bandwidth-fast transfer can look done
        // while substantial client-side mip generation is still pending.
        submitControlTask(() -> {
            if (isActive.get() && sessionId == activeSessionId) {
                reportCompletionIfReady(sessionId);
            }
        });
    }

    /**
     * Handle cache query from server.
     */
    private void handleCacheQuery(VoxyPacketPayload payload) {
        long sessionId = payload.parseSessionId();
        if (!acceptSession(sessionId)) {
            return;
        }

        submitControlTask(() -> sendPersistedCacheFilter(sessionId));
    }

    private boolean acceptSession(long sessionId) {
        if (sessionId == activeSessionId && sessionId != NO_SESSION) {
            return true;
        }

        if (stalePacketsDiscarded.incrementAndGet() == 1) {
            Logger.info("Discarding packet from inactive LOD session " + sessionId +
                    " (active session " + activeSessionId + ")");
        }
        return false;
    }

    private void submitControlTask(Runnable task) {
        if (!isActive.get()) {
            return;
        }
        try {
            processingExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            if (isActive.get()) {
                Logger.warn("Unable to queue LOD control task: " + e.getMessage());
            }
        }
    }

    private void submitSectionProcessing(long sessionId, byte[] data) {
        if (!isActive.get()) {
            return;
        }
        int pending = pendingSectionJobs.incrementAndGet();
        if (pending >= PROCESSING_HIGH_WATER) {
            pauseTransfers("client LOD processing queue reached " + pending + " sections");
        }
        try {
            processingExecutor.execute(() -> {
                try {
                    processSection(sessionId, data);
                } finally {
                    pendingSectionJobs.decrementAndGet();
                    updateBackpressure();
                }
            });
        } catch (RejectedExecutionException e) {
            pendingSectionJobs.decrementAndGet();
            if (isActive.get()) {
                Logger.warn("Unable to queue received LOD section: " + e.getMessage());
            }
        }
    }

    private void updateBackpressure() {
        if (!isActive.get()) {
            return;
        }
        int pending = pendingSectionJobs.get();
        if (pending >= PROCESSING_HIGH_WATER || !hasPersistenceCapacity()) {
            pauseTransfers(pending >= PROCESSING_HIGH_WATER
                    ? "client LOD processing queue reached " + pending + " sections"
                    : "client LOD persistence queue is full");
            return;
        }
        if (pending <= PROCESSING_LOW_WATER
                && pendingMapperSections.isEmpty()
                && hasPersistenceCapacity()
                && transfersPausedForBackpressure.compareAndSet(true, false)) {
            VoxyNetworkHandler.sendToServer(VoxyPacketPayload.rateUpdate(Integer.MAX_VALUE));
            Logger.info("Resuming LOD transfer after client queues drained (pending " + pending + ")");
        }
    }

    private void pauseTransfers(String reason) {
        if (isActive.get() && activeSessionId != NO_SESSION
                && transfersPausedForBackpressure.compareAndSet(false, true)) {
            VoxyNetworkHandler.sendToServer(VoxyPacketPayload.rateUpdate(0));
            Logger.info("Pausing LOD transfer: " + reason);
        }
    }

    private boolean hasPersistenceCapacity() {
        return worldEngine.instanceIn == null
                || worldEngine.instanceIn.savingServiceRateLimiter.getAsBoolean();
    }

    private boolean awaitPersistenceCapacity(long sessionId) {
        while (isActive.get() && sessionId == activeSessionId && !hasPersistenceCapacity()) {
            pauseTransfers("client LOD persistence queue is full");
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            if (Thread.currentThread().isInterrupted()) {
                return false;
            }
        }
        return isActive.get() && sessionId == activeSessionId;
    }

    /**
     * Process received section data.
     */
    private void processSection(long sessionId, byte[] data) {
        if (!isActive.get() || sessionId != activeSessionId)
            return;

        try {
            SectionSerializer.SectionData sectionData = SectionSerializer.deserialize(data);
            if (sectionData == null) {
                Logger.warn("Failed to deserialize section data");
                return;
            }

            if (sectionData.level != 0) {
                Logger.warn("Ignoring non-source section from server at LOD level " + sectionData.level);
                return;
            }

            if (!isValidSourceHeight(sectionData.y)) {
                Logger.warn("Ignoring source section outside the dimension build height: " +
                        WorldEngine.pprintPos(sectionData.getKey()));
                return;
            }

            long key = sectionData.getKey();
            if (!sectionData.hasData()) {
                Logger.warn("Source section has no voxel data: " + key);
                return;
            }

            if (!idRemapper.isReady()) {
                pendingMapperSections.put(key, new PendingMapperSection(sessionId, data));
                pauseTransfers("waiting for the initial block-state mapper");
                requestMapperRefresh(sessionId);
                return;
            }

            if (!hasAllMappings(sectionData.voxelData)) {
                if (sessionId == activeSessionId) {
                    pendingMapperSections.put(key, new PendingMapperSection(sessionId, data));
                    pauseTransfers("waiting for an updated block-state mapper");
                    requestMapperRefresh(sessionId);
                }
                return;
            }

            long[] remappedVoxels = remapVoxelData(sectionData.voxelData);
            if (sessionId != activeSessionId) {
                return;
            }

            if (!awaitPersistenceCapacity(sessionId)) {
                return;
            }
            buildClientLods(sectionData, remappedVoxels);
            if (sessionId != activeSessionId) {
                return;
            }
            receivedSections.add(key);
            sectionsApplied.incrementAndGet();

        } catch (Exception e) {
            Logger.error("Error processing section: " + e.getMessage());
            Logger.error(e);
        }
    }

    /**
     * Remap the source section's server-side voxel IDs to this client's mapper.
     */
    private long[] remapVoxelData(long[] voxelData) {
        long[] remapped = new long[voxelData.length];
        for (int i = 0; i < voxelData.length; i++) {
            remapped[i] = idRemapper.remapVoxelId(voxelData[i]);
        }
        return remapped;
    }

    private boolean hasAllMappings(long[] voxelData) {
        for (long voxel : voxelData) {
            if (!idRemapper.hasMappingsForVoxel(voxel)) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidSourceHeight(int sourceSectionY) {
        var level = Minecraft.getInstance().level;
        if (level == null) {
            return false;
        }
        int minSourceY = Math.floorDiv(level.getMinBuildHeight(), 32);
        int maxSourceY = Math.floorDiv(level.getMaxBuildHeight() - 1, 32);
        return sourceSectionY >= minSourceY && sourceSectionY <= maxSourceY;
    }

    private void requestMapperRefresh(long sessionId) {
        if (sessionId == activeSessionId && mapperRefreshRequested.compareAndSet(false, true)) {
            Logger.info("Requesting mapper refresh for session " + sessionId +
                    " because imported source sections use newer IDs");
            VoxyNetworkHandler.sendToServer(VoxyPacketPayload.mapperRequest(sessionId));
        }
    }

    private void retryPendingMapperSections(long sessionId) {
        for (var entry : pendingMapperSections.entrySet()) {
            PendingMapperSection pending = entry.getValue();
            if (pending.sessionId == sessionId
                    && pendingMapperSections.remove(entry.getKey(), pending)) {
                processSection(sessionId, pending.data);
            }
        }
        updateBackpressure();
    }

    private void reportCompletionIfReady(long sessionId) {
        if (sessionId != activeSessionId || transferCompleteSession != sessionId) {
            return;
        }
        long pending = pendingMapperSections.values().stream()
                .filter(section -> section.sessionId == sessionId)
                .count();
        if (pending != 0) {
            Logger.info("Client LOD build waiting for " + pending +
                    " source sections that need a mapper refresh (session " + sessionId + ")");
            return;
        }
        if (!reassemblyBuffers.isEmpty()) {
            Logger.info("Client LOD build waiting for " + reassemblyBuffers.size() +
                    " source sections still being reassembled (session " + sessionId + ")");
            return;
        }
        transferCompleteSession = NO_SESSION;
        Logger.info("Client LOD build complete for session " + sessionId +
                "! Received source sections: " + sectionsReceived.get() +
                ", Built: " + sectionsApplied.get() +
                ", Reassembling: " + reassemblyBuffers.size());
    }

    private void sendPersistedCacheFilter(long sessionId) {
        if (!isActive.get() || sessionId != activeSessionId) {
            return;
        }

        BloomFilter filter = BloomFilter.forExpectedElements(CACHE_FILTER_EXPECTED_SECTIONS);
        AtomicInteger cachedSources = new AtomicInteger();
        try {
            worldEngine.storage.iterateStoredSectionPositions(key -> {
                if (WorldEngine.getLevel(key) == 0) {
                    filter.add(key);
                    cachedSources.incrementAndGet();
                }
            });
        } catch (Exception e) {
            Logger.warn("Unable to enumerate persisted LOD cache; using current-session cache only: " +
                    e.getMessage());
        }
        synchronized (receivedSections) {
            for (long key : receivedSections) {
                filter.add(key);
            }
        }

        if (sessionId == activeSessionId) {
            Logger.info("Sending persisted cache filter with " + cachedSources.get() +
                    " level-0 source sections (" + filter.getSerializedSize() + " bytes, session " +
                    sessionId + ")");
            VoxyNetworkHandler.sendToServer(VoxyPacketPayload.cacheResponse(sessionId, filter));
        }
    }

    /**
     * A level-0 WorldSection is 32x32x32 voxels and contains eight Minecraft
     * 16x16x16 chunk sections. Split it back into those source sections, build
     * their mip chains locally, and let WorldUpdater assemble client LODs 0-4.
     */
    private void buildClientLods(SectionSerializer.SectionData source, long[] voxels) {
        if (voxels.length != 32 * 32 * 32) {
            throw new IllegalArgumentException("Expected 32768 source voxels, got " + voxels.length);
        }

        VoxelizedSection chunkSection = VoxelizedSection.createEmpty();
        for (int sectionY = 0; sectionY < 2; sectionY++) {
            for (int sectionZ = 0; sectionZ < 2; sectionZ++) {
                for (int sectionX = 0; sectionX < 2; sectionX++) {
                    chunkSection.zero().setPosition(
                            source.x * 2 + sectionX,
                            source.y * 2 + sectionY,
                            source.z * 2 + sectionZ);

                    int nonAirCount = 0;
                    for (int y = 0; y < 16; y++) {
                        for (int z = 0; z < 16; z++) {
                            for (int x = 0; x < 16; x++) {
                                int sourceIndex = (x + sectionX * 16)
                                        | ((z + sectionZ * 16) << 5)
                                        | ((y + sectionY * 16) << 10);
                                int targetIndex = x | (z << 4) | (y << 8);
                                long voxel = voxels[sourceIndex];
                                chunkSection.section[targetIndex] = voxel;
                                if (!Mapper.isAir(voxel)) {
                                    nonAirCount++;
                                }
                            }
                        }
                    }

                    chunkSection.lvl0NonAirCount = nonAirCount;
                    WorldConversionFactory.mipSection(chunkSection, clientMapper);
                    WorldUpdater.insertUpdate(worldEngine, chunkSection,
                            WorldEngine.MAX_LOD_LAYER, true);
                }
            }
        }
    }

    /**
     * Called when congestion control adjusts rate.
     */
    private void onRateUpdate() {
        // Optionally send rate update to server
        // congestionControl.sendRateUpdate();
    }

    /**
     * Request LOD sync from server.
     */
    public void requestSync() {
        if (!VoxyNetworkHandler.shouldEnableStreaming()) {
            Logger.info("LOD streaming disabled in single-player");
            return;
        }

        int radiusChunks = getConfiguredRadiusChunks();
        Logger.info("Requesting LOD resync with circular radius " + radiusChunks + " chunks");
        VoxyNetworkHandler.sendToServer(VoxyPacketPayload.syncRequest(radiusChunks));
    }

    private static int getConfiguredRadiusChunks() {
        // The config page displays each top-level Voxy section as 32 Minecraft
        // chunks, so transmit the displayed unit rather than the internal value.
        return Math.max(1, VoxyConfig.CONFIG.sectionRenderDistance * 32);
    }

    /**
     * Get reception stats.
     */
    public String getStats() {
        return String.format("Sources received: %d, LOD sources built: %d, Cached sources: %d, Pending builds: %d",
                sectionsReceived.get(), sectionsApplied.get(),
                receivedSections.size(), pendingSectionJobs.get());
    }

    @Override
    public void close() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }
        VoxyNetworkHandler.clearClientMessageHandler(this.networkHandler);
        processingExecutor.shutdownNow();
        reassemblyBuffers.clear();
        receivedSections.clear();
        pendingMapperSections.clear();
        pendingSectionJobs.set(0);
        activeSessionId = NO_SESSION;
        Logger.info("LodReceptionService closed");
    }

    /**
     * Buffer for reassembling chunked section data.
     */
    private static class ChunkReassemblyBuffer {
        private final ConcurrentHashMap<Integer, byte[]> chunks = new ConcurrentHashMap<>();
        private int totalSize = 0;

        synchronized void addChunk(int offset, byte[] data, int srcOffset, int length) {
            byte[] chunk = new byte[length];
            System.arraycopy(data, srcOffset, chunk, 0, length);
            chunks.put(offset, chunk);
            totalSize = Math.max(totalSize, offset + length);
        }

        synchronized byte[] assemble() {
            if (chunks.isEmpty())
                return null;

            byte[] result = new byte[totalSize];
            for (var entry : chunks.entrySet()) {
                System.arraycopy(entry.getValue(), 0, result, entry.getKey(), entry.getValue().length);
            }
            return result;
        }
    }

    private record PendingMapperSection(long sessionId, byte[] data) {
    }
}
