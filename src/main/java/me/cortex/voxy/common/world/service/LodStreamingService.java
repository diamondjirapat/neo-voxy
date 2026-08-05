package me.cortex.voxy.common.world.service;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import me.cortex.voxy.common.Logger;
import me.cortex.voxy.common.network.*;
import me.cortex.voxy.common.world.SectionSerializer;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldSection;
import net.minecraft.server.level.ServerPlayer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Server-side service that manages streaming LOD sections to connected players.
 * <p>
 * Uses a unified server-driven architecture:
 * <ul>
 * <li>Server iterates stored level-0 source sections inside a chunk-based circle around the player</li>
 * <li>Uses client bloom filter hints to skip already-received sections</li>
 * <li>Client builds the complete LOD hierarchy from each source section</li>
 * </ul>
 * Uses {@link ChunkedLodSender} for bandwidth-limited transfer.
 */
public class LodStreamingService implements AutoCloseable {

    private final WorldEngine worldEngine;
    private final SharedBandwidthLimit sharedBandwidthLimit;
    private final ConcurrentHashMap<UUID, PlayerStreamingState> playerStates = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;
    private final ExecutorService scanExecutor;
    private final AtomicBoolean isActive = new AtomicBoolean(true);
    private final AtomicLong nextSessionId = new AtomicLong(System.nanoTime());

    // Config
    private static final int DEFAULT_RADIUS_CHUNKS = 16 * 32;
    private static final int CANDIDATES_PER_BATCH = 128;
    private static final int MAX_PENDING_TRANSFERS = 256;
    private static final int SCAN_SORT_BATCH = 2048;
    private static final int MAX_DISCOVERED_CANDIDATES = 8192;
    private static final int ANGULAR_SCAN_SECTORS = 64;
    private static final int RADIAL_BUCKET_WINDOW = 256;
    private static final long CACHE_RESPONSE_TIMEOUT_SECONDS = 30;
    private final int perPlayerLimitKBps;
    private final int maxStreamingRadiusChunks;

    /**
     * Create streaming service with default settings.
     */
    public LodStreamingService(WorldEngine worldEngine) {
        this(worldEngine, new SharedBandwidthLimit(),
                SharedBandwidthLimit.DEFAULT_PLAYER_LIMIT_KBPS, 64 * 32);
    }

    /**
     * Create streaming service with custom settings.
     */
    public LodStreamingService(WorldEngine worldEngine, SharedBandwidthLimit sharedBandwidthLimit,
            int perPlayerLimitKBps, int maxStreamingRadiusChunks) {
        this.worldEngine = worldEngine;
        this.sharedBandwidthLimit = sharedBandwidthLimit;
        this.perPlayerLimitKBps = perPlayerLimitKBps;
        this.maxStreamingRadiusChunks = maxStreamingRadiusChunks;
        // Keep the world engine alive while streaming service exists
        this.worldEngine.acquireRef();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "VoxyLodStreaming");
            t.setDaemon(true);
            return t;
        });

        this.scanExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "VoxyLodScanner");
            t.setDaemon(true);
            return t;
        });

        Logger.info("LodStreamingService initialized with server-driven streaming");
    }

    /**
     * Handle rate update from client.
     */
    public void handleRateUpdate(ServerPlayer player, VoxyPacketPayload payload) {
        int desiredRate = payload.parseRate();
        PlayerStreamingState state = playerStates.get(player.getUUID());
        if (state != null) {
            state.clientDesiredRate = desiredRate;
            state.sender.setClientRateLimitKBps(desiredRate);
        }
    }

    /**
     * Handle section request from client (deprecated - pull mode removed).
     * <p>
     * This method is kept for backward compatibility with older clients but no
     * longer
     * processes requests. The unified architecture uses server-driven streaming
     * only.
     * 
     * @deprecated Pull mode has been removed. Server now drives all section
     *             streaming.
     */
    @Deprecated
    public void handleSectionRequest(ServerPlayer player, VoxyPacketPayload payload) {
        // Pull mode deprecated - server now drives all streaming
        // Log once per player to help diagnose old clients
        Logger.info("Ignoring pull request from " + player.getName().getString() +
                " (pull mode deprecated, using server-driven streaming)");
    }

    /**
     * Start LOD sync for a player. Called by VoxyServer when it receives a sync
     * request.
     */
    public void startSyncForPlayer(ServerPlayer player, int requestedRadiusChunks) {
        if (!isActive.get()) {
            return;
        }

        long sessionId = nextSessionId.incrementAndGet();
        int radiusChunks = Math.max(1, Math.min(requestedRadiusChunks, maxStreamingRadiusChunks));
        Logger.info("Received sync request from " + player.getName().getString() +
                " (session " + sessionId + ", circular radius " + radiusChunks +
                " chunks)");

        // A render-system reload creates a new client receiver and sends another
        // sync request. Replace the entire state so the old scan and sender backlog
        // cannot overlap the replacement session.
        PlayerStreamingState state = new PlayerStreamingState(
                player, sessionId, radiusChunks, sharedBandwidthLimit, perPlayerLimitKBps);
        PlayerStreamingState previous = playerStates.put(player.getUUID(), state);
        if (previous != null) {
            int droppedTransfers = previous.sender.getPendingCount();
            previous.close();
            Logger.info("Replaced streaming session " + previous.sessionId + " for " +
                    player.getName().getString() + " (discarded " + droppedTransfers +
                    " pending transfers)");
        }

        // Send mapper data
        sendMapperSync(state);

        // Request bloom filter from client to skip sections they already have
        VoxyNetworkHandler.sendToPlayer(player, VoxyPacketPayload.cacheQuery(sessionId));

        // Publish distance-sorted batches while the database scan is still running.
        beginCircularScan(state);
        scheduleStreaming(state, 0, TimeUnit.MILLISECONDS);

        Logger.info("Server-driven streaming enabled for " + player.getName().getString() +
                " (session " + sessionId + ")");
    }

    public void startSyncForPlayer(ServerPlayer player) {
        startSyncForPlayer(player, DEFAULT_RADIUS_CHUNKS);
    }

    /**
     * Send the mapper sync packet to a player.
     */
    private void sendMapperSync(PlayerStreamingState state) {
        byte[] mapperData = IdRemapper.serializeMapper(worldEngine.getMapper());
        VoxyNetworkHandler.sendToPlayer(state.player,
                VoxyPacketPayload.mapperSync(state.sessionId, mapperData));
        Logger.info("Sent mapper sync to " + state.player.getName().getString() +
                " (" + mapperData.length + " bytes, session " + state.sessionId + ")");
    }

    /** Refresh the mapper without replacing a partially completed sync session. */
    public void handleMapperRequest(ServerPlayer player, VoxyPacketPayload payload) {
        PlayerStreamingState state = playerStates.get(player.getUUID());
        if (state != null && isCurrentState(state)
                && payload.parseSessionId() == state.sessionId) {
            sendMapperSync(state);
        }
    }

    /** Cancel the old-center scan and continue around the player's current
     * position without replacing the session or resending cached data. */
    public void handleRecenterRequest(ServerPlayer player, VoxyPacketPayload payload) {
        PlayerStreamingState state = playerStates.get(player.getUUID());
        if (state == null || !isCurrentState(state)
                || payload.parseSessionId() != state.sessionId) {
            return;
        }

        synchronized (state.scanLock) {
            state.scanGeneration.incrementAndGet();
            state.scanInProgress.set(false);
            state.scanComplete.set(false);
            state.candidates.clear();
        }

        Logger.info("Recentering circular LOD scan for " + player.getName().getString() +
                " at chunk [" + (player.getBlockX() >> 4) + ", " +
                (player.getBlockZ() >> 4) + "] (session " + state.sessionId + ")");
        beginCircularScan(state);
        scheduleStreaming(state, 0, TimeUnit.MILLISECONDS);
    }

    private void scheduleStreaming(PlayerStreamingState state, long delay, TimeUnit unit) {
        if (state.streamingTickScheduled.compareAndSet(false, true)) {
            scheduler.schedule(() -> {
                state.streamingTickScheduled.set(false);
                startStreaming(state);
            }, delay, unit);
        }
    }

    /** Drains candidates discovered by the asynchronous circular database scan. */
    private void startStreaming(PlayerStreamingState state) {
        if (!isCurrentState(state)) {
            return;
        }

        worldEngine.markActive();

        if (!state.cacheReady.get()) {
            if (System.nanoTime() < state.cacheWaitDeadlineNanos) {
                scheduleStreaming(state, 100, TimeUnit.MILLISECONDS);
                return;
            }
            if (state.cacheReady.compareAndSet(false, true)) {
                Logger.info("Client cache response timed out for " +
                        state.player.getName().getString() +
                        "; streaming without cache hints (session " + state.sessionId + ")");
            }
        }

        // Keep serialization from outrunning the bandwidth-limited sender and
        // consuming a large amount of heap during wide-radius scans.
        if (state.sender.getPendingCount() >= MAX_PENDING_TRANSFERS) {
            scheduleStreaming(state, 100, TimeUnit.MILLISECONDS);
            return;
        }

        int examined = 0;
        int queued = 0;
        while (examined < CANDIDATES_PER_BATCH
                && state.sender.getPendingCount() < MAX_PENDING_TRANSFERS) {
            if (!isCurrentState(state)) {
                return;
            }

            SectionCandidate candidate = state.candidates.poll();
            if (candidate == null) {
                break;
            }
            long key = candidate.key();
            examined++;
            state.examinedCandidates.incrementAndGet();

            BloomFilter cacheFilter = state.clientCacheFilter;
            if (state.sentSections.contains(key)
                    || (cacheFilter != null && cacheFilter.mightContain(key))) {
                continue;
            }

            WorldSection section = worldEngine.acquireIfExists(key);
            if (section == null) {
                continue;
            }

            try {
                boolean hasContent = section.lvl == 0
                        ? section.getNonEmptyBlockCount() > 0
                        : section.getNonEmptyChildren() != 0;
                if (!hasContent) {
                    continue;
                }

                byte[] data = SectionSerializer.serialize(section);
                if (!isCurrentState(state)) {
                    return;
                }

                if (state.sentSections.add(key)) {
                    state.sender.queueSection(data, key,
                            () -> {
                                BloomFilter currentFilter = state.clientCacheFilter;
                                if (currentFilter != null) {
                                    currentFilter.add(key);
                                }
                            });
                    queued++;
                }
            } finally {
                section.release();
            }
        }

        state.streamBatches++;
        if (queued > 0 && (state.streamBatches == 1 || state.streamBatches % 10 == 0)) {
            Logger.info("Circular LOD scan progress for " +
                    state.player.getName().getString() + " (session " +
                    state.sessionId + "): examined " + state.examinedCandidates.get() +
                    ", discovered " + state.discoveredCandidates.get() +
                    ", source keys checked " + state.checkedSectionKeys.get() +
                    ", W/E " + state.discoveredWest.get() + "/" + state.discoveredEast.get() +
                    ", N/S " + state.discoveredNorth.get() + "/" + state.discoveredSouth.get() +
                    (state.scanComplete.get() ? " (scan complete), " : ", ") +
                    state.sender.getStatsString());
        }

        synchronized (state.scanLock) {
            if (!state.candidates.isEmpty() || !state.scanComplete.get()) {
                scheduleStreaming(state, 25, TimeUnit.MILLISECONDS);
                return;
            }

            int scanGeneration = state.scanGeneration.get();
            if (state.completedScanGeneration.getAndSet(scanGeneration) == scanGeneration) {
                return;
            }
        }

        Logger.info("Entering maintenance mode for " + state.player.getName().getString() +
                " (completed circular radius " + state.radiusChunks +
                " chunks, session " + state.sessionId + ")");

        if (state.initialScanComplete.compareAndSet(false, true)) {
            Logger.info("Initial LOD scan complete for " + state.player.getName().getString() +
                    "; waiting for " + state.sender.getPendingCount() +
                    " queued transfers to drain (session " + state.sessionId + ")");
            scheduleTransferCompletionCheck(state, 0);
        }

        scheduleMaintenanceScan(state);
    }

    private void beginCircularScan(PlayerStreamingState state) {
        int generation;
        synchronized (state.scanLock) {
            if (!isCurrentState(state) || !state.scanInProgress.compareAndSet(false, true)) {
                return;
            }

            generation = state.scanGeneration.incrementAndGet();
            state.scanComplete.set(false);
            state.checkedSectionKeys.set(0);
            state.discoveredCandidates.set(0);
            state.examinedCandidates.set(0);
            state.discoveredWest.set(0);
            state.discoveredEast.set(0);
            state.discoveredNorth.set(0);
            state.discoveredSouth.set(0);
            state.streamBatches = 0;
        }
        scanExecutor.submit(() -> scanStoredSections(state, generation));
    }

    private void scanStoredSections(PlayerStreamingState state, int generation) {
        boolean radialScan = worldEngine.storage.supportsSectionExistenceChecks();
        try {
            if (radialScan) {
                scanRadially(state, generation);
            } else {
                Logger.warn("Storage backend has no fast existence check; falling back to " +
                        "database-order LOD scan for session " + state.sessionId);
                scanInStorageOrder(state, generation);
            }
        } catch (ScanCancelledException ignored) {
            Logger.info("Cancelled obsolete LOD database scan for " +
                    state.player.getName().getString() + " (session " + state.sessionId + ")");
            return;
        } catch (Exception e) {
            Logger.error("LOD database scan failed for " + state.player.getName().getString(), e);
        } finally {
            boolean completedCurrentScan;
            synchronized (state.scanLock) {
                completedCurrentScan = isCurrentScan(state, generation);
                if (completedCurrentScan) {
                    state.scanComplete.set(true);
                    state.scanInProgress.set(false);
                }
            }
            if (completedCurrentScan) {
                Logger.info("Finished incremental " + (radialScan ? "radial" : "database-order") +
                        " LOD scan for " + state.player.getName().getString() + " (session " +
                        state.sessionId + "): checked " + state.checkedSectionKeys.get() +
                        " source keys, discovered " + state.discoveredCandidates.get() +
                        " source sections (W/E " + state.discoveredWest.get() + "/" +
                        state.discoveredEast.get() + ", N/S " + state.discoveredNorth.get() +
                        "/" + state.discoveredSouth.get() + ")");
                scheduleStreaming(state, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Probe level-0 section keys in increasing horizontal distance. This avoids
     * RocksDB's level/Y/Z/X key ordering painting a long strip before adjacent
     * Z rows are ever visited.
     */
    private void scanRadially(PlayerStreamingState state, int generation) {
        int playerChunkX = state.player.getBlockX() >> 4;
        int playerChunkZ = state.player.getBlockZ() >> 4;
        long playerCenterX2 = playerChunkX * 2L + 1;
        long playerCenterZ2 = playerChunkZ * 2L + 1;
        long radius2 = state.radiusChunks * 2L;
        long radiusSquared = radius2 * radius2;

        int centerSectionX = Math.floorDiv(playerChunkX, 2);
        int centerSectionZ = Math.floorDiv(playerChunkZ, 2);
        int sourceRadius = Math.floorDiv(state.radiusChunks + 1, 2) + 1;

        int minSectionY = Math.floorDiv(state.player.serverLevel().getMinBuildHeight(), 32);
        int maxSectionY = Math.floorDiv(state.player.serverLevel().getMaxBuildHeight() - 1, 32);
        List<SectionCandidate> batch = new ArrayList<>(SCAN_SORT_BATCH);

        // Build only a narrow radial window at a time. The former implementation
        // retained every horizontal position for the entire (potentially hours-long)
        // scan; at the maximum radius two scans could pin close to 100 MiB.
        for (int windowStart = 0; windowStart <= radius2; windowStart += RADIAL_BUCKET_WINDOW) {
            ensureScanCurrent(state, generation);
            int windowEnd = (int) Math.min(radius2, windowStart + RADIAL_BUCKET_WINDOW - 1L);
            LongArrayList[] radialBuckets = new LongArrayList[windowEnd - windowStart + 1];
            long minimumDistanceSquared = (long) windowStart * windowStart;
            long maximumDistanceExclusive = (long) (windowEnd + 1) * (windowEnd + 1);

            for (int dz = -sourceRadius; dz <= sourceRadius; dz++) {
                ensureScanCurrent(state, generation);
                int sectionZ = centerSectionZ + dz;
                long distanceZ2 = sectionZ * 4L + 2L - playerCenterZ2;
                for (int dx = -sourceRadius; dx <= sourceRadius; dx++) {
                    int sectionX = centerSectionX + dx;
                    long distanceX2 = sectionX * 4L + 2L - playerCenterX2;
                    long distanceSquared = distanceX2 * distanceX2 + distanceZ2 * distanceZ2;
                    if (distanceSquared > radiusSquared
                            || distanceSquared < minimumDistanceSquared
                            || distanceSquared >= maximumDistanceExclusive) {
                        continue;
                    }

                    int bucketIndex = (int) Math.sqrt(distanceSquared) - windowStart;
                    LongArrayList bucket = radialBuckets[bucketIndex];
                    if (bucket == null) {
                        bucket = radialBuckets[bucketIndex] = new LongArrayList();
                    }
                    bucket.add(((long) sectionX << 32) | (sectionZ & 0xFFFFFFFFL));
                }
            }

            for (LongArrayList bucket : radialBuckets) {
                if (bucket != null) {
                    processRadialBucket(state, generation, bucket, playerCenterX2, playerCenterZ2,
                            minSectionY, maxSectionY, batch);
                }
            }
        }
        publishCandidates(state, generation, batch, true);
    }

    private void processRadialBucket(PlayerStreamingState state, int generation, LongArrayList bucket,
            long playerCenterX2, long playerCenterZ2, int minSectionY, int maxSectionY,
            List<SectionCandidate> batch) {
        // Interleave positions from every compass sector inside this radial band.
        LongArrayList[] angularBuckets = new LongArrayList[ANGULAR_SCAN_SECTORS];
        int largestAngularBucket = 0;
        for (int positionIndex = 0; positionIndex < bucket.size(); positionIndex++) {
            long packedPosition = bucket.getLong(positionIndex);
            int sectionX = (int) (packedPosition >> 32);
            int sectionZ = (int) packedPosition;
            long distanceX2 = sectionX * 4L + 2L - playerCenterX2;
            long distanceZ2 = sectionZ * 4L + 2L - playerCenterZ2;
            double angle = Math.atan2(distanceZ2, distanceX2) + Math.PI;
            int sector = Math.min(ANGULAR_SCAN_SECTORS - 1,
                    (int) (angle * ANGULAR_SCAN_SECTORS / (Math.PI * 2.0)));
            LongArrayList angularBucket = angularBuckets[sector];
            if (angularBucket == null) {
                angularBucket = angularBuckets[sector] = new LongArrayList();
            }
            angularBucket.add(packedPosition);
            largestAngularBucket = Math.max(largestAngularBucket, angularBucket.size());
        }

        for (int angularIndex = 0; angularIndex < largestAngularBucket; angularIndex++) {
            for (LongArrayList angularBucket : angularBuckets) {
                if (angularBucket == null || angularIndex >= angularBucket.size()) {
                    continue;
                }
                probeHorizontalSourcePosition(state, generation, angularBucket.getLong(angularIndex),
                        playerCenterX2, playerCenterZ2, minSectionY, maxSectionY, batch);
            }
        }
    }

    private void probeHorizontalSourcePosition(PlayerStreamingState state, int generation,
            long packedPosition, long playerCenterX2, long playerCenterZ2,
            int minSectionY, int maxSectionY, List<SectionCandidate> batch) {
        ensureScanCurrent(state, generation);
        int sectionX = (int) (packedPosition >> 32);
        int sectionZ = (int) packedPosition;
        long distanceX2 = sectionX * 4L + 2L - playerCenterX2;
        long distanceZ2 = sectionZ * 4L + 2L - playerCenterZ2;
        long distanceSquared = distanceX2 * distanceX2 + distanceZ2 * distanceZ2;

        for (int sectionY = minSectionY; sectionY <= maxSectionY; sectionY++) {
            ensureScanCurrent(state, generation);
            long key = WorldEngine.getWorldSectionId(0, sectionX, sectionY, sectionZ);
            state.checkedSectionKeys.incrementAndGet();
            if (state.sentSections.contains(key) || !worldEngine.storage.hasSection(key)) {
                continue;
            }
            if (distanceX2 < 0) {
                state.discoveredWest.incrementAndGet();
            } else {
                state.discoveredEast.incrementAndGet();
            }
            if (distanceZ2 < 0) {
                state.discoveredNorth.incrementAndGet();
            } else {
                state.discoveredSouth.incrementAndGet();
            }
            batch.add(new SectionCandidate(key, distanceSquared));
            if (batch.size() >= SCAN_SORT_BATCH) {
                publishCandidates(state, generation, batch, true);
            }
        }
    }

    private void scanInStorageOrder(PlayerStreamingState state, int generation) {
        int playerChunkX = state.player.getBlockX() >> 4;
        int playerChunkZ = state.player.getBlockZ() >> 4;
        long playerCenterX2 = playerChunkX * 2L + 1;
        long playerCenterZ2 = playerChunkZ * 2L + 1;
        long radius2 = state.radiusChunks * 2L;
        long radiusSquared = radius2 * radius2;

        List<SectionCandidate> batch = new ArrayList<>(SCAN_SORT_BATCH);
        worldEngine.storage.iterateStoredSectionPositions(key -> {
            ensureScanCurrent(state, generation);
            state.checkedSectionKeys.incrementAndGet();

            if (state.sentSections.contains(key) || WorldEngine.getLevel(key) != 0) {
                return;
            }

            long sectionSizeChunks = 2L;
            long sectionCenterX2 = WorldEngine.getX(key) * sectionSizeChunks * 2L
                    + sectionSizeChunks;
            long sectionCenterZ2 = WorldEngine.getZ(key) * sectionSizeChunks * 2L
                    + sectionSizeChunks;
            long dx2 = sectionCenterX2 - playerCenterX2;
            long dz2 = sectionCenterZ2 - playerCenterZ2;
            long distanceSquared = dx2 * dx2 + dz2 * dz2;

            if (distanceSquared <= radiusSquared) {
                batch.add(new SectionCandidate(key, distanceSquared));
                if (batch.size() >= SCAN_SORT_BATCH) {
                    publishCandidates(state, generation, batch, false);
                }
            }
        });
        publishCandidates(state, generation, batch, false);
    }

    private void publishCandidates(PlayerStreamingState state, int generation,
            List<SectionCandidate> batch, boolean preserveRadialOrder) {
        if (batch.isEmpty()) {
            return;
        }
        if (!preserveRadialOrder) {
            batch.sort(Comparator.comparingLong(SectionCandidate::distanceSquared));
        }
        while (state.candidates.size() >= MAX_DISCOVERED_CANDIDATES) {
            ensureScanCurrent(state, generation);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
        }
        synchronized (state.scanLock) {
            ensureScanCurrent(state, generation);
            state.candidates.addAll(batch);
            state.discoveredCandidates.addAndGet(batch.size());
            batch.clear();
        }
    }

    private boolean isCurrentScan(PlayerStreamingState state, int generation) {
        return isCurrentState(state) && state.scanGeneration.get() == generation;
    }

    private void ensureScanCurrent(PlayerStreamingState state, int generation) {
        if (!isCurrentScan(state, generation)) {
            throw ScanCancelledException.INSTANCE;
        }
    }

    private record SectionCandidate(long key, long distanceSquared) {
    }

    private static final class ScanCancelledException extends RuntimeException {
        static final ScanCancelledException INSTANCE = new ScanCancelledException();

        private ScanCancelledException() {
            super(null, null, false, false);
        }
    }

    /**
     * Maintenance mode - periodically rescans from player position for new LODs.
     */
    private void scheduleMaintenanceScan(PlayerStreamingState state) {
        if (!isCurrentState(state)) {
            return;
        }

        // Rebuild the stored-section snapshot after 30 seconds to pick up newly
        // imported/generated sections and recenter around the player's new chunk.
        scheduler.schedule(() -> {
            if (isCurrentState(state)) {
                Logger.info("Starting circular maintenance scan for " +
                        state.player.getName().getString() + " (radius " +
                        state.radiusChunks + " chunks, session " + state.sessionId + ")");
                beginCircularScan(state);
            }
        }, 30, TimeUnit.SECONDS);
    }

    private boolean isCurrentState(PlayerStreamingState state) {
        return state != null
                && isActive.get()
                && state.active.get()
                && state.player.isAlive()
                && playerStates.get(state.player.getUUID()) == state;
    }

    /**
     * The circular scan only fills the bandwidth-limited sender. Report completion
     * after that queue is actually empty, and log progress while it drains.
     */
    private void scheduleTransferCompletionCheck(PlayerStreamingState state, long delaySeconds) {
        scheduler.schedule(() -> {
            if (!isCurrentState(state) || state.completionSent.get()) {
                return;
            }

            if (!state.scanComplete.get() || !state.candidates.isEmpty()) {
                scheduleTransferCompletionCheck(state, 1);
                return;
            }

            int pending = state.sender.getPendingCount();
            if (pending == 0) {
                if (state.completionSent.compareAndSet(false, true)) {
                    VoxyNetworkHandler.sendToPlayer(state.player,
                            VoxyPacketPayload.syncComplete(state.sessionId));
                    Logger.info("LOD sync transfer complete for " +
                            state.player.getName().getString() + " (session " +
                            state.sessionId + "): " + state.sender.getStatsString());
                }
                return;
            }

            state.transferProgressChecks++;
            if (state.transferProgressChecks == 1 || state.transferProgressChecks % 10 == 0) {
                Logger.info("LOD transfer progress for " + state.player.getName().getString() +
                        " (session " + state.sessionId + "): " +
                        state.sender.getStatsString());
            }
            scheduleTransferCompletionCheck(state, 1);
        }, delaySeconds, TimeUnit.SECONDS);
    }

    /**
     * Handle cache response from client (bloom filter).
     */
    public void handleCacheResponse(ServerPlayer player, VoxyPacketPayload payload) {
        PlayerStreamingState state = playerStates.get(player.getUUID());

        if (state != null && state.active.get()) {
            long responseSessionId = payload.parseSessionId();
            if (responseSessionId != state.sessionId) {
                Logger.info("Ignoring stale cache response for " +
                        player.getName().getString() + " (session " +
                        responseSessionId + ", current " + state.sessionId + ")");
                return;
            }

            BloomFilter clientCache = payload.parseCacheResponseBloomFilter();
            if (clientCache != null) {
                // The client sizes this filter for its persisted cache. Replacing the
                // empty server placeholder avoids invalid merges and prevents the old
                // 10k-element filter from saturating on large worlds.
                state.clientCacheFilter = clientCache;
                state.cacheReady.set(true);
                Logger.info("Accepted client cache filter for " + player.getName().getString() +
                        " (" + clientCache.getSerializedSize() + " bytes, session " +
                        state.sessionId + ")");
                scheduleStreaming(state, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Called when a player disconnects.
     */
    public void onPlayerDisconnect(UUID playerId) {
        PlayerStreamingState state = playerStates.remove(playerId);
        if (state != null) {
            state.close();
        }
        VoxyNetworkHandler.removePlayer(playerId);
    }

    public boolean hasActivePlayers() {
        return !playerStates.isEmpty();
    }

    /**
     * Get streaming stats for a player.
     */
    public String getPlayerStats(UUID playerId) {
        PlayerStreamingState state = playerStates.get(playerId);
        if (state == null) {
            return "No active streaming";
        }
        return state.sender.getStatsString() +
                ", Sent: " + state.sentSections.size();
    }

    @Override
    public void close() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdown();
        scanExecutor.shutdownNow();

        for (PlayerStreamingState state : playerStates.values()) {
            state.close();
        }
        playerStates.clear();

        // Release the world engine reference
        try {
            worldEngine.releaseRef();
        } catch (Exception e) {
            Logger.error("Error releasing world engine ref", e);
        }

        Logger.info("LodStreamingService closed");
    }

    /**
     * Per-player streaming state.
     */
    private static class PlayerStreamingState {
        final ServerPlayer player;
        final long sessionId;
        final int radiusChunks;
        final ChunkedLodSender sender;
        final LongSet sentSections = LongSets.synchronize(new LongOpenHashSet());
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicBoolean initialScanComplete = new AtomicBoolean(false);
        final AtomicBoolean completionSent = new AtomicBoolean(false);
        final AtomicBoolean streamingTickScheduled = new AtomicBoolean(false);

        // Streaming state
        final BlockingQueue<SectionCandidate> candidates = new LinkedBlockingQueue<>();
        final Object scanLock = new Object();
        final AtomicBoolean scanInProgress = new AtomicBoolean(false);
        final AtomicBoolean scanComplete = new AtomicBoolean(false);
        final AtomicBoolean cacheReady = new AtomicBoolean(false);
        final AtomicInteger scanGeneration = new AtomicInteger(0);
        final AtomicInteger completedScanGeneration = new AtomicInteger(0);
        final AtomicLong checkedSectionKeys = new AtomicLong();
        final AtomicLong discoveredCandidates = new AtomicLong();
        final AtomicLong examinedCandidates = new AtomicLong();
        final AtomicLong discoveredWest = new AtomicLong();
        final AtomicLong discoveredEast = new AtomicLong();
        final AtomicLong discoveredNorth = new AtomicLong();
        final AtomicLong discoveredSouth = new AtomicLong();
        final long cacheWaitDeadlineNanos = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(CACHE_RESPONSE_TIMEOUT_SECONDS);
        int streamBatches = 0;
        int transferProgressChecks = 0;
        int clientDesiredRate = SharedBandwidthLimit.DEFAULT_PLAYER_LIMIT_KBPS;
        volatile BloomFilter clientCacheFilter;

        PlayerStreamingState(ServerPlayer player, long sessionId, int radiusChunks,
                SharedBandwidthLimit sharedLimit, int limitKBps) {
            this.player = player;
            this.sessionId = sessionId;
            this.radiusChunks = radiusChunks;
            this.sender = new ChunkedLodSender(player, sessionId, sharedLimit, limitKBps);
        }

        void close() {
            if (active.compareAndSet(true, false)) {
                sender.close();
            }
        }
    }
}
