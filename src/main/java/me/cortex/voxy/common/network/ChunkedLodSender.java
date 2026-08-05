package me.cortex.voxy.common.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import me.cortex.voxy.common.Logger;
import net.minecraft.server.level.ServerPlayer;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles bandwidth-limited chunked transfer of LOD section data to players.
 * <p>
 * Based on Distant Horizons' FullDataPayloadSender pattern. Large sections are
 * split into smaller chunks (64KB by default) and sent at a controlled rate
 * based on the player's bandwidth allocation.
 */
public class ChunkedLodSender implements AutoCloseable {

    /** Default chunk size: 64KB (smaller than DH's 1MB for lower latency) */
    public static final int CHUNK_SIZE = 65536;

    /** Tick rate for sending (20 ticks/sec = 50ms per tick) */
    private static final int TICK_RATE = 20;

    /** Timer for tick-based sending */
    private static final Timer SEND_TIMER = new Timer("VoxyChunkedLodSender", true);

    private final ServerPlayer player;
    private final long sessionId;
    private final SharedBandwidthLimit sharedBandwidthLimit;
    private final int perPlayerLimitKBps;
    private final AtomicInteger clientRateLimitKBps = new AtomicInteger(Integer.MAX_VALUE);

    private final ConcurrentLinkedQueue<PendingTransfer> transferQueue = new ConcurrentLinkedQueue<>();
    private final TimerTask tickTask;
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    // Stats
    private final AtomicLong totalBytesSent = new AtomicLong();
    private final AtomicInteger sectionsQueued = new AtomicInteger();
    private final AtomicInteger sectionsCompleted = new AtomicInteger();

    /**
     * Create a chunked sender for a specific player.
     */
    public ChunkedLodSender(ServerPlayer player, long sessionId,
            SharedBandwidthLimit sharedBandwidthLimit, int perPlayerLimitKBps) {
        this.player = player;
        this.sessionId = sessionId;
        this.sharedBandwidthLimit = sharedBandwidthLimit;
        this.perPlayerLimitKBps = perPlayerLimitKBps;

        this.tickTask = new TimerTask() {
            @Override
            public void run() {
                tick();
            }
        };

        SEND_TIMER.scheduleAtFixedRate(tickTask, 0, 1000 / TICK_RATE);
        sharedBandwidthLimit.setSenderActive(this, false);
    }

    /**
     * Queue a section for chunked transfer.
     * 
     * @param sectionData Serialized section data
     * @param sectionId   Unique ID for this section (for reassembly)
     * @param onComplete  Callback when transfer completes
     */
    public synchronized void queueSection(byte[] sectionData, long sectionId, Runnable onComplete) {
        if (!isActive.get()) {
            return;
        }

        transferQueue.add(new PendingTransfer(sectionData, sectionId, onComplete));
        sectionsQueued.incrementAndGet();
        updateSharedActiveStatus();
    }

    /**
     * Queue a section without completion callback.
     */
    public void queueSection(byte[] sectionData, long sectionId) {
        queueSection(sectionData, sectionId, null);
    }

    /**
     * Called every tick to send pending data.
     */
    private synchronized void tick() {
        if (!isActive.get() || player == null || !player.isAlive()) {
            return;
        }

        try {
            int clientLimit = clientRateLimitKBps.get();
            if (clientLimit == 0) {
                return;
            }
            // Calculate bytes we can send this tick
            int effectivePlayerLimit = Math.min(perPlayerLimitKBps, clientLimit);
            int bytesRemaining = sharedBandwidthLimit.getBytesPerTick(effectivePlayerLimit);

            while (bytesRemaining > 0) {
                PendingTransfer transfer = transferQueue.peek();
                if (transfer == null) {
                    break;
                }

                // Calculate chunk size (min of remaining bytes, CHUNK_SIZE, and remaining data)
                int dataRemaining = transfer.buffer.readableBytes();
                int chunkSize = Math.min(Math.min(bytesRemaining, CHUNK_SIZE), dataRemaining);

                if (chunkSize <= 0) {
                    break;
                }

                // Read chunk from buffer
                byte[] chunkData = new byte[chunkSize + 13]; // 8 byte sectionId + 4 byte offset + 1 byte isLast = 13 bytes header
                int offset = transfer.bytesSent;

                // Build chunk packet: [sectionId:8][offset:4][isLast:1][data:N]
                chunkData[0] = (byte) (transfer.sectionId >> 56);
                chunkData[1] = (byte) (transfer.sectionId >> 48);
                chunkData[2] = (byte) (transfer.sectionId >> 40);
                chunkData[3] = (byte) (transfer.sectionId >> 32);
                chunkData[4] = (byte) (transfer.sectionId >> 24);
                chunkData[5] = (byte) (transfer.sectionId >> 16);
                chunkData[6] = (byte) (transfer.sectionId >> 8);
                chunkData[7] = (byte) transfer.sectionId;

                chunkData[8] = (byte) (offset >> 24);
                chunkData[9] = (byte) (offset >> 16);
                chunkData[10] = (byte) (offset >> 8);
                chunkData[11] = (byte) offset;

                boolean isLast = (dataRemaining - chunkSize) <= 0;
                chunkData[12] = (byte) (isLast ? 1 : 0);

                // Copy actual data
                transfer.buffer.readBytes(chunkData, 13, chunkSize);
                transfer.bytesSent += chunkSize;

                // Send the chunk
                VoxyNetworkHandler.sendToPlayer(player, VoxyPacketPayload.chunk(sessionId, chunkData));

                bytesRemaining -= chunkSize;
                totalBytesSent.addAndGet(chunkSize);

                // Check if transfer is complete
                if (transfer.buffer.readableBytes() == 0) {
                    transferQueue.poll();
                    sectionsCompleted.incrementAndGet();

                    if (transfer.onComplete != null) {
                        try {
                            transfer.onComplete.run();
                        } catch (Exception e) {
                            Logger.error("Error in transfer completion callback: " + e.getMessage());
                        }
                    }

                    transfer.release();
                }
            }
        } catch (Exception e) {
            Logger.error("Error in ChunkedLodSender.tick: " + e.getMessage(), e);
        } finally {
            // Update active status based on queue
            updateSharedActiveStatus();
        }
    }

    /**
     * Apply client-side processing backpressure. A zero rate pauses this sender;
     * positive values are clamped to the configured per-player ceiling.
     */
    public void setClientRateLimitKBps(int requestedLimitKBps) {
        int limit = Math.max(0, Math.min(requestedLimitKBps, perPlayerLimitKBps));
        clientRateLimitKBps.set(limit);
        updateSharedActiveStatus();
    }

    private void updateSharedActiveStatus() {
        sharedBandwidthLimit.setSenderActive(this,
                isActive.get() && clientRateLimitKBps.get() > 0 && !transferQueue.isEmpty());
    }

    /**
     * Get the number of pending transfers.
     */
    public int getPendingCount() {
        return transferQueue.size();
    }

    /**
     * Get total bytes sent.
     */
    public long getTotalBytesSent() {
        return totalBytesSent.get();
    }

    /**
     * Get stats string for debugging.
     */
    public String getStatsString() {
        return String.format("Queued: %d, Completed: %d, Pending: %d, Sent: %.2f MB",
                sectionsQueued.get(), sectionsCompleted.get(), transferQueue.size(),
                totalBytesSent.get() / (1024.0 * 1024.0));
    }

    @Override
    public synchronized void close() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }
        tickTask.cancel();
        sharedBandwidthLimit.setSenderActive(this, false);
        PendingTransfer transfer;
        while ((transfer = transferQueue.poll()) != null) {
            transfer.release();
        }
    }

    /**
     * Represents a pending section transfer.
     */
    private static class PendingTransfer {
        final ByteBuf buffer;
        final long sectionId;
        final Runnable onComplete;
        int bytesSent = 0;

        PendingTransfer(byte[] data, long sectionId, Runnable onComplete) {
            this.buffer = Unpooled.wrappedBuffer(data);
            this.sectionId = sectionId;
            this.onComplete = onComplete;
        }

        void release() {
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }
}
