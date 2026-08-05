package me.cortex.voxy.common.world.service;

import me.cortex.voxy.common.Logger;
import me.cortex.voxy.common.thread.Service;
import me.cortex.voxy.common.thread.ServiceManager;
import me.cortex.voxy.common.util.Pair;
import me.cortex.voxy.common.voxelization.ILightingSupplier;
import me.cortex.voxy.common.voxelization.VoxelizedSection;
import me.cortex.voxy.common.voxelization.WorldConversionFactory;
import me.cortex.voxy.common.world.WorldEngine;
import me.cortex.voxy.common.world.WorldUpdater;
import me.cortex.voxy.commonImpl.VoxyCommon;
import me.cortex.voxy.commonImpl.WorldIdentifier;
import net.minecraft.core.SectionPos;
import net.minecraft.world.level.LightLayer;
import net.minecraft.world.level.chunk.DataLayer;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import net.minecraft.world.level.lighting.LayerLightSectionStorage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

public class VoxelIngestService {
    private static final int MAX_PENDING_INGEST_SECTIONS = 4096;
    private static final ThreadLocal<VoxelizedSection> SECTION_CACHE = ThreadLocal.withInitial(VoxelizedSection::createEmpty);
    private final Service service;
    private record IngestSection(int cx, int cy, int cz, WorldEngine world, LevelChunkSection section,
            DataLayer blockLight, DataLayer skyLight, int maxLodLayer){}
    private final ConcurrentLinkedDeque<IngestSection> ingestQueue = new ConcurrentLinkedDeque<>();
    private final AtomicInteger queuedSections = new AtomicInteger();

    public VoxelIngestService(ServiceManager pool) {
        this(pool, () -> true);
    }

    public VoxelIngestService(ServiceManager pool, BooleanSupplier rateLimiter) {
        this.service = pool.createService(
                () -> new Pair<>(this::processJob, () -> {}),
                5000, "Ingest service", rateLimiter);
    }

    private void processJob() {
        var task = this.ingestQueue.pop();
        this.queuedSections.decrementAndGet();
        task.world.markActive();

        var section = task.section;
        var vs = SECTION_CACHE.get().setPosition(task.cx, task.cy, task.cz);

        if (section.hasOnlyAir() && task.blockLight==null && task.skyLight==null) {//If the chunk section has lighting data, propagate it
            WorldUpdater.insertUpdate(task.world, vs.zero(), task.maxLodLayer);
        } else {
            VoxelizedSection csec = WorldConversionFactory.convert(
                    SECTION_CACHE.get(),
                    task.world.getMapper(),
                    section.getStates(),
                    section.getBiomes(),
                    getLightingSupplier(task)
            );
            if (task.maxLodLayer > 0) {
                WorldConversionFactory.mipSection(csec, task.world.getMapper());
            }
            WorldUpdater.insertUpdate(task.world, csec, task.maxLodLayer);
        }
    }

    @NotNull
    private static ILightingSupplier getLightingSupplier(IngestSection task) {
        ILightingSupplier supplier = (x,y,z) -> (byte) 0;
        var sla = task.skyLight;
        var bla = task.blockLight;
        boolean sl = sla != null && !sla.isEmpty();
        boolean bl = bla != null && !bla.isEmpty();
        if (sl || bl) {
            if (sl && bl) {
                supplier = (x,y,z)-> {
                    int block = Math.min(15,bla.get(x, y, z));
                    int sky = Math.min(15,sla.get(x, y, z));
                    return (byte) (sky|(block<<4));
                };
            } else if (bl) {
                supplier = (x,y,z)-> {
                    int block = Math.min(15,bla.get(x, y, z));
                    int sky = 0;
                    return (byte) (sky|(block<<4));
                };
            } else {
                supplier = (x,y,z)-> {
                    int block = 0;
                    int sky = Math.min(15,sla.get(x, y, z));
                    return (byte) (sky|(block<<4));
                };
            }
        }
        return supplier;
    }

    private static boolean shouldIngestSection(LevelChunkSection section, int cx, int cy, int cz) {
        return true;
    }

    public boolean enqueueIngest(WorldEngine engine, LevelChunk chunk) {
        return enqueueIngest(engine, chunk, WorldEngine.MAX_LOD_LAYER);
    }

    private boolean enqueue(IngestSection section) {
        while (this.service.isLive()
                && this.queuedSections.get() >= MAX_PENDING_INGEST_SECTIONS) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            if (Thread.currentThread().isInterrupted()) {
                return false;
            }
        }
        if (!this.service.isLive()) {
            return false;
        }

        this.ingestQueue.add(section);
        this.queuedSections.incrementAndGet();
        try {
            this.service.execute();
            return true;
        } catch (Exception e) {
            if (this.ingestQueue.remove(section)) {
                this.queuedSections.decrementAndGet();
            }
            Logger.error("Executing had an error: assume shutting down, aborting", e);
            return false;
        }
    }

    public boolean enqueueIngest(WorldEngine engine, LevelChunk chunk, int maxLodLayer) {
        if (maxLodLayer < 0 || maxLodLayer > WorldEngine.MAX_LOD_LAYER) {
            throw new IllegalArgumentException("Invalid maximum LOD layer: " + maxLodLayer);
        }
        if (!this.service.isLive()) {
            return false;
        }
        if (!engine.isLive()) {
            throw new IllegalStateException("Tried inserting chunk into WorldEngine that was not alive");
        }

        engine.markActive();

        var lightingProvider = chunk.getLevel().getLightEngine();
        boolean gotLighting = false;

        int i = chunk.getMinSection() - 1;
        boolean allEmpty = true;
        for (var section : chunk.getSections()) {
            i++;
            if (section == null || !shouldIngestSection(section, chunk.getPos().x, i, chunk.getPos().z)) continue;
            allEmpty&=section.hasOnlyAir();
            //if (section.isEmpty()) continue;
            var pos = SectionPos.of(chunk.getPos(), i);
            if (lightingProvider.getDebugSectionType(LightLayer.SKY, pos) != LayerLightSectionStorage.SectionType.LIGHT_AND_DATA && lightingProvider.getDebugSectionType(LightLayer.BLOCK, pos) != LayerLightSectionStorage.SectionType.LIGHT_AND_DATA)
                continue;
            gotLighting = true;
        }

        if (allEmpty&&!gotLighting) {
            //Special case all empty chunk columns, we need to clear it out
            i = chunk.getMinSection() - 1;
            for (var section : chunk.getSections()) {
                i++;
                if (section == null || !shouldIngestSection(section, chunk.getPos().x, i, chunk.getPos().z)) continue;
                if (!this.enqueue(new IngestSection(chunk.getPos().x, i, chunk.getPos().z, engine, section,
                        null, null, maxLodLayer))) {
                    break;
                }
            }
        }

        if (!gotLighting) {
            return false;
        }

        var blp = lightingProvider.getLayerListener(LightLayer.BLOCK);
        var slp = lightingProvider.getLayerListener(LightLayer.SKY);


        i = chunk.getMinSection() - 1;
        for (var section : chunk.getSections()) {
            i++;
            if (section == null || !shouldIngestSection(section, chunk.getPos().x, i, chunk.getPos().z)) continue;
            //if (section.isEmpty()) continue;
            var pos = SectionPos.of(chunk.getPos(), i);

            var bl = blp.getDataLayerData(pos);
            if (bl != null) {
                bl = bl.copy();
            }

            var sl = slp.getDataLayerData(pos);
            if (sl != null) {
                sl = sl.copy();
            }

            //If its null for either, assume failure to obtain lighting and ignore section
            //if (blNone && slNone) {
            //    continue;
            //}

            if (!this.enqueue(new IngestSection(chunk.getPos().x, i, chunk.getPos().z, engine, section,
                    bl, sl, maxLodLayer))) {//TODO: fixme, this is technically not safe todo on the chunk load ingest, we need to copy the section data so it cant be modified while being read
                break;
            }
        }
        return true;
    }

    public int getTaskCount() {
        return this.service.numJobs();
    }

    public void shutdown() {
        this.service.shutdown();
    }

    //Utility method to ingest a chunk into the given WorldIdentifier or world
    public static boolean tryIngestChunk(WorldIdentifier worldId, LevelChunk chunk) {
        return tryIngestChunk(worldId, chunk, WorldEngine.MAX_LOD_LAYER);
    }

    public static boolean tryIngestChunk(WorldIdentifier worldId, LevelChunk chunk, int maxLodLayer) {
        if (worldId == null) return false;
        var instance = VoxyCommon.getInstance();
        if (instance == null) return false;
        if (!instance.isIngestEnabled(worldId)) return false;
        var engine = instance.getOrCreate(worldId);
        if (engine == null) return false;
        return instance.getIngestService().enqueueIngest(engine, chunk, maxLodLayer);
    }

    //Try to automatically ingest the chunk into the correct world
    public static boolean tryAutoIngestChunk(LevelChunk chunk) {
        int maxLodLayer = chunk.getLevel().isClientSide()
                ? WorldEngine.MAX_LOD_LAYER
                : 0;
        return tryIngestChunk(WorldIdentifier.of(chunk.getLevel()), chunk, maxLodLayer);
    }

    public static boolean tryAutoIngestChunk(LevelChunk chunk, int maxLodLayer) {
        return tryIngestChunk(WorldIdentifier.of(chunk.getLevel()), chunk, maxLodLayer);
    }

    private boolean rawIngest0(WorldEngine engine, LevelChunkSection section, int x, int y, int z, DataLayer bl, DataLayer sl) {
        return this.enqueue(new IngestSection(x, y, z, engine, section, bl, sl,
                WorldEngine.MAX_LOD_LAYER));
    }

    public static boolean rawIngest(WorldIdentifier id, LevelChunkSection section, int x, int y, int z, DataLayer bl, DataLayer sl) {
        if (id == null) return false;
        var engine = id.getOrCreateEngine();
        if (engine == null) return false;
        return rawIngest(engine, section, x, y, z, bl, sl);
    }

    public static boolean rawIngest(WorldEngine engine, LevelChunkSection section, int x, int y, int z, DataLayer bl, DataLayer sl) {
        if (!shouldIngestSection(section, x, y, z)) return false;
        if (engine.instanceIn == null) return false;
        if (!engine.instanceIn.isIngestEnabled(null)) return false;//TODO: dont pass in null
        return engine.instanceIn.getIngestService().rawIngest0(engine, section, x, y, z, bl, sl);
    }
}
