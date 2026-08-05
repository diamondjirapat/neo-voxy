package me.cortex.voxy.client.config;

import me.cortex.voxy.client.RenderStatistics;
import me.cortex.voxy.client.VoxyClientInstance;
import me.cortex.voxy.client.core.IGetVoxyRenderSystem;
import me.cortex.voxy.common.util.cpu.CpuLayout;
import me.cortex.voxy.commonImpl.VoxyCommon;
import net.caffeinemc.mods.sodium.api.config.ConfigEntryPoint;
import net.caffeinemc.mods.sodium.api.config.StorageEventHandler;
import net.caffeinemc.mods.sodium.api.config.option.OptionFlag;
import net.caffeinemc.mods.sodium.api.config.option.OptionImpact;
import net.caffeinemc.mods.sodium.api.config.structure.*;
import net.minecraft.client.Minecraft;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.ResourceLocation;

public class VoxySodiumConfigEntryPoint implements ConfigEntryPoint {

    @Override
    public void registerConfigLate(ConfigBuilder builder) {
        VoxyConfig storage = VoxyConfig.CONFIG;
        StorageEventHandler saveHandler = storage::save;

        ModOptionsBuilder modOptions = builder.registerModOptions("neovoxy", "Voxy", "0.7.5");
        OptionPageBuilder pageBuilder = builder.createOptionPage();
        pageBuilder.setName(Component.translatable("voxy.config.title"));

        // Group 1: General Enable
        OptionGroupBuilder group1 = builder.createOptionGroup();
        BooleanOptionBuilder enabledOpt = builder.createBooleanOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "enabled"))
                .setName(Component.translatable("voxy.config.general.enabled"))
                .setTooltip(Component.translatable("voxy.config.general.enabled.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue(true)
                .setBinding(v -> {
                    storage.enabled = v;
                    if (v) {
                        if (VoxyClientInstance.isInGame) {
                            VoxyCommon.createInstance();
                            var vrsh = (IGetVoxyRenderSystem) Minecraft.getInstance().levelRenderer;
                            if (vrsh != null && storage.enableRendering) {
                                vrsh.createRenderer();
                            }
                        }
                    } else {
                        var vrsh = (IGetVoxyRenderSystem) Minecraft.getInstance().levelRenderer;
                        if (vrsh != null) {
                            vrsh.shutdownRenderer();
                        }
                        VoxyCommon.shutdownInstance();
                    }
                }, () -> storage.enabled)
                .setFlags(OptionFlag.REQUIRES_RENDERER_RELOAD);
        group1.addOption(enabledOpt);
        pageBuilder.addOptionGroup(group1);

        // Group 2: Service Threads & Options
        OptionGroupBuilder group2 = builder.createOptionGroup();
        IntegerOptionBuilder serviceThreadsOpt = builder.createIntegerOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "service_threads"))
                .setName(Component.translatable("voxy.config.general.serviceThreads"))
                .setTooltip(Component.translatable("voxy.config.general.serviceThreads.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue((int) Math.max(CpuLayout.getCoreCount() / 1.5, 1))
                .setValueFormatter(v -> Component.literal(Integer.toString(v)))
                .setRange(1, CpuLayout.getCoreCount(), 1)
                .setBinding(v -> {
                    storage.serviceThreads = v;
                    var instance = VoxyCommon.getInstance();
                    if (instance != null) {
                        instance.updateDedicatedThreads();
                    }
                }, () -> storage.serviceThreads)
                .setImpact(OptionImpact.HIGH);
        group2.addOption(serviceThreadsOpt);

        BooleanOptionBuilder useSodiumBuilderOpt = builder.createBooleanOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "use_sodium_builder"))
                .setName(Component.translatable("voxy.config.general.useSodiumBuilder"))
                .setTooltip(Component.translatable("voxy.config.general.useSodiumBuilder.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue(true)
                .setImpact(OptionImpact.VARIES)
                .setFlags(OptionFlag.REQUIRES_RENDERER_RELOAD)
                .setBinding(v -> {
                    storage.dontUseSodiumBuilderThreads = !v;
                    var instance = VoxyCommon.getInstance();
                    if (instance != null) {
                        instance.updateDedicatedThreads();
                    }
                }, () -> !storage.dontUseSodiumBuilderThreads);
        group2.addOption(useSodiumBuilderOpt);

        BooleanOptionBuilder ingestOpt = builder.createBooleanOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "ingest"))
                .setName(Component.translatable("voxy.config.general.ingest"))
                .setTooltip(Component.translatable("voxy.config.general.ingest.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue(true)
                .setBinding(v -> storage.ingestEnabled = v, () -> storage.ingestEnabled)
                .setImpact(OptionImpact.MEDIUM);
        group2.addOption(ingestOpt);
        pageBuilder.addOptionGroup(group2);

        // Group 3: Rendering
        OptionGroupBuilder group3 = builder.createOptionGroup();
        BooleanOptionBuilder renderingOpt = builder.createBooleanOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "rendering"))
                .setName(Component.translatable("voxy.config.general.rendering"))
                .setTooltip(Component.translatable("voxy.config.general.rendering.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue(true)
                .setBinding(v -> {
                    storage.enableRendering = v;
                    var vrsh = (IGetVoxyRenderSystem) Minecraft.getInstance().levelRenderer;
                    if (vrsh != null) {
                        if (v) {
                            vrsh.createRenderer();
                        } else {
                            vrsh.shutdownRenderer();
                        }
                    }
                }, () -> storage.enableRendering)
                .setImpact(OptionImpact.HIGH);
        group3.addOption(renderingOpt);

        IntegerOptionBuilder renderDistanceOpt = builder.createIntegerOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "render_distance"))
                .setName(Component.translatable("voxy.config.general.renderDistance"))
                .setTooltip(Component.translatable("voxy.config.general.renderDistance.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue(16)
                .setRange(2, 64, 1)
                .setValueFormatter(v -> Component.literal(Integer.toString(v * 32)))
                .setBinding(v -> {
                    storage.sectionRenderDistance = v;
                    var vrsh = (IGetVoxyRenderSystem) Minecraft.getInstance().levelRenderer;
                    if (vrsh != null) {
                        var vrs = vrsh.getVoxyRenderSystem();
                        if (vrs != null) {
                            vrs.setRenderDistance(v);
                            var receptionService = vrs.getLodReceptionService();
                            if (receptionService != null) {
                                receptionService.requestSync();
                            }
                        }
                    }
                }, () -> storage.sectionRenderDistance)
                .setImpact(OptionImpact.LOW);
        group3.addOption(renderDistanceOpt);

        BooleanOptionBuilder fogOpt = builder.createBooleanOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "vanilla_fog"))
                .setName(Component.translatable("voxy.config.general.vanilla_fog"))
                .setTooltip(Component.translatable("voxy.config.general.vanilla_fog.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue(false)
                .setBinding(v -> storage.renderVanillaFog = v, () -> storage.renderVanillaFog);
        group3.addOption(fogOpt);

        BooleanOptionBuilder statsOpt = builder.createBooleanOption(ResourceLocation.fromNamespaceAndPath("neovoxy", "render_statistics"))
                .setName(Component.translatable("voxy.config.general.render_statistics"))
                .setTooltip(Component.translatable("voxy.config.general.render_statistics.tooltip"))
                .setStorageHandler(saveHandler)
                .setDefaultValue(false)
                .setBinding(v -> RenderStatistics.enabled = v, () -> RenderStatistics.enabled)
                .setFlags(OptionFlag.REQUIRES_RENDERER_RELOAD);
        group3.addOption(statsOpt);
        pageBuilder.addOptionGroup(group3);

        modOptions.addPage(pageBuilder);
    }
}
