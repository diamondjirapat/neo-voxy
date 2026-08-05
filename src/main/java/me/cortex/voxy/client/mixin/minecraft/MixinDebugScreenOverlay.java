package me.cortex.voxy.client.mixin.minecraft;

import me.cortex.voxy.client.core.IGetVoxyRenderSystem;
import me.cortex.voxy.client.core.VoxyRenderSystem;
import me.cortex.voxy.commonImpl.VoxyCommon;
import net.minecraft.ChatFormatting;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.components.DebugScreenOverlay;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.util.ArrayList;
import java.util.List;

@Mixin(DebugScreenOverlay.class)
public class MixinDebugScreenOverlay {
    @Inject(method = "getGameInformation", at = @At("RETURN"))
    private void injectVoxyDebugInfo(CallbackInfoReturnable<List<String>> cir) {
        List<String> list = cir.getReturnValue();
        if (list == null) return;

        if (!VoxyCommon.isAvailable()) {
            list.add(ChatFormatting.RED + "[NeoVoxy] Disabled");
            return;
        }

        var instance = VoxyCommon.getInstance();
        if (instance == null) {
            list.add(ChatFormatting.YELLOW + "[NeoVoxy] Inactive");
            return;
        }

        var wr = Minecraft.getInstance().levelRenderer;
        VoxyRenderSystem vrs = null;
        if (wr != null) {
            vrs = ((IGetVoxyRenderSystem) wr).getVoxyRenderSystem();
        }

        list.add((vrs != null ? ChatFormatting.GREEN : ChatFormatting.DARK_GREEN) + "[NeoVoxy] " + VoxyCommon.MOD_VERSION);

        List<String> instanceLines = new ArrayList<>();
        instance.addDebug(instanceLines);
        for (String line : instanceLines) {
            list.add(ChatFormatting.AQUA + " [Instance] " + line);
        }

        if (vrs != null) {
            List<String> renderLines = new ArrayList<>();
            vrs.addDebugInfo(renderLines);
            for (String line : renderLines) {
                list.add(ChatFormatting.GREEN + " [Render] " + line);
            }
        }
    }
}
