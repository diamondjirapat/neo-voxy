package me.cortex.voxy.common.config.section;

import me.cortex.voxy.common.config.IMappingStorage;
import me.cortex.voxy.common.world.WorldSection;

public abstract class SectionStorage implements IMappingStorage {
    public abstract int loadSection(WorldSection into);

    public abstract void saveSection(WorldSection section);

    public boolean supportsSectionExistenceChecks() {
        return false;
    }

    public boolean hasSection(long key) {
        throw new UnsupportedOperationException("Section existence checks are not supported");
    }
}
