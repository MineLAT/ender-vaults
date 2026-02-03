package com.github.dig.endervaults.api;

import com.github.dig.endervaults.api.exception.PluginAlreadySetException;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@UtilityClass
public class VaultPluginProvider {

    private static EnderVaultsPlugin plugin = null;

    @NotNull
    @SuppressWarnings("unchecked")
    public <T extends EnderVaultsPlugin> T getPlugin() {
        return (T) plugin;
    }

    @ApiStatus.Internal
    public void set(EnderVaultsPlugin instance) throws PluginAlreadySetException {
        if (plugin == null) {
            plugin = instance;
            return;
        }
        throw new PluginAlreadySetException("Plugin instance already set.");
    }
}
