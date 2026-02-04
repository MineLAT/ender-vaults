package com.github.dig.endervaults.bukkit.vault;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.storage.DataStorage;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.VaultHolder;
import com.github.dig.endervaults.api.vault.VaultRegistry;
import com.github.dig.endervaults.api.vault.VaultState;
import com.github.dig.endervaults.api.vault.exception.VaultOrderException;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitTask;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

public class BukkitVaultRegistry implements VaultRegistry {

    private final EVBukkitPlugin plugin = VaultPluginProvider.getPlugin();
    private final DataStorage dataStorage = plugin.getDataStorage();
    private final Map<UUID, VaultHolder> holders = new HashMap<>();

    private final Map<UUID, BukkitTask> tasks = new HashMap<>();

    @Override
    public @NotNull VaultHolder getHolder(@NotNull UUID owner) {
        return holders.computeIfAbsent(owner, VaultHolder::new);
    }

    @Override
    public synchronized void load(@NotNull UUID owner) {
        BukkitTask task = tasks.remove(owner);
        if (task != null) {
            task.cancel();
        }

        final VaultHolder holder = getHolder(owner);
        holder.setState(VaultState.LOADING);

        final Runnable runnable = () -> {
            try {
                for (Vault vault : dataStorage.load(owner)) {
                    try {
                        holder.compute(vault);
                    } catch (VaultOrderException e) {
                        plugin.getLogger().log(Level.WARNING, "Exception while loading player " + owner, e);
                        final Player player = Bukkit.getPlayer(owner);
                        if (player != null) {
                            player.sendMessage(plugin.getLanguage().get(Lang.INVALID_VAULT_MULTIPLE, Map.of("order", e.getOrder())));
                        }
                    }
                }
                holder.setState(VaultState.LOADED);
            } catch (Throwable t) {
                holder.setState(VaultState.ERROR);
                plugin.getLogger().log(Level.SEVERE, "Exception while loading player " + owner, t);
            }
        };
        final long delay = plugin.getConfigFile().getConfiguration().getLong("storage.settings.load-delay", 5 * 20);
        if (delay > 0) {
            task = Bukkit.getScheduler().runTaskLaterAsynchronously(plugin, runnable, delay);
        } else {
            task = Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable);
        }
        tasks.put(owner, task);
    }

    @Override
    public void unload(@NotNull UUID owner) {
        BukkitTask task = tasks.remove(owner);
        if (task != null) {
            task.cancel();
        }

        final VaultHolder holder = holders.get(owner);
        if (holder != null) {
            holder.setState(VaultState.UNKNOWN);
            final Map<UUID, Vault> vaults = holder.clear();
            Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                for (Map.Entry<UUID, Vault> entry : vaults.entrySet()) {
                    final Vault vault = entry.getValue();
                    if (vault.meet(VaultState.MODIFIED)) {
                        try {
                            dataStorage.save(vault);
                        } catch (Throwable t) {
                            plugin.getLogger().log(Level.SEVERE, "Cannot save vault " + vault.getId() + " from " + owner, t);
                        }
                    }
                }
            });
        }
    }
}
