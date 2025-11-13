package com.github.dig.endervaults.bukkit;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.permission.UserPermission;
import com.github.dig.endervaults.api.vault.VaultPersister;
import com.github.dig.endervaults.bukkit.ui.selector.SelectorInventory;
import com.github.dig.endervaults.bukkit.vault.BukkitVault;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.event.inventory.InventoryMoveItemEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.scheduler.BukkitTask;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class BukkitListener implements Listener {

    private final EVBukkitPlugin plugin = (EVBukkitPlugin) VaultPluginProvider.getPlugin();
    private final VaultPersister persister = plugin.getPersister();
    private final UserPermission<Player> permission = plugin.getPermission();

    private final Map<UUID, BukkitTask> pendingLoadMap = new HashMap<>();

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onJoin(PlayerJoinEvent event) {
        Player player = event.getPlayer();
        FileConfiguration config = plugin.getConfigFile().getConfiguration();
        BukkitTask bukkitTask = Bukkit.getScheduler().runTaskLaterAsynchronously(plugin,
                () -> persister.load(player.getUniqueId()),
                config.getLong("storage.settings.load-delay", 5 * 20));
        pendingLoadMap.put(player.getUniqueId(), bukkitTask);
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        if (pendingLoadMap.containsKey(player.getUniqueId())) {
            pendingLoadMap.remove(player.getUniqueId()).cancel();
        }
        Bukkit.getScheduler().runTaskAsynchronously(plugin,
                () -> persister.save(player.getUniqueId()));
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onClick(InventoryClickEvent event) {
        final Player player = (Player) event.getWhoClicked();
        final ItemStack item = event.getCurrentItem();
        final Inventory inventory = event.getInventory();

        if (inventory.getHolder() instanceof BukkitVault) {
            final BukkitVault vault = (BukkitVault) inventory;
            if (item != null && isBlacklistEnabled() && !permission.canBypassBlacklist(player) && getBlacklisted().contains(item.getType())) {
                player.sendMessage(plugin.getLanguage().get(Lang.BLACKLISTED_ITEM));
                event.setCancelled(true);
                return;
            }
            vault.setModified(true);
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onMove(InventoryMoveItemEvent event) {
        final ItemStack item = event.getItem();
        final Inventory inventory = event.getDestination();

        if (inventory.getHolder() instanceof BukkitVault) {
            final BukkitVault vault = (BukkitVault) inventory;
            if (isBlacklistEnabled() && getBlacklisted().contains(item.getType())) {
                event.setCancelled(true);
                return;
            }
            vault.setModified(true);
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onDrag(InventoryDragEvent event) {
        final Player player = (Player) event.getWhoClicked();
        final ItemStack item = event.getCursor();
        final Inventory inventory = event.getInventory();

        if (inventory.getHolder() instanceof BukkitVault) {
            final BukkitVault vault = (BukkitVault) inventory;
            if (item != null && isBlacklistEnabled() && !permission.canBypassBlacklist(player) && getBlacklisted().contains(item.getType())) {
                event.setCancelled(true);
                return;
            }
            vault.setModified(true);
        }
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onClose(InventoryCloseEvent event) {
        if (event.getInventory().getHolder() instanceof BukkitVault) {
            final BukkitVault vault = (BukkitVault) event.getInventory().getHolder();
            if (vault.isModified()) {
                Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                    try {
                        plugin.getDataStorage().save(vault);
                        vault.setModified(false);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @EventHandler
    public void onInteract(PlayerInteractEvent event) {
        Player player = event.getPlayer();
        Block block = event.getClickedBlock();

        if (event.getAction() == Action.RIGHT_CLICK_BLOCK && block.getType() == Material.ENDER_CHEST && isEnderchestReplaced()) {
            event.setCancelled(true);
            final VaultPersister.State state = plugin.getPersister().getState(player.getUniqueId());
            if (state == VaultPersister.State.UNKNOWN) {
                player.sendMessage("§4Your vaults state is unknown, try again or contact an administrator");
                return;
            } else if (state == VaultPersister.State.LOADING) {
                player.sendMessage(plugin.getLanguage().get(Lang.PLAYER_NOT_LOADED));
                return;
            } else if (state == VaultPersister.State.ERROR) {
                player.sendMessage(plugin.getLanguage().get(Lang.PLAYER_LOADING_ERROR));
                return;
            }
            new SelectorInventory(player.getUniqueId(), 1).launchFor(player);
        }
    }

    private boolean isEnderchestReplaced() {
        FileConfiguration configuration = (FileConfiguration) plugin.getConfigFile().getConfiguration();
        return configuration.getBoolean("enderchest.replace-with-selector", false);
    }

    private boolean isBlacklistEnabled() {
        FileConfiguration configuration = (FileConfiguration) plugin.getConfigFile().getConfiguration();
        return configuration.getBoolean("vault.blacklist.enabled", false);
    }

    private Set<Material> getBlacklisted() {
        FileConfiguration configuration = (FileConfiguration) plugin.getConfigFile().getConfiguration();
        return configuration.getStringList("vault.blacklist.items")
                .stream()
                .map(Material::valueOf)
                .collect(Collectors.toSet());
    }
}
