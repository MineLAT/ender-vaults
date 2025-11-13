package com.github.dig.endervaults.bukkit.command;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.lang.Language;
import com.github.dig.endervaults.api.permission.UserPermission;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import com.github.dig.endervaults.bukkit.ui.selector.SelectorInventory;
import com.github.dig.endervaults.bukkit.util.PlayerLookup;
import com.github.dig.endervaults.bukkit.vault.BukkitVault;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class VaultAdminCommand implements CommandExecutor {

    private final EVBukkitPlugin plugin = (EVBukkitPlugin) VaultPluginProvider.getPlugin();
    private final Language language = plugin.getLanguage();
    private final UserPermission<Player> permission = plugin.getPermission();

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (sender instanceof Player) {
            Player player = (Player) sender;
            if (permission.isVaultAdmin(player)) {
                if (args.length == 0) {
                    sender.sendMessage(ChatColor.RED + "Usage: /pvadmin <name> [vault]");
                } else {
                    final CompletableFuture<PlayerLookup.Context> future = PlayerLookup.lookup(args[0]);
                    if (future.isDone()) {
                        // The target is online
                        future.whenComplete((target, throwable) -> run(player, target, throwable, args));
                    } else {
                        // The target is offline
                        future.whenCompleteAsync((target, throwable) -> run(player, target, throwable, args), runnable -> Bukkit.getScheduler().runTaskAsynchronously(plugin, runnable));
                    }
                }
            } else {
                sender.sendMessage(language.get(Lang.NO_PERMISSION));
            }
        }
        return true;
    }

    private void run(@NotNull Player player, @Nullable PlayerLookup.Context target, @Nullable Throwable throwable, @NotNull String[] args) {
        if (target == null || throwable != null) {
            player.sendMessage(language.get(Lang.PLAYER_NOT_FOUND));
            return;
        }
        if (!permission.isAdminImmune(player) && target.hasPermission("endervaults.admin.immune")) {
            player.sendMessage(language.get(Lang.NO_PERMISSION));
            return;
        }

        if (args.length == 1) {
            // TODO: Add compatibility with offline player selector
            if (!target.isOnline()) {
                player.sendMessage("§cCannot see offline player vault selector. Consider using §6/pvadmin <name> <vault> §cfor individual vault view");
                return;
            }
            String title = language.get(Lang.ADMIN_VAULT_SELECTOR_TITLE, Map.of("player", target.getName()));
            new SelectorInventory(target.getUniqueId(), 1, title).launchFor(player);
            return;
        }

        int vaultOrder;
        try {
            vaultOrder = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            player.sendMessage(language.get(Lang.INVALID_VAULT_ORDER));
            return;
        }

        final Optional<Vault> result;
        if (target.isOnline()) {
            result = plugin.getRegistry().getByMetadata(target.getUniqueId(), VaultDefaultMetadata.ORDER.getKey(), vaultOrder);
        } else {
            result = plugin.getDataStorage().load(target.getUniqueId(), VaultDefaultMetadata.ORDER, vaultOrder);
        }
        result.ifPresent(vault -> {
            if (Bukkit.isPrimaryThread()) {
                ((BukkitVault) vault).launchFor(player);
            } else {
                Bukkit.getScheduler().runTask(plugin, () -> ((BukkitVault) vault).launchFor(player));
            }
        });
    }
}
