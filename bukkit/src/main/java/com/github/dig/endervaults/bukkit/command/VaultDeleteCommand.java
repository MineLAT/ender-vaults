package com.github.dig.endervaults.bukkit.command;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.lang.Language;
import com.github.dig.endervaults.api.permission.UserPermission;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import net.luckperms.api.LuckPermsProvider;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.ConsoleCommandSender;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.UUID;

public class VaultDeleteCommand implements CommandExecutor {

    private final EVBukkitPlugin plugin = (EVBukkitPlugin) VaultPluginProvider.getPlugin();
    private final Language language = plugin.getLanguage();
    private final UserPermission<Player> permission = plugin.getPermission();

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (sender instanceof ConsoleCommandSender || permission.canReload((Player) sender)) {
            if (args.length == 0) {
                sender.sendMessage(ChatColor.RED + "Usage: /pvdelete <player>");
            } else {
                if (args[0].length() < 32) {
                    if (Bukkit.getPluginManager().isPluginEnabled("LuckPerms")) {
                        LuckPermsProvider.get().getUserManager().lookupUniqueId(args[0]).whenComplete((ownerUUID, throwable) -> {
                            if (throwable != null) {
                                throwable.printStackTrace();
                                return;
                            }
                            deleteVaults(sender, args[0], ownerUUID);
                        });
                    } else {
                        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                            deleteVaults(sender, args[0], Bukkit.getOfflinePlayer(args[0]).getUniqueId());
                        });
                    }
                } else {
                    Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                        deleteVaults(sender, args[0], UUID.fromString(args[0]));
                    });
                }
            }
        } else {
            sender.sendMessage(language.get(Lang.NO_PERMISSION));
        }

        return true;
    }

    private void deleteVaults(@NotNull CommandSender sender, @NotNull String player, @NotNull UUID ownerUUID) {
        final int amount = plugin.getDataStorage().delete(ownerUUID);
        String msg = language.get(Lang.ADMIN_VAULT_SELECTOR_TITLE, Map.of(
                "amount", amount,
                "player", player
        ));
        sender.sendMessage(msg);
    }
}
