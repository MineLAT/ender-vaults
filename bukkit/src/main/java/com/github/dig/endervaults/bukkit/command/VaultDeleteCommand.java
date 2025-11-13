package com.github.dig.endervaults.bukkit.command;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.lang.Language;
import com.github.dig.endervaults.api.permission.UserPermission;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import com.github.dig.endervaults.bukkit.util.PlayerLookup;
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
                PlayerLookup.lookup(args[0]).whenComplete((target, throwable) -> {
                    if (throwable != null) {
                        throwable.printStackTrace();
                        return;
                    }
                    Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                        deleteVaults(sender, target.getName(), target.getUniqueId());
                    });
                });
            }
        } else {
            sender.sendMessage(language.get(Lang.NO_PERMISSION));
        }

        return true;
    }

    private void deleteVaults(@NotNull CommandSender sender, @NotNull String name, @NotNull UUID ownerUUID) {
        final int amount = plugin.getDataStorage().delete(ownerUUID);
        String msg = language.get(Lang.ADMIN_VAULT_SELECTOR_TITLE, Map.of(
                "amount", amount,
                "player", name
        ));
        sender.sendMessage(msg);
    }
}
