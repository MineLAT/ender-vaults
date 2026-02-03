package com.github.dig.endervaults.bukkit.command;

import com.github.dig.endervaults.api.EnderVaultsPlugin;
import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.lang.Language;
import com.github.dig.endervaults.api.permission.UserPermission;
import com.github.dig.endervaults.api.vault.VaultHolder;
import com.github.dig.endervaults.api.vault.VaultRegistry;
import com.github.dig.endervaults.api.vault.VaultState;
import com.github.dig.endervaults.bukkit.ui.selector.SelectorInventory;
import com.github.dig.endervaults.bukkit.vault.BukkitVault;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import java.util.Optional;

public class VaultCommand implements CommandExecutor {

    private final EnderVaultsPlugin plugin = VaultPluginProvider.getPlugin();
    private final Language language = plugin.getLanguage();
    private final UserPermission<Player> permission = plugin.getPermission();

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (sender instanceof Player) {
            Player player = (Player) sender;

            if (!permission.canUseVaultCommand(player)) {
                sender.sendMessage(language.get(Lang.NO_PERMISSION));
                return true;
            }

            final VaultHolder holder = plugin.getRegistry().getHolder(player.getUniqueId());
            final VaultState state = holder.getState();
            if (state == VaultState.UNKNOWN) {
                sender.sendMessage(plugin.getLanguage().get(Lang.INVALID_VAULT_STATE));
                return true;
            } else if (state == VaultState.LOADING) {
                sender.sendMessage(language.get(Lang.PLAYER_NOT_LOADED));
                return true;
            } else if (state == VaultState.ERROR) {
                sender.sendMessage(language.get(Lang.PLAYER_LOADING_ERROR));
                return true;
            }

            if (args.length == 1) {
                VaultRegistry registry = plugin.getRegistry();

                int orderValue;
                try {
                    orderValue = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    sender.sendMessage(language.get(Lang.INVALID_VAULT_ORDER));
                    return true;
                }

                if (orderValue <= 0) {
                    sender.sendMessage(language.get(Lang.INVALID_VAULT_ORDER));
                    return true;
                }

                if (!permission.canUseVault(player, orderValue)) {
                    sender.sendMessage(language.get(Lang.NO_PERMISSION));
                    return true;
                }

                Optional<BukkitVault> vaultOptional = holder.getVault(orderValue);

                BukkitVault vault;
                if (vaultOptional.isPresent()) {
                    vault = vaultOptional.get();
                } else {
                    vault = BukkitVault.create(player.getUniqueId(), orderValue);
                    holder.compute(vault);
                }

                vault.launchFor(player);
            } else {
                new SelectorInventory(player.getUniqueId(), 1).launchFor(player);
            }
        }
        return true;
    }
}
