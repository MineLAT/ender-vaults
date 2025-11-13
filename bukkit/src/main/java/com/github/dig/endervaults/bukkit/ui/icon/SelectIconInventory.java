package com.github.dig.endervaults.bukkit.ui.icon;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import com.saicone.rtag.RtagItem;
import lombok.extern.java.Log;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemFlag;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.jetbrains.annotations.NotNull;

import java.util.logging.Level;

@Log
public class SelectIconInventory implements InventoryHolder {

    private final EVBukkitPlugin plugin = (EVBukkitPlugin) VaultPluginProvider.getPlugin();
    private final FileConfiguration configuration = (FileConfiguration) plugin.getConfigFile().getConfiguration();

    private final Vault vault;
    private final Inventory inventory;

    public SelectIconInventory(Vault vault) {
        int size = configuration.getInt("selector.select-icon.rows", 3) * 9;
        this.vault = vault;
        this.inventory = Bukkit.createInventory(this, size,
                plugin.getLanguage().get(Lang.VAULT_SELECT_ICON_TITLE));
        init();
    }

    @NotNull
    @Override
    public Inventory getInventory() {
        return inventory;
    }

    private void init() {
        for (String matName : configuration.getStringList("selector.select-icon.items")) {
            Material material;
            try {
                material = Material.valueOf(matName);
            } catch (IllegalArgumentException e) {
                log.log(Level.SEVERE, "[EnderVaults] Unable to find material " + matName + ", skipping...", e);
                continue;
            }

            ItemStack item = new ItemStack(material, 1);
            ItemMeta meta = item.getItemMeta();
            meta.addItemFlags(ItemFlag.HIDE_ATTRIBUTES,
                    ItemFlag.HIDE_UNBREAKABLE,
                    ItemFlag.HIDE_ENCHANTS,
                    ItemFlag.HIDE_DESTROYS,
                    ItemFlag.values()[5], // HIDE_POTION_EFFECTS
                    ItemFlag.HIDE_PLACED_ON);
            if (material == SelectIconConstants.REMOVE_ICON_MATERIAL) {
                meta.setDisplayName(plugin.getLanguage().get(Lang.VAULT_SELECT_ICON_TITLE));
            }
            item.setItemMeta(meta);

            int slot = inventory.firstEmpty();
            if (slot > -1) {
                inventory.setItem(inventory.firstEmpty(), RtagItem.edit(item, tag -> {
                    tag.set(true, SelectIconConstants.NBT_ICON_ITEM);
                    tag.set(vault.getId(), SelectIconConstants.NBT_ICON_ID);
                    tag.set(vault.getOwner(), SelectIconConstants.NBT_ICON_OWNER_UUID);
                }));
            } else {
                log.log(Level.INFO, "[EnderVaults] Unable to find available spot to put item in, please reconfigure select icon settings.");
            }
        }
    }

    public void launchFor(Player player) {
        player.openInventory(inventory);
    }
}
