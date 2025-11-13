package com.github.dig.endervaults.bukkit.ui.icon;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.vault.VaultRegistry;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import com.github.dig.endervaults.bukkit.ui.selector.SelectorInventory;
import com.saicone.rtag.RtagItem;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryMoveItemEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;

import java.util.UUID;

public class SelectIconListener implements Listener {

    private final VaultRegistry registry = VaultPluginProvider.getPlugin().getRegistry();

    @EventHandler(priority = EventPriority.HIGH)
    public void onClick(InventoryClickEvent event) {
        Player player = (Player) event.getWhoClicked();
        Inventory inventory = event.getClickedInventory();
        ItemStack item = event.getCurrentItem();

        if (inventory != null && item != null && item.getType() != Material.AIR) {
            RtagItem tag = RtagItem.of(item);
            if (tag.hasTag(SelectIconConstants.NBT_ICON_ITEM) && tag.hasTag(SelectIconConstants.NBT_ICON_ID)) {
                event.setCancelled(true);
                UUID vaultID = tag.getOptional(SelectIconConstants.NBT_ICON_ID).asUuid();
                UUID vaultOwnerUUID = tag.getOptional(SelectIconConstants.NBT_ICON_OWNER_UUID).asUuid();

                registry.get(vaultOwnerUUID, vaultID).ifPresent(vault -> {
                    if (item.getType() == SelectIconConstants.REMOVE_ICON_MATERIAL) {
                        vault.getMetadata().remove(VaultDefaultMetadata.ICON.getKey());
                    } else {
                        vault.getMetadata().put(VaultDefaultMetadata.ICON.getKey(), item.getType().toString());
                    }
                });
                new SelectorInventory(vaultOwnerUUID, 1).launchFor(player);
            }
        }
    }

    @EventHandler(priority = EventPriority.HIGH)
    public void onMove(InventoryMoveItemEvent event) {
        ItemStack item = event.getItem();
        if (item != null && item.getType() != Material.AIR) {
            if (RtagItem.of(item).hasTag(SelectIconConstants.NBT_ICON_ITEM)) {
                event.setCancelled(true);
            }
        }
    }
}
