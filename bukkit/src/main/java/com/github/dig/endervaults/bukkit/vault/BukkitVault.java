package com.github.dig.endervaults.bukkit.vault;

import com.github.dig.endervaults.api.util.VaultSerializable;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.bukkit.util.ItemDataFix;
import com.saicone.rtag.item.ItemObject;
import com.saicone.rtag.item.ItemTagStream;
import com.saicone.rtag.stream.TStreamTools;
import com.saicone.rtag.tag.TagBase;
import com.saicone.rtag.tag.TagCompound;
import com.saicone.rtag.tag.TagList;
import com.saicone.rtag.util.ServerInstance;
import lombok.Getter;
import lombok.extern.java.Log;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.logging.Level;

@Log
public class BukkitVault implements Vault, VaultSerializable {

    public static final String DATA_VERSION_KEY = "DataVersion";

    private UUID id;
    private UUID ownerUUID;
    @Getter
    private Inventory inventory;
    private Map<String, Object> metadata = new HashMap<>();

    public BukkitVault(UUID id, String title, int size, UUID ownerUUID) {
        this.id = id;
        this.ownerUUID = ownerUUID;
        this.inventory = Bukkit.createInventory(new BukkitInventoryHolder(this), size, title);
    }

    public BukkitVault(UUID id, String title, int size, UUID ownerUUID, Map<String, Object> metadata) {
        this(id, title, size, ownerUUID);
        this.metadata = metadata;
    }

    public BukkitVault(UUID id, UUID ownerUUID, Inventory inventory, Map<String, Object> metadata) {
        this.id = id;
        this.ownerUUID = ownerUUID;
        this.inventory = inventory;
        this.metadata = metadata;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public UUID getOwner() {
        return ownerUUID;
    }

    @Override
    public int getSize() {
        return inventory.getSize();
    }

    @Override
    public int getFreeSize() {
        int free = 0;
        for (int i = 0; i < inventory.getSize(); i++) {
            ItemStack item = inventory.getItem(i);
            if (item == null || item.getType() == Material.AIR) {
                free++;
            }
        }
        return free;
    }

    @Override
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    @Nullable
    public String encode() {
        try (ByteArrayOutputStream array = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(array)) {
            Object tagList = TagList.newTag();
            List<Object> list = TagList.getValue(tagList);
            for (ItemStack item : inventory.getContents()) {
                final Object compound;
                if (item != null && item.getType() != Material.AIR) {
                    compound = ItemObject.save(ItemObject.asNMSCopy(item));
                    TagCompound.set(compound, DATA_VERSION_KEY, TagBase.newTag(ServerInstance.DATA_VERSION));
                } else {
                    compound = TagCompound.newTag();
                }
                list.add(compound);
            }
            TStreamTools.write(tagList, out);
            return new String(Base64.getEncoder().encode(array.toByteArray()));
        } catch (IOException e) {
            log.log(Level.SEVERE, "[EnderVaults] Unable to encode bukkit vault.", e);
            return null;
        }
    }

    @Override
    public void decode(String encoded) {
        ItemStack[] items = new ItemStack[inventory.getSize()];
        try (ByteArrayInputStream array = new ByteArrayInputStream(Base64.getDecoder().decode(encoded)); DataInputStream in = new DataInputStream(array)) {
            Object tagList = TStreamTools.read(in);
            List<Object> list = TagList.getValue(tagList);
            for (int i = 0; i < list.size() && i < items.length; i++) {
                Object compound = list.get(i);
                final var value = TagCompound.getValue(compound);
                if (compound == null || value.isEmpty()) {
                    continue;
                }

                final String id = String.valueOf(TagBase.getValue(value.get("id")));
                if (id.equalsIgnoreCase("minecraft:air") || id.equalsIgnoreCase("air")) {
                    continue;
                }

                if (ServerInstance.Release.COMPONENT && ServerInstance.Type.MOJANG_MAPPED) {
                    items[i] = ItemDataFix.decodeItem(compound);
                } else {
                    items[i] = ItemTagStream.INSTANCE.fromCompound(compound);
                }
            }
        } catch (IOException e) {
            log.log(Level.SEVERE, "[EnderVaults] Unable to decode bukkit vault.", e);
            return;
        }

        inventory.setContents(items);
    }

    public void launchFor(Player player) {
        player.openInventory(inventory);
    }

    public boolean compare(Inventory inventory) {
        return this.inventory == inventory;
    }
}
