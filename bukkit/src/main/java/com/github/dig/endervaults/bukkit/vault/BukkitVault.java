package com.github.dig.endervaults.bukkit.vault;

import com.github.dig.endervaults.api.util.VaultSerializable;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import com.github.dig.endervaults.bukkit.util.ItemDataFix;
import com.saicone.rtag.item.ItemObject;
import com.saicone.rtag.item.ItemTagStream;
import com.saicone.rtag.stream.TStreamTools;
import com.saicone.rtag.tag.TagBase;
import com.saicone.rtag.tag.TagCompound;
import com.saicone.rtag.tag.TagList;
import com.saicone.rtag.util.ServerInstance;
import lombok.extern.java.Log;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.logging.Level;

@Log
public class BukkitVault implements Vault, VaultSerializable, InventoryHolder {

    public static final String DATA_VERSION_KEY = "DataVersion";
    private static final Object EMPTY_ITEM = TagCompound.newTag();

    static {
        if (!ServerInstance.Release.COMPONENT) {
            TagCompound.set(EMPTY_ITEM, "id", TagBase.newTag("minecraft:air"));
        }
    }

    private final UUID id;
    private final UUID ownerUUID;
    private final Inventory inventory;
    private final Map<String, Object> metadata;

    private transient boolean modified = false;

    public BukkitVault(@NotNull UUID id, @NotNull String title, int size, @NotNull UUID ownerUUID) {
        this(id, title, size, ownerUUID, new HashMap<>());
    }

    public BukkitVault(@NotNull UUID id, @NotNull String title, int size, @NotNull UUID ownerUUID, @NotNull Map<String, Object> metadata) {
        this.id = id;
        this.ownerUUID = ownerUUID;
        this.inventory = Bukkit.createInventory(this, size, title);
        this.metadata = metadata;
    }

    @ApiStatus.Internal
    public BukkitVault(@NotNull UUID id, @NotNull UUID ownerUUID, @NotNull Inventory inventory, @NotNull Map<String, Object> metadata) {
        this.id = id;
        this.ownerUUID = ownerUUID;
        this.inventory = inventory;
        this.metadata = metadata;
    }

    @Override
    public boolean isModified() {
        return modified;
    }

    @Override
    public @NotNull UUID getId() {
        return id;
    }

    @Override
    public @NotNull UUID getOwner() {
        return ownerUUID;
    }

    @Override
    public @NotNull Inventory getInventory() {
        return inventory;
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
    public @NotNull Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public <T> void set(@NotNull VaultDefaultMetadata<T> meta, @Nullable T value) {
        setModified(true);
        if (value == null) {
            metadata.put(meta.getKey(), Vault.NULL_VALUE);
        } else {
            metadata.put(meta.getKey(), value);
        }
    }

    public void setModified(boolean modified) {
        this.modified = modified;
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
                    compound = EMPTY_ITEM;
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
}
