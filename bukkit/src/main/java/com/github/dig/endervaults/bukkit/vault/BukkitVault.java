package com.github.dig.endervaults.bukkit.vault;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.VaultState;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import com.saicone.rtag.item.ItemData;
import com.saicone.rtag.item.ItemDataFix;
import com.saicone.rtag.item.ItemObject;
import com.saicone.rtag.item.ItemTagStream;
import com.saicone.rtag.stream.TStreamTools;
import com.saicone.rtag.tag.TagBase;
import com.saicone.rtag.tag.TagCompound;
import com.saicone.rtag.tag.TagList;
import com.saicone.rtag.util.MC;
import com.saicone.rtag.util.ServerInstance;
import lombok.extern.java.Log;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Log
public class BukkitVault implements Vault, InventoryHolder {

    private static final Object EMPTY_ITEM = TagCompound.newTag();

    static {
        if (!MC.version().isComponent()) {
            TagCompound.set(EMPTY_ITEM, "id", TagBase.newTag("minecraft:air"));
        }
    }

    @NotNull
    public static BukkitVault create(@NotNull UUID owner, int order) {
        FileConfiguration configuration = (FileConfiguration) VaultPluginProvider.getPlugin().getConfigFile().getConfiguration();

        String title = VaultPluginProvider.getPlugin().getLanguage().get(Lang.VAULT_TITLE, Map.of("order", order));
        int size = configuration.getInt("vault.default-rows", 3) * 9;

        final BukkitVault vault = new BukkitVault(UUID.randomUUID(), title, size, owner, new HashMap<>());
        vault.set(VaultDefaultMetadata.ORDER, order);
        vault.setMetadataState(VaultState.LOADED);

        return vault;
    }

    private final UUID id;
    private final UUID ownerUUID;
    private final Inventory inventory;
    private final Map<String, Object> metadata;

    private transient VaultState contentState = VaultState.UNKNOWN;
    private transient VaultState metadataState = VaultState.UNKNOWN;

    private static final Map<UUID, UUID> WAITING = new HashMap<>();
    private transient CompletableFuture<Void> future;

    public BukkitVault(@NotNull UUID id, @NotNull String title, int size, @NotNull UUID ownerUUID) {
        this(id, title, size, ownerUUID, new HashMap<>());
    }

    public BukkitVault(@NotNull UUID id, @NotNull String title, int size, @NotNull UUID ownerUUID, @NotNull Map<String, Object> metadata) {
        this.id = id;
        this.ownerUUID = ownerUUID;
        this.inventory = Bukkit.createInventory(this, size, title);
        this.metadata = metadata;

        setMetadataState(VaultState.LOADED);
    }

    @ApiStatus.Internal
    public BukkitVault(@NotNull UUID id, @NotNull UUID ownerUUID, @NotNull Inventory inventory, @NotNull Map<String, Object> metadata) {
        this.id = id;
        this.ownerUUID = ownerUUID;
        this.inventory = inventory;
        this.metadata = metadata;

        setContentState(VaultState.MODIFIED);
        setMetadataState(VaultState.MODIFIED);
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
        if (getContentState().isNotValid()) {
            final Integer freeSize = (Integer) metadata.get(VaultDefaultMetadata.FREE_SIZE.getKey());
            return freeSize != null ? freeSize : 0;
        }
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
    public @NotNull String getContent() throws IOException {
        try (ByteArrayOutputStream array = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(array)) {
            Object tagList = TagList.newTag();
            List<Object> list = TagList.getValue(tagList);
            for (ItemStack item : inventory.getContents()) {
                final Object compound;
                if (item != null && item.getType() != Material.AIR) {
                    compound = ItemObject.save(ItemObject.asNMSCopy(item));
                    TagCompound.set(compound, ItemData.VERSION_KEY, TagBase.newTag(MC.version().dataVersion().orElse(98)));
                } else {
                    compound = EMPTY_ITEM;
                }
                list.add(compound);
            }
            TStreamTools.write(tagList, out);
            return new String(Base64.getEncoder().encode(array.toByteArray()));
        }
    }

    @Override
    public @NotNull Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public @NotNull VaultState getContentState() {
        return contentState;
    }

    @Override
    public @NotNull VaultState getMetadataState() {
        return metadataState;
    }

    @Override
    public void setContent(@NotNull String encoded) throws IOException {
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

                // Safe check since 1.20.5 "minecraft:air" no longer exist
                final String id = String.valueOf(TagBase.getValue(value.get("id")));
                if (id.equalsIgnoreCase("minecraft:air") || id.equalsIgnoreCase("air")) {
                    continue;
                }

                try {
                    items[i] = ItemDataFix.safe().decodeItem(compound);
                } catch (Throwable t) {
                    throw new IOException("Cannot decode item: " + compound, t);
                }
            }
        }

        inventory.setContents(items);
    }

    @Override
    public @Nullable <T> Object setMetadata(@NotNull VaultDefaultMetadata<T> meta, @Nullable T value) {
        Object result;
        if (value == null) {
            result = metadata.put(meta.getKey(), Vault.NULL_VALUE);
        } else {
            result = metadata.put(meta.getKey(), value);
        }

        if (result == Vault.NULL_VALUE) {
            result = null;
        }

        if (getMetadataState() != VaultState.MODIFIED && result != value) {
            setMetadataState(VaultState.MODIFIED);
        }

        return result;
    }

    @Override
    public void setContentState(@NotNull VaultState state) {
        this.contentState = state;
    }

    @Override
    public void setMetadataState(@NotNull VaultState state) {
        this.metadataState = state;
    }

    public synchronized void launchFor(Player player) {
        if (getContentState().isValid()) {
            player.openInventory(inventory);
            return;
        } else if (getContentState() == VaultState.ERROR) {
            player.sendMessage(VaultPluginProvider.getPlugin().getLanguage().get(Lang.PLAYER_LOADING_ERROR));
            return;
        }

        if (future == null) {
            future = new CompletableFuture<>();
            Bukkit.getScheduler().runTaskAsynchronously(VaultPluginProvider.<EVBukkitPlugin>getPlugin(), () -> {
                try {
                    VaultPluginProvider.getPlugin().getDataStorage().loadContents(this, false);

                    setContentState(VaultState.LOADED);
                    set(VaultDefaultMetadata.FREE_SIZE, getFreeSize());

                    future.complete(null);
                } catch (Throwable t) {
                    setContentState(VaultState.ERROR);

                    future.completeExceptionally(t);
                    return;
                }

                Bukkit.getScheduler().runTask(VaultPluginProvider.<EVBukkitPlugin>getPlugin(), () -> {
                    final BukkitVault vault = VaultPluginProvider.getPlugin().getRegistry().getHolder(this.getOwner()).<BukkitVault>getVault(this.getId()).orElse(null);
                    // Vault is no longer loaded
                    if (vault != this) {
                        return;
                    }

                    WAITING.entrySet().removeIf(entry -> {
                        if (!entry.getValue().equals(vault.getId()))  {
                            return false;
                        }
                        final Player onlinePlayer = Bukkit.getPlayer(entry.getKey());
                        if (onlinePlayer != null) {
                            vault.launchFor(onlinePlayer);
                        }
                        return true;
                    });
                });
            });
        } else if (future.isCompletedExceptionally()) {
            player.sendMessage(VaultPluginProvider.getPlugin().getLanguage().get(Lang.PLAYER_LOADING_ERROR));
            return;
        }

        if (WAITING.put(player.getUniqueId(), this.getId()) == null) {
            Bukkit.getScheduler().runTaskLaterAsynchronously(VaultPluginProvider.<EVBukkitPlugin>getPlugin(), () -> {
                if (this.getId().equals(WAITING.get(player.getUniqueId()))) {
                    player.sendMessage(VaultPluginProvider.getPlugin().getLanguage().get(Lang.PLAYER_NOT_LOADED));
                }
            }, 100L);
        }
        player.closeInventory();
    }

    public static void stopWaiting(Player player) {
        WAITING.remove(player.getUniqueId());
    }
}
