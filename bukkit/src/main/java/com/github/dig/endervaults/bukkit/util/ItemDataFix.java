package com.github.dig.endervaults.bukkit.util;

import com.github.dig.endervaults.bukkit.vault.BukkitVault;
import com.google.gson.JsonParser;
import com.mojang.serialization.Dynamic;
import com.saicone.rtag.Rtag;
import com.saicone.rtag.item.ItemData;
import com.saicone.rtag.item.ItemObject;
import com.saicone.rtag.tag.TagBase;
import com.saicone.rtag.tag.TagCompound;
import com.saicone.rtag.util.ChatComponent;
import com.saicone.rtag.util.ServerInstance;
import net.minecraft.nbt.NbtOps;
import net.minecraft.nbt.Tag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.util.datafix.fixes.References;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

public class ItemDataFix {

    @SuppressWarnings("deprecation")
    private static final JsonParser JSON_PARSER = new JsonParser();

    ItemDataFix() {
    }

    private static int itemVersion(@NotNull Object compound) {
        final Object version = TagBase.getValue(TagCompound.get(compound, BukkitVault.DATA_VERSION_KEY));
        if (version instanceof Number) {
            return ((Number) version).intValue();
        }
        final Float itemVersion = ItemData.getItemVersion(compound);
        if (itemVersion == null) {
            throw new IllegalArgumentException("Cannot lookup item version from = " + compound);
        }
        return dataVersion(itemVersion);
    }

    private static int dataVersion(float number) {
        int integralPart = (int) number;
        int decimalPart = (int) (number - integralPart);

        String resultStr = String.format("1%02d%02d", integralPart, decimalPart);
        return ServerInstance.dataVersion(Integer.parseInt(resultStr));
    }

    private static Object fixItem(@NotNull Object compound) {
        // Fix enchantments with invalid levels
        final Object enchantments;
        if (ServerInstance.VERSION >= 21.04f) { // 1.21.5
            enchantments = Rtag.INSTANCE.getExact(compound, "components", "minecraft:enchantments");
        } else {
            enchantments = Rtag.INSTANCE.getExact(compound, "components", "minecraft:enchantments", "levels");
        }
        if (enchantments != null) {
            for (Map.Entry<String, Object> entry : TagCompound.getValue(enchantments).entrySet()) {
                final Number level = (Number) TagBase.getValue(entry.getValue());
                if (level.intValue() < 1) {
                    entry.setValue(TagBase.newTag(1));
                }
            }
        }

        return compound;
    }

    private static ItemStack fixItem(@NotNull ItemStack item) {
        if (item.hasItemMeta()) {
            final ItemMeta meta = item.getItemMeta();

            // Fix rare item json lore
            if (meta.hasLore()) {
                final List<String> lore = meta.getLore();
                boolean modified = false;
                for (int i = 0; i < lore.size(); i++) {
                    final String line = lore.get(i);
                    try {
                        JSON_PARSER.parse(line);
                        lore.set(i, ChatComponent.toString(line));
                        modified = true;
                    } catch (Throwable ignored) { }
                }
                if (modified) {
                    meta.setLore(lore);
                    item.setItemMeta(meta);
                }
            }
        }

        return item;
    }

    public static ItemStack decodeItem(@NotNull Object compound) {
        final int version = itemVersion(compound);
        // Update item version
        compound = updateItem(compound, version, ServerInstance.DATA_VERSION);
        // Fix result compound
        compound = fixItem(compound);

        // Parse item
        final Object item = ItemObject.newItem(compound);
        if (item == null) {
            throw new IllegalArgumentException("Cannot decode item: " + compound);
        }

        // Fix result item
        return fixItem(ItemObject.asCraftMirror(item));
    }

    public static Object updateItem(@NotNull Object compound, int version, int newVersion) {
        if (version == newVersion) {
            return compound;
        }
        return MinecraftServer.getServer().getFixerUpper().update(References.ITEM_STACK, new Dynamic<>(NbtOps.INSTANCE, (Tag) compound), version, newVersion).getValue();
    }
}
