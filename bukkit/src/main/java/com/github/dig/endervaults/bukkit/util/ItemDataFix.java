package com.github.dig.endervaults.bukkit.util;

import com.google.gson.JsonParser;
import com.mojang.serialization.Dynamic;
import com.saicone.rtag.Rtag;
import com.saicone.rtag.item.ItemData;
import com.saicone.rtag.item.ItemObject;
import com.saicone.rtag.tag.TagBase;
import com.saicone.rtag.tag.TagCompound;
import com.saicone.rtag.tag.TagList;
import com.saicone.rtag.util.ChatComponent;
import com.saicone.rtag.util.MC;
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

    private static Object fixItem(@NotNull Object compound) {
        // Fix enchantments with invalid levels
        final Object enchantments;
        if (MC.version().isNewerThanOrEquals(MC.V_1_21_5)) {
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

        // Fix sub-items
        final Object container = Rtag.INSTANCE.getExact(compound, "components", "minecraft:container");
        if (container != null) {
            for (Object element : TagList.getValue(container)) {
                for (Map.Entry<String, Object> entry : TagCompound.getValue(element).entrySet()) {
                    if (entry.getKey().equals("item")) {
                        fixItem(entry.getValue());
                    }
                }
            }
        }

        return compound;
    }

    private static ItemStack fixItem(@NotNull ItemStack item) {
        if (item.hasItemMeta()) {
            final ItemMeta meta = item.getItemMeta();

            // Fix rare item json lore
            // TODO: Use adventure library if applicable to avoid incompatibilities with shadow_color
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
        final MC version = ItemData.lookupVersion(compound);
        // Update item version
        compound = updateItem(compound, version, MC.version());
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

    public static Object updateItem(@NotNull Object compound, @NotNull MC version, @NotNull MC newVersion) {
        if (version == newVersion) {
            return compound;
        }
        return MinecraftServer.getServer().getFixerUpper().update(References.ITEM_STACK, new Dynamic<>(NbtOps.INSTANCE, (Tag) compound), version.dataVersion().get(), newVersion.dataVersion().get()).getValue();
    }
}
