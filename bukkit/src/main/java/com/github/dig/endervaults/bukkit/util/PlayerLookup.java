package com.github.dig.endervaults.bukkit.util;

import net.luckperms.api.LuckPermsProvider;
import net.luckperms.api.model.user.User;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class PlayerLookup {

    private static final Supplier<Boolean> USE_LUCKPERMS = new Supplier<>() {
        private Boolean result;

        @Override
        public Boolean get() {
            if (result == null) {
                result = Bukkit.getPluginManager().isPluginEnabled("LuckPerms");
            }
            return result;
        }
    };

    @NotNull
    public static CompletableFuture<Context> lookup(@NotNull String s) {
        try {
            final UUID uniqueId = UUID.fromString(s);
            final Player player = Bukkit.getPlayer(uniqueId);
            if (player != null) {
                return CompletableFuture.completedFuture(new OnlineContext(player));
            } else {
                return lookupName(uniqueId).thenApply(name -> new OfflineContext(uniqueId, name));
            }
        } catch (IllegalArgumentException e) {
            final Player player = Bukkit.getPlayerExact(s);
            if (player != null) {
                return CompletableFuture.completedFuture(new OnlineContext(player));
            } else {
                return lookupUniqueId(s).thenApply(uniqueId -> new OfflineContext(uniqueId, s));
            }
        }
    }

    @NotNull
    @SuppressWarnings("deprecation")
    public static CompletableFuture<UUID> lookupUniqueId(@NotNull String name) {
        try {
            final UUID uniqueId = UUID.fromString(name);
            return CompletableFuture.completedFuture(uniqueId);
        } catch (Throwable ignored) { }

        final Player player = Bukkit.getPlayerExact(name);
        if (player != null) {
            return CompletableFuture.completedFuture(player.getUniqueId());
        } else if (USE_LUCKPERMS.get()) {
            return LuckPermsProvider.get().getUserManager().lookupUniqueId(name);
        } else {
            return CompletableFuture.supplyAsync(() -> Bukkit.getOfflinePlayer(name).getUniqueId());
        }
    }

    @NotNull
    public static CompletableFuture<String> lookupName(@NotNull UUID uniqueId) {
        final Player player = Bukkit.getPlayer(uniqueId);
        if (player != null) {
            return CompletableFuture.completedFuture(player.getName());
        } else if (USE_LUCKPERMS.get()) {
            return LuckPermsProvider.get().getUserManager().lookupUsername(uniqueId);
        } else {
            return CompletableFuture.supplyAsync(() -> Bukkit.getOfflinePlayer(uniqueId).getName());
        }
    }

    public interface Context {

        boolean isOnline();

        boolean hasPermission(@NotNull String name);

        @NotNull
        UUID getUniqueId();

        @NotNull
        String getName();

        Player getPlayer();
    }

    public static class OnlineContext implements Context {

        private final Player player;

        public OnlineContext(@NotNull Player player) {
            this.player = player;
        }

        @Override
        public boolean isOnline() {
            return true;
        }

        @Override
        public boolean hasPermission(@NotNull String name) {
            return player.hasPermission(name);
        }

        @NotNull
        @Override
        public UUID getUniqueId() {
            return player.getUniqueId();
        }

        @NotNull
        @Override
        public String getName() {
            return player.getName();
        }

        @Override
        public Player getPlayer() {
            return player;
        }
    }

    public static class OfflineContext implements Context {

        private final UUID uniqueId;
        private final String name;

        private transient Object user;

        public OfflineContext(@NotNull UUID uniqueId, @NotNull String name) {
            this.uniqueId = uniqueId;
            this.name = name;
        }

        @Override
        public boolean isOnline() {
            return false;
        }

        @Override
        public boolean hasPermission(@NotNull String name) {
            if (USE_LUCKPERMS.get()) {
                if (user == null) {
                    user = LuckPermsProvider.get().getUserManager().loadUser(uniqueId).join();
                }
                return ((User) user).getCachedData().getPermissionData().checkPermission(name).asBoolean();
            } else {
                throw new IllegalStateException("Cannot check permission from offline player");
            }
        }

        @NotNull
        @Override
        public UUID getUniqueId() {
            return uniqueId;
        }

        @NotNull
        @Override
        public String getName() {
            return name;
        }

        @Override
        public Player getPlayer() {
            return null;
        }
    }
}
