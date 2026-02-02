package com.github.dig.endervaults.api.vault;

import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.UUID;

public interface Vault {

    Object NULL_VALUE = new Object();

    boolean isModified();

    boolean isContentLoaded();

    @NotNull
    UUID getId();

    @NotNull
    UUID getOwner();

    int getSize();

    int getFreeSize();

    @NotNull
    Map<String, Object> getMetadata();

    default boolean has(@NotNull VaultDefaultMetadata<?> meta) {
        return getMetadata().getOrDefault(meta.getKey(), NULL_VALUE) != NULL_VALUE;
    }

    @Nullable
    default <T> T get(@NotNull VaultDefaultMetadata<T> meta) {
        final Object value = getMetadata().getOrDefault(meta.getKey(), NULL_VALUE);
        return value == NULL_VALUE ? null : meta.parse(value);
    }

    @Nullable
    <T> Object set(@NotNull VaultDefaultMetadata<T> meta, @Nullable T value);

}
