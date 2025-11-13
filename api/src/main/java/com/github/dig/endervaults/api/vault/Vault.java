package com.github.dig.endervaults.api.vault;

import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.UUID;

public interface Vault {

    UUID getId();

    UUID getOwner();

    int getSize();

    int getFreeSize();

    Map<String, Object> getMetadata();

    default boolean has(@NotNull VaultDefaultMetadata<?> meta) {
        return getMetadata().containsKey(meta.getKey());
    }

    @Nullable
    default <T> T get(@NotNull VaultDefaultMetadata<T> meta) {
        final Object value = getMetadata().get(meta.getKey());
        return value == null ? null : meta.parse(value);
    }

    <T> void set(@NotNull VaultDefaultMetadata<T> meta, @Nullable T value);

}
