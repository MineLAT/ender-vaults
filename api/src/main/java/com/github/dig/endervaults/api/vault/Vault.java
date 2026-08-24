package com.github.dig.endervaults.api.vault;

import com.github.dig.endervaults.api.storage.ContentMethod;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public interface Vault {

    Object NULL_VALUE = new Object();

    @NotNull
    UUID getId();

    @NotNull
    UUID getOwner();

    int getSize();

    int getFreeSize();

    @NotNull
    String getContent() throws IOException;

    @NotNull
    Map<String, Object> getMetadata();

    @NotNull
    VaultState getContentState();

    @NotNull
    VaultState getMetadataState();

    default void setContent(@NotNull String encoded) throws IOException {
        setContent(encoded, ContentMethod.THROW);
    }

    @ApiStatus.Experimental
    void setContent(@NotNull String encoded, @NotNull ContentMethod method) throws IOException;

    @Nullable
    <T> Object setMetadata(@NotNull VaultDefaultMetadata<T> meta, @Nullable T value);

    void setContentState(@NotNull VaultState state);

    void setMetadataState(@NotNull VaultState state);



    // utility methods

    default boolean isBlank() {
        return getSize() == getFreeSize() && !has(VaultDefaultMetadata.ICON);
    }

    default boolean meet(@NotNull VaultState state) {
        return getContentState() == state || getMetadataState() == state;
    }

    default void set(@NotNull VaultState state) {
        setContentState(state);
        setMetadataState(state);
    }

    default boolean has(@NotNull VaultDefaultMetadata<?> meta) {
        return getMetadata().getOrDefault(meta.getKey(), NULL_VALUE) != NULL_VALUE;
    }

    @Nullable
    default <T> T get(@NotNull VaultDefaultMetadata<T> meta) {
        final Object value = getMetadata().getOrDefault(meta.getKey(), NULL_VALUE);
        return value == NULL_VALUE ? null : meta.parse(value);
    }

    @Nullable
    default <T> Object set(@NotNull VaultDefaultMetadata<T> meta, @Nullable T value) {
        return setMetadata(meta, value);
    }
}
