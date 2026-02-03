package com.github.dig.endervaults.api.vault;

import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class VaultHolder {

    private final UUID owner;
    private Map<UUID, Vault> vaults;

    private transient VaultState state = VaultState.UNKNOWN;

    public VaultHolder(@NotNull UUID owner) {
        this.owner = owner;
        this.vaults = new ConcurrentHashMap<>();
    }

    @NotNull
    public UUID getOwner() {
        return owner;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public <V extends Vault> Optional<V> getVault(@NotNull UUID id) {
        return Optional.ofNullable((V) vaults.get(id));
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public <V extends Vault> Optional<V> getVault(@Nullable Integer order) {
        for (Map.Entry<UUID, Vault> entry : vaults.entrySet()) {
            if (Objects.equals(entry.getValue().get(VaultDefaultMetadata.ORDER), order)) {
                return Optional.of((V) entry.getValue());
            }
        }
        return Optional.empty();
    }

    @NotNull
    public VaultState getState() {
        return state;
    }

    public synchronized void setState(@NotNull VaultState state) {
        this.state = state;
    }

    public synchronized void compute(@NotNull Vault vault) {
        final Integer order = vault.get(VaultDefaultMetadata.ORDER);
        if (order == null) {
            throw new IllegalArgumentException("Cannot add vault with invalid order");
        }

        final Vault current = this.vaults.remove(vault.getId());
        if (current != null) {
            vault.set(VaultState.ERROR);
            current.set(VaultState.ERROR);
            throw new IllegalArgumentException("Duplicated vault entry found for owner " + owner + " and vault " + vault.getId());
        }

        final Optional<Vault> optional = getVault(order);
        if (optional.isPresent()) {
            vault.set(VaultState.ERROR);
            optional.get().set(VaultState.ERROR);
            throw new IllegalArgumentException("Duplicated vault #" + order + " entry found for owner " + owner + " and vault " + vault.getId());
        }

        this.vaults.put(vault.getId(), vault);
    }

    @NotNull
    public Map<UUID, Vault> clear() {
        final Map<UUID, Vault> result = this.vaults;
        this.vaults = new ConcurrentHashMap<>();
        return result;
    }
}
