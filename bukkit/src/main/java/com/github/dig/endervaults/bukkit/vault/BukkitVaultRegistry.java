package com.github.dig.endervaults.bukkit.vault;

import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.VaultRegistry;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class BukkitVaultRegistry implements VaultRegistry {

    private final Map<UUID, Map<UUID, Vault>> vaults;

    public BukkitVaultRegistry() {
        this.vaults = new HashMap<>();
    }

    @Override
    public Optional<Vault> get(UUID ownerUUID, UUID id) {
        final Map<UUID, Vault> map = vaults.get(ownerUUID);
        if (map == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public Map<UUID, Vault> get(UUID ownerUUID) {
        Map<UUID, Vault> map = vaults.get(ownerUUID);
        if (map == null) {
            map = new HashMap<>();
            vaults.put(ownerUUID, map);
        }
        return map;
    }

    @Override
    public Optional<Vault> getByMetadata(UUID ownerUUID, String key, Object value) {
        return get(ownerUUID).values()
                .stream()
                .filter(vault -> vault.getMetadata().get(key) != null)
                .map(vault -> new Pair<>(vault, vault.getMetadata().get(key)))
                .filter(vaultObjectPair -> vaultObjectPair.getValue1().equals(value))
                .map(vaultObjectPair -> vaultObjectPair.getValue0())
                .findFirst();
    }

    @Override
    public Set<UUID> getAllOwners() {
        return vaults.keySet();
    }

    @Override
    public synchronized void register(UUID ownerUUID, Vault vault) {
        Map<UUID, Vault> map = vaults.get(ownerUUID);
        if (map == null) {
            map = new HashMap<>();
            vaults.put(ownerUUID, map);
        }
        map.put(vault.getId(), vault);
    }

    @Override
    public synchronized void clean(UUID ownerUUID) {
        final Map<UUID, Vault> map = vaults.remove(ownerUUID);
        if (map != null) {
            map.clear();
        }
    }
}
