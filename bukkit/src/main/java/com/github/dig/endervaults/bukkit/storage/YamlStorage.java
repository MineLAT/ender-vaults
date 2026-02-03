package com.github.dig.endervaults.bukkit.storage;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.storage.DataStorage;
import com.github.dig.endervaults.api.storage.Storage;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import com.github.dig.endervaults.api.vault.metadata.VaultMetadataRegistry;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import com.github.dig.endervaults.bukkit.vault.BukkitVault;
import com.google.common.io.Files;
import lombok.extern.java.Log;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.*;

@Log
public class YamlStorage implements DataStorage {

    private final EVBukkitPlugin plugin = VaultPluginProvider.getPlugin();

    @Override
    public boolean init(Storage storage) {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean exists(UUID ownerUUID, UUID id) {
        return getVaultFile(ownerUUID, id).exists();
    }

    @Override
    public List<Vault> load(UUID ownerUUID) throws Throwable {
        List<Vault> vaults = new ArrayList<>();

        File file = getOwnerFolder(ownerUUID);
        if (file.isDirectory()) {
            File[] files = file.listFiles((File f, String name) -> name.endsWith(".yml"));
            for (File vaultFile : files) {
                UUID id = UUID.fromString(Files.getNameWithoutExtension(vaultFile.getName()));
                load(ownerUUID, id).ifPresent(vault -> vaults.add(vault));
            }
        }

        return vaults;
    }

    @Override
    public Optional<Vault> load(UUID ownerUUID, UUID id) throws Throwable {
        final FileConfiguration configuration = loadFile(ownerUUID, id);
        if (configuration == null) {
            return Optional.empty();
        }
        return Optional.of(deserialize(ownerUUID, id, configuration));
    }

    @Override
    public @NotNull <T> Optional<Vault> loadSnapshot(@NotNull UUID ownerUUID, @NotNull VaultDefaultMetadata<T> meta, @NotNull T value, int snapshot) throws Throwable {
        final File file = getOwnerFolder(ownerUUID);
        if (file.isDirectory()) {
            File[] files = file.listFiles((File f, String name) -> name.endsWith(".yml"));
            int count = 0;
            for (File vaultFile : files) {
                final UUID id = UUID.fromString(Files.getNameWithoutExtension(vaultFile.getName()));
                final FileConfiguration configuration = loadFile(ownerUUID, id);
                if (configuration == null) {
                    continue;
                }
                final Object fileValue = configuration.get("metadata." + meta.getKey());
                if (fileValue == null) {
                    continue;
                }
                if (value.equals(meta.parse(fileValue))) {
                    if (count == snapshot) {
                        return Optional.of(deserialize(ownerUUID, id, configuration));
                    }
                    count++;
                }
            }
        }
        return Optional.empty();
    }

    @Nullable
    private FileConfiguration loadFile(@NotNull UUID ownerUUID, @NotNull UUID id) {
        if (!exists(ownerUUID, id)) {
            return null;
        }
        FileConfiguration configuration = YamlConfiguration.loadConfiguration(getVaultFile(ownerUUID, id));

        if (!configuration.contains("size") || !configuration.contains("contents") || !configuration.contains("metadata")) {
            return null;
        }

        return configuration;
    }

    @NotNull
    private Vault deserialize(@NotNull UUID ownerUUID, @NotNull UUID id, @NotNull FileConfiguration configuration) throws Throwable {
        VaultMetadataRegistry metadataRegistry = plugin.getMetadataRegistry();

        int size = configuration.getInt("size");

        ConfigurationSection metadataSection = configuration.getConfigurationSection("metadata");
        Map<String, Object> metadata = new HashMap<>();
        for (String key : metadataSection.getKeys(false)) {
            String value = metadataSection.getString(key);
            metadataRegistry.get(key).ifPresent(converter -> metadata.put(key, converter.to(value)));
        }

        String title = plugin.getLanguage().get(Lang.VAULT_TITLE, metadata);
        BukkitVault vault = new BukkitVault(id, title, size, ownerUUID, metadata);

        vault.setContent(configuration.getString("contents"));

        return vault;
    }

    @Override
    public void save(Vault vault)  throws Throwable {
        File file = getVaultFile(vault.getOwner(), vault.getId());
        file.getParentFile().mkdirs();
        file.createNewFile();

        VaultMetadataRegistry metadataRegistry = plugin.getMetadataRegistry();
        FileConfiguration configuration = YamlConfiguration.loadConfiguration(file);

        configuration.set("size", vault.getSize());
        vault.getMetadata().entrySet().removeIf(entry -> {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            // Clean keys that are marked to be deleted
            if (value == Vault.NULL_VALUE) {
                return true;
            }
            metadataRegistry.get(key).ifPresent(converter -> configuration.set("metadata." + key, converter.from(value)));
            return false;
        });

        configuration.set("contents", vault.getContent());

        configuration.save(file);
    }

    @Override
    public int delete(UUID ownerUUID) {
        File file = getOwnerFolder(ownerUUID);
        int result = 0;
        if (file.exists() && file.isDirectory()) {
            result = file.listFiles((File f, String name) -> name.endsWith(".yml")).length;
            file.delete();
        }
        return result;
    }

    private String getDirectoryName() {
        FileConfiguration configuration = (FileConfiguration) plugin.getConfigFile().getConfiguration();
        return configuration.getString("storage.settings.flatfile.directory", "data");
    }

    private File getOwnerFolder(UUID ownerUUID) {
        String filePath = getDirectoryName() + File.separator + ownerUUID.toString();
        return new File(plugin.getDataFolder(), filePath);
    }

    private File getVaultFile(UUID ownerUUID, UUID id) {
        String filePath = getDirectoryName() + File.separator + ownerUUID.toString() + File.separator + id.toString() + ".yml";
        return new File(plugin.getDataFolder(), filePath);
    }
}
