package com.github.dig.endervaults.bukkit.storage;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.storage.DataStorage;
import com.github.dig.endervaults.api.storage.Storage;
import com.github.dig.endervaults.api.util.VaultSerializable;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.metadata.MetadataConverter;
import com.github.dig.endervaults.api.vault.metadata.VaultMetadataRegistry;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import com.github.dig.endervaults.bukkit.vault.BukkitVault;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import lombok.extern.java.Log;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.scheduler.BukkitTask;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.logging.Level;

@Log
public class HikariStorage implements DataStorage {

    private final EVBukkitPlugin plugin = (EVBukkitPlugin) VaultPluginProvider.getPlugin();

    private HikariDataSource hikariDataSource;
    private String vaultTable;
    private String metadataTable;
    private String stateTable;
    private final ReadWriteLock taskLock = new ReentrantReadWriteLock();
    private BukkitTask getTask;
    private BukkitTask aliveTask;

    private final Map<UUID, Consumer<List<Vault>>> lockedPlayers = new HashMap<>();

    @Override
    public boolean init(Storage storage) {
        FileConfiguration config = (FileConfiguration) plugin.getConfigFile().getConfiguration();
        ConfigurationSection settings = config.getConfigurationSection(storage == Storage.MYSQL ? "storage.settings.mysql" : "storage.settings.mariadb");

        String address = settings.getString("address", "localhost");
        String database = settings.getString("database", "minecraft");
        String user = settings.getString("user", "minecraft");
        String password = settings.getString("password", "123");

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(String.format("jdbc:%s://%s/%s", storage == Storage.MYSQL ? "mysql" : "mariadb", address, database));

        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);

        if (storage == Storage.MARIADB) {
            hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
        }

        ConfigurationSection properties = settings.getConfigurationSection("properties");
        for (String key : properties.getKeys(false)) {
            hikariConfig.addDataSourceProperty(key, properties.getString(key));
        }

        try {
            hikariDataSource = new HikariDataSource(hikariConfig);
        } catch (HikariPool.PoolInitializationException e) {
            log.log(Level.SEVERE, "[EnderVaults] Unable to connect to database.", e);
            return false;
        }

        vaultTable = settings.getString("tables.vault");
        metadataTable = settings.getString("tables.vault-metadata");
        stateTable = settings.getString("tables.vault-state", "endervaults_vault_state");

        createTableIfNotExist(vaultTable, DatabaseConstants.SQL_CREATE_TABLE_VAULT);
        createTableIfNotExist(metadataTable, DatabaseConstants.SQL_CREATE_TABLE_VAULT_METADATA);
        createTableIfNotExist(stateTable, DatabaseConstants.SQL_CREATE_TABLE_VAULT_STATE);

        getTask = Bukkit.getScheduler().runTaskTimerAsynchronously(plugin, this::getStates, 40L, 20L);
        aliveTask = Bukkit.getScheduler().runTaskTimerAsynchronously(plugin, () -> {
            aliveStates(plugin.getPersister().getPersisted());
        }, 20L, 600L);

        return hikariDataSource.isRunning();
    }

    @Override
    public void close() {
        if (getTask != null && !getTask.isCancelled()) {
            getTask.cancel();
        }
        if (aliveTask != null && !aliveTask.isCancelled()) {
            aliveTask.cancel();
        }
        if (hikariDataSource != null && hikariDataSource.isRunning()) {
            hikariDataSource.close();
        }
    }

    @Override
    public boolean exists(UUID ownerUUID, UUID id) {
        String sql = String.format(DatabaseConstants.SQL_SELECT_VAULT_BY_ID_AND_OWNER, vaultTable);
        boolean has;
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());

            ResultSet rs = stmt.executeQuery();
            has = rs.next();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
            return false;
        }
        return has;
    }

    @Override
    public void load(UUID ownerUUID, Consumer<List<Vault>> consumer) {
        if (!lockedPlayers.containsKey(ownerUUID)) {
            lockedPlayers.put(ownerUUID, consumer);
        }
    }

    @Override
    public Optional<Vault> load(UUID ownerUUID, UUID id) {
        return get(id, ownerUUID);
    }

    @Override
    public void save(Vault vault) {
        VaultMetadataRegistry metadataRegistry = plugin.getMetadataRegistry();
        if (exists(vault.getOwner(), vault.getId())) {
            update(vault.getId(), vault.getOwner(), vault.getSize(), ((VaultSerializable) vault).encode());
            for (String key : vault.getMetadata().keySet()) {
                Object value = vault.getMetadata().get(key);
                metadataRegistry.get(key)
                        .ifPresent(converter -> {
                            if (exists(vault.getId(), vault.getOwner(), key)) {
                                update(vault.getId(), vault.getOwner(), key, converter.from(value));
                            } else {
                                insert(vault.getId(), vault.getOwner(), key, converter.from(value));
                            }
                        });
            }
        } else {
            String contents = ((VaultSerializable) vault).encode();
            insert(vault.getId(), vault.getOwner(), vault.getSize(), contents);
            for (String key : vault.getMetadata().keySet()) {
                Object value = vault.getMetadata().get(key);
                metadataRegistry.get(key)
                        .ifPresent(converter -> insert(vault.getId(), vault.getOwner(), key, converter.from(value)));
            }
        }
    }

    @Override
    public void save(UUID ownerUUID, Collection<Vault> vaults) throws IOException {
        updateState(ownerUUID, "LOCKED");
        try {
            DataStorage.super.save(ownerUUID, vaults);
        } finally {
            updateState(ownerUUID, "SAVED");
        }
    }

    private void createTableIfNotExist(String table, String TABLE_SQL) {
        TABLE_SQL = String.format(TABLE_SQL, table);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(TABLE_SQL)) {
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Unable to create table " + table + ".", ex);
        }
    }

    private Vault create(UUID id, UUID ownerUUID, int size, String contents) {
        Map<String, Object> metadata = getMetadata(ownerUUID, id);
        String title = plugin.getLanguage().get(Lang.VAULT_TITLE, metadata);
        BukkitVault vault = new BukkitVault(id, title, size, ownerUUID, metadata);

        VaultSerializable serializable = vault;
        serializable.decode(contents);
        return vault;
    }

    private void insert(UUID id, UUID ownerUUID, int size, String contents) {
        String sql = String.format(DatabaseConstants.SQL_INSERT_VAULT, vaultTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());
            stmt.setInt(3, size);
            stmt.setString(4, contents);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        }
    }

    private void insert(UUID id, UUID ownerUUID, String key, String value) {
        String sql = String.format(DatabaseConstants.SQL_INSERT_VAULT_METADATA, metadataTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());
            stmt.setString(3, key);
            stmt.setString(4, value);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        }
    }

    private void update(UUID id, UUID ownerUUID, int size, String contents) {
        String sql = String.format(DatabaseConstants.SQL_UPDATE_VAULT_BY_ID_AND_OWNER, vaultTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, size);
            stmt.setString(2, contents);
            stmt.setString(3, id.toString());
            stmt.setString(4, ownerUUID.toString());
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        }
    }

    private void update(UUID id, UUID ownerUUID, String key, String value) {
        String sql = String.format(DatabaseConstants.SQL_UPDATE_VAULT_METADATA_BY_ID_AND_OWNER_AND_KEY, metadataTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, value);
            stmt.setString(2, id.toString());
            stmt.setString(3, ownerUUID.toString());
            stmt.setString(4, key);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        }
    }

    private Optional<Vault> get(UUID id, UUID ownerUUID) {
        int size;
        String contents;
        String sql = String.format(DatabaseConstants.SQL_SELECT_VAULT_BY_ID_AND_OWNER, vaultTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                size = rs.getInt("size");
                contents = rs.getString("contents");
            } else {
                return Optional.empty();
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
            return Optional.empty();
        }
        return Optional.ofNullable(create(id, ownerUUID, size, contents));
    }

    private List<Vault> get(UUID ownerUUID) {
        List<Vault> vaults = new ArrayList<>();
        String sql = String.format(DatabaseConstants.SQL_SELECT_VAULT_BY_OWNER, vaultTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, ownerUUID.toString());

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                UUID id = UUID.fromString(rs.getString("id"));
                int size = rs.getInt("size");
                String contents = rs.getString("contents");
                vaults.add(create(id, ownerUUID, size, contents));
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        }
        return vaults;
    }

    private Map<String, Object> getMetadata(UUID ownerUUID, UUID id) {
        VaultMetadataRegistry metadataRegistry = plugin.getMetadataRegistry();

        Map<String, Object> metadata = new HashMap<>();
        String sql = String.format(DatabaseConstants.SQL_SELECT_VAULT_METADATA_BY_ID_AND_OWNER, metadataTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String key = rs.getString("name");
                Optional<MetadataConverter> converterOptional = metadataRegistry.get(key);
                if (converterOptional.isPresent()) {
                    MetadataConverter converter = converterOptional.get();
                    metadata.put(key, converter.to(rs.getString("value")));
                }
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        }

        return metadata;
    }

    private boolean exists(UUID id, UUID ownerUUID, String key) {
        String sql = String.format(DatabaseConstants.SQL_SELECT_VAULT_METADATA_BY_ID_AND_OWNER_AND_KEY, metadataTable);
        boolean has;
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());
            stmt.setString(3, key);

            ResultSet rs = stmt.executeQuery();
            has = rs.next();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
            return false;
        }
        return has;
    }

    private void updateState(UUID uniqueId, String state) {
        String sql = String.format(DatabaseConstants.SQL_INSERT_VAULT_STATE, stateTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, uniqueId.toString());
            stmt.setString(2, state);

            stmt.execute();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        }
    }

    private void getStates() {
        if (lockedPlayers.isEmpty()) {
            return;
        }
        taskLock.readLock().lock();

        Set<UUID> locked = new HashSet<>();
        String sql = String.format(DatabaseConstants.SQL_SELECT_VAULT_STATES, stateTable);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSet result = stmt.executeQuery();
            while (result.next()) {
                locked.add(UUID.fromString(result.getString("id")));
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        } finally {
            for (Map.Entry<UUID, Consumer<List<Vault>>> entry : lockedPlayers.entrySet()) {
                if (!locked.contains(entry.getKey())) {
                    Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                        if (Bukkit.getPlayer(entry.getKey()) != null) {
                            entry.getValue().accept(get(entry.getKey()));
                            updateState(entry.getKey(), "LOCKED");
                        }
                        lockedPlayers.remove(entry.getKey());
                    });
                }
            }
            locked.clear();
            taskLock.readLock().unlock();
        }
    }

    private void aliveStates(Set<UUID> loaded) {
        taskLock.readLock().lock();

        try (Connection conn = hikariDataSource.getConnection()) {
            if (!loaded.isEmpty()) {
                String insert = String.format(DatabaseConstants.SQL_INSERT_VAULT_STATE, stateTable);
                try (PreparedStatement stmt = conn.prepareStatement(insert)) {
                    for (UUID uuid : loaded) {
                        stmt.setString(1, uuid.toString());
                        stmt.setString(2, "LOCKED");
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                }
            }
            String delete = String.format(DatabaseConstants.SQL_DELETE_VAULT_STATES, stateTable);
            try (PreparedStatement stmt = conn.prepareStatement(delete)) {
                stmt.execute();
            }
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Error while executing query.", ex);
        } finally {
            taskLock.readLock().unlock();
        }
    }
}
