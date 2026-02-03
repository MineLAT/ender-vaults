package com.github.dig.endervaults.bukkit.storage;

import com.github.dig.endervaults.api.VaultPluginProvider;
import com.github.dig.endervaults.api.lang.Lang;
import com.github.dig.endervaults.api.storage.DataStorage;
import com.github.dig.endervaults.api.storage.Storage;
import com.github.dig.endervaults.api.vault.Vault;
import com.github.dig.endervaults.api.vault.VaultState;
import com.github.dig.endervaults.api.vault.metadata.MetadataConverter;
import com.github.dig.endervaults.api.vault.metadata.VaultDefaultMetadata;
import com.github.dig.endervaults.api.vault.metadata.VaultMetadataRegistry;
import com.github.dig.endervaults.bukkit.EVBukkitPlugin;
import com.github.dig.endervaults.bukkit.vault.BukkitVault;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import lombok.extern.java.Log;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;

@Log
public class HikariStorage implements DataStorage {

    private final EVBukkitPlugin plugin = VaultPluginProvider.getPlugin();

    private HikariDataSource hikariDataSource;
    private String vaultTable;
    private String metadataTable;

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

        createTableIfNotExist(vaultTable, SqlConstants.SQL_CREATE_TABLE_VAULT);
        createTableIfNotExist(metadataTable, SqlConstants.SQL_CREATE_TABLE_VAULT_METADATA);
        return hikariDataSource.isRunning();
    }

    @Override
    public void close() {
        if (hikariDataSource != null && hikariDataSource.isRunning()) {
            hikariDataSource.close();
        }
    }

    @Override
    public boolean exists(UUID ownerUUID, UUID id) throws Throwable {
        return connect(con -> {
            return exists(con, ownerUUID, id);
        });
    }

    private boolean exists(@NotNull Connection con, UUID ownerUUID, UUID id) throws Throwable {
        boolean has;
        try (PreparedStatement stmt = stmt(con, SqlConstants.Vault.CHECK_VAULT, vaultTable)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());

            ResultSet rs = stmt.executeQuery();
            has = rs.next();
        }
        return has;
    }

    @Override
    public List<Vault> load(UUID ownerUUID) throws Throwable {
        return connect(con -> {
            return selectVaults(con, ownerUUID);
        });
    }

    @Override
    public Optional<Vault> load(UUID ownerUUID, UUID id) throws Throwable {
        return connect(con -> {
            return selectVault(con, id, ownerUUID);
        });
    }

    @Override
    public @NotNull <T> Optional<Vault> load(@NotNull UUID ownerUUID, @NotNull VaultDefaultMetadata<T> meta, @NotNull T value) throws Throwable {
        return connect(con -> {
            final UUID id = selectMetadataId(con, ownerUUID, meta.getKey(), meta.save(value));
            if (id == null) {
                return Optional.empty();
            }
            return selectVault(con, id, ownerUUID);
        });
    }

    @Override
    public void loadContents(@NotNull Vault vault) throws Throwable {
        connect(con -> {
            final Integer order = vault.get(VaultDefaultMetadata.ORDER);
            final UUID id = selectMetadataId(con, vault.getOwner(), VaultDefaultMetadata.ORDER.getKey(), String.valueOf(order));
            if (id != null && !id.equals(vault.getId())) {
                throw new IllegalStateException("Duplicated vault #" + order + " entry found for owner " + vault.getOwner() + " and vault " + vault.getId());
            }

            try (PreparedStatement stmt = stmt(con, SqlConstants.Vault.SELECT_CONTENT, vaultTable)) {
                stmt.setString(1, vault.getId().toString());

                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    String contents = rs.getString("contents");
                    vault.setContent(contents);
                }
            }
        });
    }

    @Override
    public void save(Vault vault) throws Throwable {
        connect(con -> {
            if (exists(con, vault.getOwner(), vault.getId())) {
                if (vault.getContentState() == VaultState.MODIFIED) {
                    updateContent(con, vault.getId(), vault.getOwner(), vault.getSize(), vault.getContent());

                    vault.setContentState(VaultState.LOADED);
                }
                if (vault.getMetadataState() == VaultState.MODIFIED) {
                    final Iterator<Map.Entry<String, Object>> iterator = vault.getMetadata().entrySet().iterator();
                    while (iterator.hasNext()) {
                        final Map.Entry<String, Object> entry = iterator.next();
                        final String key = entry.getKey();
                        final Object value = entry.getValue();

                        if (value == Vault.NULL_VALUE) {
                            deleteMetadata(con, vault.getId(), key);
                            iterator.remove();
                            continue;
                        }

                        if (getMetadata(con, vault.getId(), vault.getOwner(), key) != null) {
                            updateMetadata(con, vault.getId(), vault.getOwner(), key, value);
                        } else {
                            insertMetadata(con, vault.getId(), vault.getOwner(), key, value);
                        }
                    }

                    vault.setMetadataState(VaultState.LOADED);
                }
            } else {
                if (vault.getContentState() == VaultState.MODIFIED) {
                    String contents = vault.getContent();
                    insertContent(con, vault.getId(), vault.getOwner(), vault.getSize(), contents);

                    vault.setContentState(VaultState.LOADED);
                }
                if (vault.getMetadataState() == VaultState.MODIFIED) {
                    for (Map.Entry<String, Object> entry : vault.getMetadata().entrySet()) {
                        insertMetadata(con, vault.getId(), vault.getOwner(), entry.getKey(), entry.getValue());
                    }

                    vault.setMetadataState(VaultState.LOADED);
                }
            }
        });
    }

    @Override
    public int delete(UUID ownerUUID) throws Throwable {
        return connect(con -> {
            int result = 0;
            try (PreparedStatement stmt = stmt(con, SqlConstants.DELETE, vaultTable)) {
                stmt.setString(1, ownerUUID.toString());
                result = stmt.executeUpdate();
            }
            try (PreparedStatement stmt = stmt(con, SqlConstants.DELETE, metadataTable)) {
                stmt.setString(1, ownerUUID.toString());
                stmt.executeUpdate();
            }
            return result;
        });
    }

    private void createTableIfNotExist(String table, String TABLE_SQL) {
        TABLE_SQL = String.format(TABLE_SQL, table);
        try (Connection conn = hikariDataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(TABLE_SQL)) {
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.log(Level.SEVERE, "[EnderVaults] Unable to create table " + table + ".", ex);
        }
    }

    private BukkitVault createVault(@NotNull Connection con, UUID id, UUID ownerUUID, int size) throws Throwable {
        Map<String, Object> metadata = getMetadata(con, ownerUUID, id);
        String title = plugin.getLanguage().get(Lang.VAULT_TITLE, metadata);
        return new BukkitVault(id, title, size, ownerUUID, metadata);
    }

    private void insertContent(@NotNull Connection con, UUID id, UUID ownerUUID, int size, String contents) throws Throwable {
        try (PreparedStatement stmt = stmt(con, SqlConstants.Vault.INSERT_VAULT, vaultTable)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());
            stmt.setInt(3, size);
            stmt.setString(4, contents);
            stmt.executeUpdate();
        }
    }

    private void insertMetadata(@NotNull Connection con, UUID id, UUID ownerUUID, String key, Object value) throws Throwable {
        if (value == Vault.NULL_VALUE) {
            return;
        }
        final MetadataConverter converter = plugin.getMetadataRegistry().get(key).orElse(null);
        if (converter == null) {
            return;
        }
        insertMetadata(con, id, ownerUUID, key, converter.from(value));
    }

    private void insertMetadata(@NotNull Connection con, UUID id, UUID ownerUUID, String key, String value) throws Throwable {
        try (PreparedStatement stmt = stmt(con, SqlConstants.Metadata.INSERT, metadataTable)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());
            stmt.setString(3, key);
            stmt.setString(4, value);
            stmt.executeUpdate();
        }
    }

    private void updateContent(@NotNull Connection con, UUID id, UUID ownerUUID, int size, String contents) throws Throwable {
        try (PreparedStatement stmt = stmt(con, SqlConstants.Vault.UPDATE_CONTENT, vaultTable)) {
            stmt.setInt(1, size);
            stmt.setString(2, contents);
            stmt.setString(3, id.toString());
            stmt.setString(4, ownerUUID.toString());
            stmt.executeUpdate();
        }
    }

    private void updateMetadata(@NotNull Connection con, UUID id, UUID ownerUUID, String key, Object value) throws Throwable {
        if (value == Vault.NULL_VALUE) {
            return;
        }
        final MetadataConverter converter = plugin.getMetadataRegistry().get(key).orElse(null);
        if (converter == null) {
            return;
        }
        updateMetadata(con, id, ownerUUID, key, converter.from(value));
    }

    private void updateMetadata(@NotNull Connection con, UUID id, UUID ownerUUID, String key, String value) throws Throwable {
        try (PreparedStatement stmt = stmt(con, SqlConstants.Metadata.UPDATE, metadataTable)) {
            stmt.setString(1, value);
            stmt.setString(2, id.toString());
            stmt.setString(3, ownerUUID.toString());
            stmt.setString(4, key);
            stmt.executeUpdate();
        }
    }

    private void deleteMetadata(@NotNull Connection con, @NotNull UUID id, @NotNull String key) throws Throwable {
        try (PreparedStatement stmt = stmt(con, SqlConstants.Metadata.DELETE, metadataTable)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, key);
            stmt.executeUpdate();
        }
    }

    @NotNull
    private Optional<Vault> selectVault(@NotNull Connection con, @NotNull UUID id, @NotNull UUID ownerUUID) throws Throwable {
        int size;
        String contents;
        try (PreparedStatement stmt = stmt(con, SqlConstants.Vault.SELECT_VAULT, vaultTable)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                size = rs.getInt("size");
                contents = rs.getString("contents");
            } else {
                return Optional.empty();
            }
        }

        final BukkitVault vault = createVault(con, id, ownerUUID, size);
        vault.setContent(contents);
        vault.setContentState(VaultState.LOADED);

        return Optional.of(vault);
    }

    private List<Vault> selectVaults(@NotNull Connection con, UUID ownerUUID) throws Throwable {
        List<Vault> vaults = new ArrayList<>();
        try (PreparedStatement stmt = stmt(con, SqlConstants.Vault.SELECT_VAULTS, vaultTable)) {
            stmt.setString(1, ownerUUID.toString());

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                UUID id = UUID.fromString(rs.getString("id"));
                int size = rs.getInt("size");
                vaults.add(createVault(con, id, ownerUUID, size));
            }
        }
        return vaults;
    }

    private Map<String, Object> getMetadata(@NotNull Connection con, UUID ownerUUID, UUID id) throws Throwable {
        VaultMetadataRegistry metadataRegistry = plugin.getMetadataRegistry();

        Map<String, Object> metadata = new HashMap<>();
        try (PreparedStatement stmt = stmt(con, SqlConstants.Metadata.SELECT_KEY_VALUE, metadataTable)) {
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
        }
        return metadata;
    }

    @Nullable
    private Object getMetadata(@NotNull Connection con, UUID id, UUID ownerUUID, String key) throws Throwable {
        Optional<MetadataConverter> converterOptional = plugin.getMetadataRegistry().get(key);
        if (converterOptional.isEmpty()) {
            return null;
        }

        try (PreparedStatement stmt = stmt(con, SqlConstants.Metadata.SELECT_VALUE, metadataTable)) {
            stmt.setString(1, id.toString());
            stmt.setString(2, ownerUUID.toString());
            stmt.setString(3, key);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                final String value = rs.getString("value");
                return converterOptional.get().to(value);
            }

            return null;
        }
    }

    @Nullable
    private UUID selectMetadataId(@NotNull Connection con, @NotNull UUID ownerUUID, @NotNull String key, @NotNull String value) throws Throwable {
        try (PreparedStatement stmt = stmt(con, SqlConstants.Metadata.SELECT_ID, metadataTable)) {
            stmt.setString(1, ownerUUID.toString());
            stmt.setString(2, key);
            stmt.setString(3, value);

            final ResultSet result = stmt.executeQuery();
            if (result.next()) {
                return UUID.fromString(result.getString("id"));
            } else {
                return null;
            }
        }
    }

    @NotNull
    private PreparedStatement stmt(@NotNull Connection con, @NotNull String sql, @NotNull Object... args) throws Throwable {
        return con.prepareStatement(String.format(sql, args));
    }

    public void connect(@NotNull SqlConsumer consumer) throws Throwable {
        if (hikariDataSource == null || hikariDataSource.isClosed()) {
            throw new IllegalStateException("The current database connection is closed");
        }

        try (Connection connection = hikariDataSource.getConnection()) {
            consumer.accept(connection);
        }
    }

    public <R> R connect(@NotNull SqlFunction<R> consumer) throws Throwable {
        if (hikariDataSource == null || hikariDataSource.isClosed()) {
            throw new IllegalStateException("The current database connection is closed");
        }

        try (Connection connection = hikariDataSource.getConnection()) {
            return consumer.apply(connection);
        }
    }

    @FunctionalInterface
    public interface SqlConsumer {
        void accept(@NotNull Connection connection) throws Throwable;
    }

    @FunctionalInterface
    public interface SqlFunction<R> {
        @Nullable
        R apply(@NotNull Connection connection) throws Throwable;
    }
}
