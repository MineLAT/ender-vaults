package com.github.dig.endervaults.api.vault.metadata;

import org.jetbrains.annotations.NotNull;

public class VaultDefaultMetadata<T> {

    public static final VaultDefaultMetadata<Integer> ORDER = new VaultDefaultMetadata<>(Integer.class, "order") {
        @Override
        public @NotNull Integer parse(@NotNull Object object) {
            if (object instanceof Number) {
                return ((Number) object).intValue();
            } else {
                return Integer.parseInt(String.valueOf(object));
            }
        }
    };
    public static final VaultDefaultMetadata<String> ICON = new VaultDefaultMetadata<>(String.class, "icon") {
        @Override
        public @NotNull String parse(@NotNull Object object) {
            return String.valueOf(object);
        }
    };
    public static final VaultDefaultMetadata<Integer> FREE_SIZE = new VaultDefaultMetadata<>(Integer.class, "free_size") {
        @Override
        public @NotNull Integer parse(@NotNull Object object) {
            if (object instanceof Number) {
                return ((Number) object).intValue();
            } else {
                return Integer.parseInt(String.valueOf(object));
            }
        }
    };

    private final @NotNull Class<T> type;
    private final @NotNull String key;

    VaultDefaultMetadata(@NotNull Class<T> type, @NotNull String key) {
        this.type = type;
        this.key = key;
    }

    @NotNull
    public Class<T> getType() {
        return type;
    }

    @NotNull
    public String getKey() {
        return key;
    }

    @NotNull
    public T parse(@NotNull Object object) {
        throw new IllegalStateException();
    }

    @NotNull
    public String save(@NotNull T value) {
        return String.valueOf(value);
    }

    @Override
    public final boolean equals(Object object) {
        if (!(object instanceof VaultDefaultMetadata)) return false;

        VaultDefaultMetadata<?> that = (VaultDefaultMetadata<?>) object;
        return getKey().equals(that.getKey());
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }
}
