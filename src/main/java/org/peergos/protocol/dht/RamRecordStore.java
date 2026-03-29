package org.peergos.protocol.dht;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class RamRecordStore implements RecordStore {

    private final Map<String, byte[]> records = new ConcurrentHashMap<>();

    private String keyString(byte[] key) {
        return Base64.getEncoder().encodeToString(key);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        records.put(keyString(key), value);
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return Optional.ofNullable(records.get(keyString(key)));
    }

    @Override
    public void remove(byte[] key) {
        records.remove(keyString(key));
    }

    @Override
    public void close() {}
}
