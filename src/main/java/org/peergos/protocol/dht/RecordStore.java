package org.peergos.protocol.dht;

import java.util.Optional;

/**
 * Stores arbitrary DHT records as raw bytes, keyed by raw bytes.
 * The meaning of keys and values is determined by the application layer
 * (e.g. IPNS uses /ipns/<peerId> keys and signed protobuf values).
 */
public interface RecordStore extends AutoCloseable {

    void put(byte[] key, byte[] value);

    Optional<byte[]> get(byte[] key);

    void remove(byte[] key);
}
