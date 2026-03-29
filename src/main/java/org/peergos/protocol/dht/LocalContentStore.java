package org.peergos.protocol.dht;

import io.ipfs.multihash.Multihash;

/**
 * Minimal interface for checking whether this node holds a piece of content locally.
 * Used by the DHT engine to answer GET_PROVIDERS queries by advertising itself as a
 * provider when it has a requested block.
 * Pass {@code Optional.empty()} when no local content store is available.
 */
public interface LocalContentStore {
    boolean has(Multihash cid) throws Exception;
}
