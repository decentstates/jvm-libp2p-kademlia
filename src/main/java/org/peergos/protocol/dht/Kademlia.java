package org.peergos.protocol.dht;

import com.offbynull.kademlia.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multiformats.Protocol;
import io.libp2p.core.multistream.*;
import io.libp2p.etc.types.*;
import io.libp2p.protocol.*;
import org.peergos.*;
import org.peergos.protocol.dnsaddr.*;
import org.peergos.util.Futures;
import org.peergos.util.Logging;

import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;
import java.util.stream.Stream;

public class Kademlia extends StrictProtocolBinding<KademliaController> implements AddressBookConsumer {

    private static final Logger LOG = Logging.LOG();
    public static final int BOOTSTRAP_PERIOD_MILLIS = 300_000;
    public static final String WAN_DHT_ID = "/ipfs/kad/1.0.0";
    public static final String LAN_DHT_ID = "/ipfs/lan/kad/1.0.0";
    private final KademliaEngine engine;
    private final boolean localDht;
    private final boolean quicEnabled, tcpEnabled;
    private AddressBook addressBook;

    public Kademlia(KademliaEngine dht, boolean localOnly, boolean quicEnabled, boolean tcpEnabled) {
        super(localOnly ? LAN_DHT_ID : WAN_DHT_ID, new KademliaProtocol(dht));
        this.engine = dht;
        this.localDht = localOnly;
        this.quicEnabled = quicEnabled;
        this.tcpEnabled = tcpEnabled;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
        this.addressBook = addrs;
    }

    public int bootstrapRoutingTable(Host host, List<MultiAddress> addrs, Predicate<String> filter) {
        List<String> resolved = addrs.stream()
                .parallel()
                .flatMap(a -> {
                    try {
                        return DnsAddr.resolve(a.toString()).stream();
                    } catch (CompletionException ce) {
                        return Stream.empty();
                    }
                })
                .filter(filter)
                .collect(Collectors.toList());
        List<? extends Future<? extends KademliaController>> futures = resolved.stream()
                .map(addr -> {
                    Multiaddr addrWithPeer = Multiaddr.fromString(addr);
                    addressBook.setAddrs(addrWithPeer.getPeerId(), 0, addrWithPeer).join();
                    return ioExec.submit(() -> dial(host, addrWithPeer).getController().join());
                })
                .collect(Collectors.toList());
        int successes = 0;
        for (Future<? extends KademliaController> future : futures) {
            try {
                future.get(5, TimeUnit.SECONDS);
                successes++;
            } catch (Exception e) {}
        }
        return successes;
    }

    private AtomicBoolean running = new AtomicBoolean(false);

    public void startBootstrapThread(Host us) {
        running.set(true);
        new Thread(() -> {
            while (running.get()) {
                try {
                    bootstrap(us);
                    Thread.sleep(BOOTSTRAP_PERIOD_MILLIS);
                } catch (Libp2pException e) {
                    if (!e.getMessage().contains("Transport is closed"))
                        e.printStackTrace();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }, "Kademlia bootstrap").start();
    }

    public void stopBootstrapThread() {
        running.set(false);
    }

    private boolean connectTo(Host us, PeerAddresses peer) {
        try {
            PeerId ourPeerId = PeerId.fromBase58(peer.peerId.toBase58());
            StreamPromise<? extends IdentifyController> conn = new Identify().dial(us, ourPeerId, getPublic(peer));
            try {
                conn.getController().join().id().join();
            } finally {
                conn.getStream().thenApply(s -> s.close());
            }
            return true;
        } catch (Exception e) {
            if (e instanceof Libp2pException && e.getMessage().contains("Transport is closed"))
                return false;
            else if (e.getCause() instanceof NothingToCompleteException || e.getCause() instanceof NonCompleteException)
                LOG.fine("Couldn't connect to " + peer.peerId);
            else
                e.printStackTrace();
            return false;
        }
    }

    public void bootstrap(Host us) {
        // lookup a random peer id
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        Multihash randomPeerId = new Multihash(Multihash.Type.sha2_256, hash);
        findClosestPeers(randomPeerId, 20, us);

        // lookup our own peer id to keep our nearest neighbours up-to-date,
        // and connect to all of them, so they know about our addresses
        List<PeerAddresses> closestToUs = findClosestPeers(Multihash.deserialize(us.getPeerId().getBytes()), 20, us);
        List<Multihash> connected = new ArrayList<>();
        for (PeerAddresses peer : closestToUs) {
            if (connectTo(us, peer))
                connected.add(peer.peerId);
        }
        LOG.info("Bootstrap connected to " + connected.size() + " nodes close to us. "
                + connected.stream().map(Multihash::toString).sorted().limit(5).collect(Collectors.toList()));

        List<Connection> allConns = us.getNetwork().getConnections();
        Set<Connection> activeConns = us.getStreams().stream().map(s -> s.getConnection()).collect(Collectors.toSet());
        List<Connection> toClose = allConns.stream().filter(c -> !activeConns.contains(c)).collect(Collectors.toList());
        for (Connection conn : toClose) {
            conn.close();
        }
    }

    private static <V> CompletableFuture<V> closeAfter(CompletableFuture<io.libp2p.core.Stream> sf, Supplier<CompletableFuture<V>> query) {
        CompletableFuture<V> res = new CompletableFuture<>();
        query.get().thenAccept(v -> {
            sf.thenAccept(s -> s.close());
            res.complete(v);
        }).exceptionally(t -> {
            sf.thenAccept(s -> s.close());
            res.completeExceptionally(t);
            return null;
        });
        return res;
    }

    static class RoutingEntry {
        public final Id key;
        public final PeerAddresses addresses;

        public RoutingEntry(Id key, PeerAddresses addresses) {
            this.key = key;
            this.addresses = addresses;
        }
    }

    private int compareKeys(RoutingEntry a, RoutingEntry b, Id keyId) {
        int prefixDiff = b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId);
        if (prefixDiff != 0)
            return prefixDiff;
        return a.addresses.peerId.toBase58().compareTo(b.addresses.peerId.toBase58());
    }

    private final ExecutorService ioExec = Executors.newFixedThreadPool(16);

    public List<PeerAddresses> findClosestPeers(Multihash peerIdkey, int maxCount, Host us) {
        if (maxCount == 1) {
            Collection<Multiaddr> existing = addressBook.get(PeerId.fromBase58(peerIdkey.toBase58())).join();
            if (!existing.isEmpty())
                return Collections.singletonList(new PeerAddresses(peerIdkey, new ArrayList<>(existing)));
        }
        byte[] key = peerIdkey.toBytes();
        return findClosestPeers(key, maxCount, us);
    }

    public List<PeerAddresses> findClosestPeers(byte[] key, int maxCount, Host us) {
        Id keyId = Id.create(Hash.sha256(key), 256);
        SortedSet<RoutingEntry> closest = Collections.synchronizedSortedSet(new TreeSet<>((a, b) -> compareKeys(a, b, keyId)));
        SortedSet<RoutingEntry> toQuery = Collections.synchronizedSortedSet(new TreeSet<>((a, b) -> compareKeys(a, b, keyId)));
        List<PeerAddresses> localClosest = engine.getKClosestPeers(key, Math.max(6, maxCount));
        if (maxCount == 1) {
            Optional<PeerAddresses> match = localClosest.stream().filter(p -> Arrays.equals(p.peerId.toBytes(), key)).findFirst();
            if (match.isPresent())
                return Collections.singletonList(match.get());
        }
        closest.addAll(localClosest.stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        toQuery.addAll(closest);
        if (toQuery.isEmpty())
            LOG.info("Couldn't find any local peers in kademlia routing table");
        Set<Multihash> queried = Collections.synchronizedSet(new HashSet<>());
        int queryParallelism = 3;
        while (true) {
            List<RoutingEntry> thisRound = toQuery.stream()
                    .filter(r -> hasTransportOverlap(r.addresses))
                    .limit(queryParallelism)
                    .collect(Collectors.toList());
            List<Future<List<PeerAddresses>>> futures = thisRound.stream()
                    .map(r -> {
                        toQuery.remove(r);
                        queried.add(r.addresses.peerId);
                        return ioExec.submit(() -> getCloserPeers(key, r.addresses, us)
                                .orTimeout(2, TimeUnit.SECONDS)
                                .join());
                    })
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (Future<List<PeerAddresses>> future : futures) {
                try {
                    List<PeerAddresses> result = future.get();
                    for (PeerAddresses peer : result) {
                        if (!queried.contains(peer.peerId)) {
                            if (maxCount == 1 && Arrays.equals(peer.peerId.toBytes(), key))
                                return Collections.singletonList(peer);
                            queried.add(peer.peerId);
                            Id peerKey = Id.create(Hash.sha256(peer.peerId.toBytes()), 256);
                            RoutingEntry e = new RoutingEntry(peerKey, peer);
                            toQuery.add(e);
                            closest.add(e);
                            foundCloser = true;
                        }
                    }
                } catch (Exception e) {
                    // couldn't contact peer
                }
            }
            if (!foundCloser)
                break;
        }
        return closest.stream()
                .limit(maxCount).map(r -> r.addresses)
                .collect(Collectors.toList());
    }

    public CompletableFuture<List<PeerAddresses>> findProviders(Multihash block, Host us, int desiredCount) {
        byte[] key = block.bareMultihash().toBytes();
        Id keyId = Id.create(key, 256);
        List<PeerAddresses> providers = new ArrayList<>();
        providers.addAll(engine.getProviders(block));

        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId));
        toQuery.addAll(engine.getKClosestPeers(key, 20).stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));

        Set<Multihash> queried = new HashSet<>();
        int queryParallelism = 3;
        while (true) {
            if (providers.size() >= desiredCount)
                return CompletableFuture.completedFuture(providers);
            List<RoutingEntry> queryThisRound = toQuery.stream().limit(queryParallelism).collect(Collectors.toList());
            toQuery.removeAll(queryThisRound);
            queryThisRound.forEach(r -> queried.add(r.addresses.peerId));
            List<CompletableFuture<Providers>> futures = queryThisRound.stream()
                    .parallel()
                    .map(r -> {
                        StreamPromise<? extends KademliaController> conn = null;
                        try {
                            conn = dialPeer(r.addresses, us);
                            return conn.getController().join()
                                    .getProviders(block).orTimeout(2, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            return null;
                        } finally {
                            if (conn != null)
                                conn.getStream().thenApply(s -> s.close());
                        }
                    }).filter(prov -> prov != null)
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (CompletableFuture<Providers> future : futures) {
                try {
                    Providers newProviders = future.join();
                    providers.addAll(newProviders.providers);
                    for (PeerAddresses peer : newProviders.closerPeers) {
                        if (!queried.contains(peer.peerId)) {
                            queried.add(peer.peerId);
                            RoutingEntry e = new RoutingEntry(Id.create(Hash.sha256(peer.peerId.toBytes()), 256), peer);
                            toQuery.add(e);
                            foundCloser = true;
                        }
                    }
                } catch (Exception e) {
                    if (!(e.getCause() instanceof TimeoutException))
                        e.printStackTrace();
                }
            }
            if (!foundCloser)
                break;
        }
        return CompletableFuture.completedFuture(providers);
    }

    private CompletableFuture<List<PeerAddresses>> getCloserPeers(byte[] key, PeerAddresses target, Host us) {
        try {
            StreamPromise<? extends KademliaController> conn = dialPeer(target, us);
            KademliaController contr = conn.getController().orTimeout(2, TimeUnit.SECONDS).join();
            return closeAfter(conn.getStream(), () -> contr.closerPeers(key));
        } catch (Exception e) {
            if (e instanceof Libp2pException && e.getMessage().contains("Transport is closed"))
                return CompletableFuture.completedFuture(Collections.emptyList());
            if (!quicEnabled && target.addresses.stream().allMatch(a -> a.toString().contains("quic")))
                return CompletableFuture.completedFuture(Collections.emptyList());
            if (e instanceof NoSuchRemoteProtocolException || e.getCause() instanceof NoSuchRemoteProtocolException)
                return CompletableFuture.completedFuture(Collections.emptyList());
            if (e.getCause() instanceof NothingToCompleteException || e.getCause() instanceof NonCompleteException)
                LOG.fine("Couldn't dial " + target.peerId + " addrs: " + target.addresses);
            else if (e.getCause() instanceof TimeoutException)
                LOG.fine("Timeout dialing " + target.peerId + " addrs: " + target.addresses);
            else if (e.getCause() instanceof ConnectionClosedException) {}
            else if (e.getCause() instanceof ClosedChannelException) {}
            else
                e.printStackTrace();
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    private Multiaddr[] getPublic(PeerAddresses target) {
        return target.addresses.stream()
                .filter(a -> localDht || PeerAddresses.isPublic(a, false))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
    }

    private StreamPromise<? extends KademliaController> dialPeer(PeerAddresses target, Host us) {
        Multiaddr[] multiaddrs = target.addresses.stream()
                .map(a -> Multiaddr.fromString(a.toString()))
                .filter(a -> !a.has(Protocol.DNS) && !a.has(Protocol.DNS4) && !a.has(Protocol.DNS6))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
        return dial(us, PeerId.fromBase58(target.peerId.toBase58()), multiaddrs);
    }

    public CompletableFuture<Void> provideBlock(Multihash block, Host us, PeerAddresses ourAddrs) {
        List<PeerAddresses> closestPeers = findClosestPeers(block, 20, us);
        List<CompletableFuture<Boolean>> provides = closestPeers.stream()
                .parallel()
                .map(p -> {
                    StreamPromise<? extends KademliaController> conn = dialPeer(p, us);
                    return conn.getController()
                            .thenCompose(contr -> contr.provide(block, ourAddrs))
                            .thenApply(res -> {
                                conn.getStream().thenApply(s -> s.close());
                                return res;
                            })
                            .exceptionally(t -> {
                                if (t.getCause() instanceof NonCompleteException)
                                    return true;
                                LOG.log(Level.FINE, t, t::getMessage);
                                conn.getStream().thenApply(s -> s.close());
                                return true;
                            });
                })
                .collect(Collectors.toList());
        return CompletableFuture.allOf(provides.toArray(new CompletableFuture[0]));
    }

    private boolean hasTransportOverlap(PeerAddresses p) {
        return p.addresses.stream()
                .anyMatch(a -> (
                        (tcpEnabled && a.has(Protocol.TCP)) ||
                        (quicEnabled && a.has(Protocol.QUICV1)) && !a.has(Protocol.WEBTRANSPORT)) &&
                        !a.has(Protocol.P2PCIRCUIT));
    }
}
