package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.protocol.*;
import org.peergos.protocol.dht.*;

import java.util.*;

public class KademliaTest {

    @Ignore
    @Test
    public void findOtherNode() throws Exception {
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore());
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1, Collections.emptyList());

        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore());
        Host node2 = builder2.build();
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2, Collections.emptyList());

        try {
            Kademlia dht2 = builder2.getWanDht().get();
            dht2.bootstrapRoutingTable(node2, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht2.bootstrap(node2);

            Kademlia dht1 = builder1.getWanDht().get();
            dht1.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht1.bootstrap(node1);

            Multihash peerId2 = Multihash.deserialize(node2.getPeerId().getBytes());
            List<PeerAddresses> closestPeers = dht1.findClosestPeers(peerId2, 2, node1);
            Optional<PeerAddresses> matching = closestPeers.stream()
                    .filter(p -> p.peerId.equals(peerId2))
                    .findFirst();
            if (matching.isEmpty())
                throw new IllegalStateException("Couldn't find node2!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    @Test
    public void kademliaFindNodeLimitTest() {
        PeerId us = new HostBuilder(new RamAddressBook()).generateIdentity().getPeerId();
        KademliaEngine kad = new KademliaEngine(
                Multihash.fromBase58(us.toBase58()),
                new RamProviderStore(1000),
                new RamRecordStore());
        RamAddressBook addrs = new RamAddressBook();
        kad.setAddressBook(addrs);
        for (int i = 0; i < 1000; i++) {
            PeerId peer = new HostBuilder(new RamAddressBook()).generateIdentity().getPeerId();
            for (int j = 0; j < 100; j++) {
                kad.addIncomingConnection(peer);
                addrs.addAddrs(peer, 0, new Multiaddr[]{new Multiaddr("/ip4/127.0.0.1/tcp/4001/p2p/" + peer.toBase58())});
            }
        }
        List<PeerAddresses> closest = kad.getKClosestPeers(new byte[32], 20);
        Assert.assertTrue(closest.size() <= 20);
        for (PeerAddresses addr : closest) {
            Assert.assertTrue(addr.addresses.size() == 1);
        }
    }
}
