// Copyright 2019 The Swarm Authors
// This file is part of the Swarm library.
//
// The Swarm library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Swarm library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Swarm library. If not, see <http://www.gnu.org/licenses/>.

package bzzeth

import (
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	p2ptest "github.com/ethersphere/swarm/p2p/testing"
	"github.com/ethersphere/swarm/testutil"
)

func init() {
	testutil.Init()
}

func newBzzEthTester() (*p2ptest.ProtocolTester, *BzzEth, func(), error) {
	b := New()

	prvkey, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, nil, err
	}

	protocolTester := p2ptest.NewProtocolTester(prvkey, 1, b.Run)
	teardown := func() {
		protocolTester.Stop()
	}

	return protocolTester, b, teardown, nil
}

func handshakeExchange(tester *p2ptest.ProtocolTester, peerID enode.ID, serveHeadersPeer, serveHeadersPivot bool) error {
	return tester.TestExchanges(
		p2ptest.Exchange{
			Label: "Handshake",
			Triggers: []p2ptest.Trigger{
				{
					Code: 0,
					Msg: Handshake{
						ServeHeaders: serveHeadersPeer,
					},
					Peer: peerID,
				},
			},
			Expects: []p2ptest.Expect{
				{
					Code: 0,
					Msg: Handshake{
						ServeHeaders: serveHeadersPivot,
					},
					Peer: peerID,
				},
			},
		})
}

// This message is exchanged between two Swarm nodes to check if the connection drops
func dummyHandshakeMessage(tester *p2ptest.ProtocolTester, peerID enode.ID) error {
	return tester.TestExchanges(
		p2ptest.Exchange{
			Label: "Handshake",
			Triggers: []p2ptest.Trigger{
				{
					Code: 0,
					Msg: Handshake{
						ServeHeaders: true,
					},
					Peer: peerID,
				},
			},
		})
}

// tests handshake between eth node and swarm node
// on successful handshake the protocol does not go idle
// peer added to the pool and serves headers is registered
func TestBzzEthHandshake(t *testing.T) {
	tester, b, teardown, err := newBzzEthTester()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	node := tester.Nodes[0]
	err = handshakeExchange(tester, node.ID(), true, true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// after successful handshake, expect peer added to peer pool
	p := getPeerAfterConnection(node.ID(), b)
	if p == nil {
		t.Fatal("bzzeth peer not added")
	}
	if !p.serveHeaders {
		t.Fatal("bzzeth peer serveHeaders not set")
	}

	close(b.quit)
	err = tester.TestDisconnected(&p2ptest.Disconnect{Peer: node.ID(), Error: errors.New("?")})
	if err == nil || err.Error() != "timed out waiting for peers to disconnect" {
		t.Fatal(err)
	}
}

// TestBzzBzzHandshake tests that a handshake between two Swarm nodes
func TestBzzBzzHandshake(t *testing.T) {
	// redefine isSwarmNodeFunc to force recognise remote peer as swarm node
	defer func(f func(*Peer) bool) {
		isSwarmNodeFunc = f
	}(isSwarmNodeFunc)
	isSwarmNodeFunc = func(_ *Peer) bool { return true }

	tester, b, teardown, err := newBzzEthTester()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	node := tester.Nodes[0]
	err = handshakeExchange(tester, node.ID(), false, true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// after successful handshake, expect peer added to peer pool
	p := getPeerAfterConnection(node.ID(), b)
	if p == nil {
		t.Fatal("bzzeth peer not added")
	}

	// after closing the protocol, expect disconnect
	close(b.quit)
	err = tester.TestDisconnected(&p2ptest.Disconnect{Peer: node.ID(), Error: errors.New("?")})
	if err == nil || err.Error() != "timed out waiting for peers to disconnect" {
		t.Fatal(err)
	}
}

// TestBzzBzzHandshakeWithMessage tests that a handshake between two Swarm nodes and message exchange
// disconnects the peer
func TestBzzBzzHandshakeWithMessage(t *testing.T) {
	// redefine isSwarmNodeFunc to force recognise remote peer as swarm node
	defer func(f func(*Peer) bool) {
		isSwarmNodeFunc = f
	}(isSwarmNodeFunc)
	isSwarmNodeFunc = func(_ *Peer) bool { return true }

	tester, b, teardown, err := newBzzEthTester()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	node := tester.Nodes[0]
	err = handshakeExchange(tester, node.ID(), false, true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// after successful handshake, expect peer added to peer pool
	var p *Peer
	for i := 0; i < 10; i++ {
		p = b.peers.get(node.ID())
		if p != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if p == nil {
		t.Fatal("bzzeth peer not added")
	}

	// Send a dummy handshake message, wait for sometime and check if peer is dropped
	err = dummyHandshakeMessage(tester, node.ID())
	if err != nil {
		t.Fatal(err)
	}
	// after successful handshake, expect peer added to peer pool
	p1 := isPeerDisconnected(node.ID(), b)
	if p1 != nil {
		t.Fatal("bzzeth peer still connected")
	}
}

func getPeerAfterConnection(id enode.ID, b *BzzEth) (p *Peer) {
	for i := 0; i < 10; i++ {
		p = b.peers.get(id)
		if p != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return
}

func isPeerDisconnected(id enode.ID, b *BzzEth) (p *Peer) {
	var p1 *Peer
	for i := 0; i < 10; i++ {
		p1 = b.peers.get(id)
		if p1 == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return
}
