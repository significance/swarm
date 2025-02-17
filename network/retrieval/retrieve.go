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

package retrieval

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/network"
	"github.com/ethersphere/swarm/network/timeouts"
	"github.com/ethersphere/swarm/p2p/protocols"
	"github.com/ethersphere/swarm/spancontext"
	"github.com/ethersphere/swarm/storage"
	"github.com/ethersphere/swarm/swap"
	opentracing "github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
)

var (
	// Compile time interface check
	_ node.Service = &Retrieval{}

	// Metrics
	processReceivedChunksCount    = metrics.NewRegisteredCounter("network.retrieve.received_chunks_handled", nil)
	handleRetrieveRequestMsgCount = metrics.NewRegisteredCounter("network.retrieve.handle_retrieve_request_msg", nil)
	retrieveChunkFail             = metrics.NewRegisteredCounter("network.retrieve.retrieve_chunks_fail", nil)

	retrievalPeers = metrics.GetOrRegisterGauge("network.retrieve.peers", nil)

	spec = &protocols.Spec{
		Name:       "bzz-retrieve",
		Version:    1,
		MaxMsgSize: 10 * 1024 * 1024,
		Messages: []interface{}{
			ChunkDelivery{},
			RetrieveRequest{},
		},
	}

	ErrNoPeerFound = errors.New("no peer found")
)

// Price is the method through which a message type marks itself
// as implementing the protocols.Price protocol and thus
// as swap-enabled message
func (rr *RetrieveRequest) Price() *protocols.Price {
	return &protocols.Price{
		Value:   swap.RetrieveRequestPrice,
		PerByte: false,
		Payer:   protocols.Sender,
	}
}

// Price is the method through which a message type marks itself
// as implementing the protocols.Price protocol and thus
// as swap-enabled message
func (cd *ChunkDelivery) Price() *protocols.Price {
	return &protocols.Price{
		Value:   swap.ChunkDeliveryPrice,
		PerByte: true,
		Payer:   protocols.Receiver,
	}
}

// Retrieval holds state and handles protocol messages for the `bzz-retrieve` protocol
type Retrieval struct {
	mtx      sync.RWMutex
	netStore *storage.NetStore
	kad      *network.Kademlia
	peers    map[enode.ID]*Peer
	spec     *protocols.Spec
	logger   log.Logger
	quit     chan struct{}
}

// New returns a new instance of the retrieval protocol handler
func New(kad *network.Kademlia, ns *storage.NetStore, baseKey []byte, balance protocols.Balance) *Retrieval {
	r := &Retrieval{
		kad:      kad,
		peers:    make(map[enode.ID]*Peer),
		spec:     spec,
		netStore: ns,
		logger:   log.New("base", hex.EncodeToString(baseKey)[:16]),
		quit:     make(chan struct{}),
	}
	if balance != nil && !reflect.ValueOf(balance).IsNil() {
		// swap is enabled, so setup the hook
		r.spec.Hook = protocols.NewAccounting(balance)
	}
	return r
}

func (r *Retrieval) addPeer(p *Peer) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.peers[p.ID()] = p
	retrievalPeers.Update(int64(len(r.peers)))
}

func (r *Retrieval) removePeer(p *Peer) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	delete(r.peers, p.ID())
	retrievalPeers.Update(int64(len(r.peers)))
}

func (r *Retrieval) getPeer(id enode.ID) *Peer {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	return r.peers[id]
}

// Run is being dispatched when 2 nodes connect
func (r *Retrieval) Run(bp *network.BzzPeer) error {
	sp := NewPeer(bp)
	r.addPeer(sp)
	defer r.removePeer(sp)

	return sp.Peer.Run(r.handleMsg(sp))
}

func (r *Retrieval) handleMsg(p *Peer) func(context.Context, interface{}) error {
	return func(ctx context.Context, msg interface{}) error {
		switch msg := msg.(type) {
		case *RetrieveRequest:
			// we must handle them in a different goroutine otherwise parallel requests
			// for other chunks from the same peer will get stuck in the queue
			go r.handleRetrieveRequest(ctx, p, msg)
		case *ChunkDelivery:
			go r.handleChunkDelivery(ctx, p, msg)
		}
		return nil
	}
}

// getOriginPo returns the originPo if the incoming Request has an Origin
// if our node is the first node that requests this chunk, then we don't have an Origin,
// and return -1
// this is used only for tracing, and can probably be refactor so that we don't have to
// iterater over Kademlia
func (r *Retrieval) getOriginPo(req *storage.Request) int {
	r.logger.Trace("retrieval.getOriginPo", "req.Addr", req.Addr)
	originPo := -1

	r.kad.EachConn(req.Addr[:], 255, func(p *network.Peer, po int) bool {
		id := p.ID()

		// get po between chunk and origin
		if bytes.Equal(req.Origin.Bytes(), id.Bytes()) {
			originPo = po
			return false
		}

		return true
	})

	return originPo
}

// findPeer finds a peer we need to ask for a specific chunk from according to our kademlia
func (r *Retrieval) findPeer(ctx context.Context, req *storage.Request) (retPeer *network.Peer, err error) {
	r.logger.Trace("retrieval.findPeer", "req.Addr", req.Addr)
	osp, _ := ctx.Value("remote.fetch").(opentracing.Span)

	// originPo - proximity of the node that made the request; -1 if the request originator is our node;
	// myPo - this node's proximity with the requested chunk
	// selectedPeerPo - kademlia suggested node's proximity with the requested chunk (computed further below)
	originPo := r.getOriginPo(req)
	myPo := chunk.Proximity(req.Addr, r.kad.BaseAddr())
	selectedPeerPo := -1

	depth := r.kad.NeighbourhoodDepth()

	if osp != nil {
		osp.LogFields(olog.Int("originPo", originPo))
		osp.LogFields(olog.Int("depth", depth))
		osp.LogFields(olog.Int("myPo", myPo))
	}

	// do not forward requests if origin proximity is bigger than our node's proximity
	// this means that origin is closer to the chunk
	if originPo > myPo {
		return nil, errors.New("not forwarding request, origin node is closer to chunk than this node")
	}

	r.kad.EachConn(req.Addr[:], 255, func(p *network.Peer, po int) bool {
		id := p.ID()

		if !p.HasCap(r.spec.Name) {
			return true
		}

		// do not send request back to peer who asked us. maybe merge with SkipPeer at some point
		if bytes.Equal(req.Origin.Bytes(), id.Bytes()) {
			return true
		}

		// skip peers that we have already tried
		if req.SkipPeer(id.String()) {
			r.logger.Trace("findpeer skip peer", "peer", id, "ref", req.Addr.String())
			return true
		}

		if myPo < depth { //  chunk is NOT within the neighbourhood
			if po <= myPo { // always choose a peer strictly closer to chunk than us
				r.logger.Trace("findpeer1a", "originpo", originPo, "mypo", myPo, "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())
				return false
			} else {
				r.logger.Trace("findpeer1b", "originpo", originPo, "mypo", myPo, "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())
			}
		} else { // chunk IS WITHIN neighbourhood
			if po < depth { // do not select peer outside the neighbourhood. But allows peers further from the chunk than us
				r.logger.Trace("findpeer2a", "originpo", originPo, "mypo", myPo, "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())
				return false
			} else if po <= originPo { // avoid loop in neighbourhood, so not forward when a request comes from the neighbourhood
				r.logger.Trace("findpeer2b", "originpo", originPo, "mypo", myPo, "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())
				return false
			} else {
				r.logger.Trace("findpeer2c", "originpo", originPo, "mypo", myPo, "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())
			}
		}

		// if selected peer is not in the depth (2nd condition; if depth <= po, then peer is in nearest neighbourhood)
		// and they have a lower po than ours, return error
		if po < myPo && depth > po {
			r.logger.Trace("findpeer4 skip peer because origin was closer", "originpo", originPo, "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())

			err = fmt.Errorf("not asking peers further away from origin; ref=%s originpo=%v po=%v depth=%v myPo=%v", req.Addr.String(), originPo, po, depth, myPo)
			return false
		}

		// if chunk falls in our nearest neighbourhood (1st condition), but suggested peer is not in
		// the nearest neighbourhood (2nd condition), don't forward the request to suggested peer
		if depth <= myPo && depth > po {
			r.logger.Trace("findpeer5 skip peer because depth", "originpo", originPo, "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())

			err = fmt.Errorf("not going outside of depth; ref=%s originpo=%v po=%v depth=%v myPo=%v", req.Addr.String(), originPo, po, depth, myPo)
			return false
		}

		retPeer = p

		// sp could be nil, if we encountered a peer that is not registered for delivery, i.e. doesn't support the `stream` protocol
		// if sp is not nil, then we have selected the next peer and we stop iterating
		// if sp is nil, we continue iterating
		if retPeer != nil {
			selectedPeerPo = po

			return false
		}

		// continue iterating
		return true
	})

	if osp != nil {
		osp.LogFields(olog.Int("selectedPeerPo", selectedPeerPo))
	}

	if err != nil {
		return nil, err
	}

	if retPeer == nil {
		return nil, ErrNoPeerFound
	}

	return retPeer, nil
}

// handleRetrieveRequest handles an incoming retrieve request from a certain Peer
// if the chunk is found in the localstore it is served immediately, otherwise
// it results in a new retrieve request to candidate peers in our kademlia
func (r *Retrieval) handleRetrieveRequest(ctx context.Context, p *Peer, msg *RetrieveRequest) {
	p.logger.Debug("retrieval.handleRetrieveRequest", "ref", msg.Addr)
	handleRetrieveRequestMsgCount.Inc(1)

	ctx, osp := spancontext.StartSpan(
		ctx,
		"handle.retrieve.request")

	osp.LogFields(olog.String("ref", msg.Addr.String()))

	defer osp.Finish()

	ctx, cancel := context.WithTimeout(ctx, timeouts.FetcherGlobalTimeout)
	defer cancel()

	req := &storage.Request{
		Addr:   msg.Addr,
		Origin: p.ID(),
	}
	chunk, err := r.netStore.Get(ctx, chunk.ModeGetRequest, req)
	if err != nil {
		retrieveChunkFail.Inc(1)
		p.logger.Error("netstore.Get can not retrieve chunk", "ref", msg.Addr, "err", err)
		return
	}

	p.logger.Trace("retrieval.handleRetrieveRequest - delivery", "ref", msg.Addr)

	deliveryMsg := &ChunkDelivery{
		Addr:  chunk.Address(),
		SData: chunk.Data(),
	}

	err = p.Send(ctx, deliveryMsg)
	if err != nil {
		p.logger.Error("retrieval.handleRetrieveRequest - peer delivery failed", "ref", msg.Addr, "err", err)
		osp.LogFields(olog.Bool("delivered", false))
		return
	}
	osp.LogFields(olog.Bool("delivered", true))
}

// handleChunkDelivery handles a ChunkDelivery message from a certain peer
// if the chunk proximity order in relation to our base address is within depth
// we treat the chunk as a chunk received in syncing
func (r *Retrieval) handleChunkDelivery(ctx context.Context, p *Peer, msg *ChunkDelivery) {
	p.logger.Debug("retrieval.handleChunkDelivery", "ref", msg.Addr)
	var osp opentracing.Span
	ctx, osp = spancontext.StartSpan(
		ctx,
		"handle.chunk.delivery")

	processReceivedChunksCount.Inc(1)

	// count how many chunks we receive for retrieve requests per peer
	peermetric := fmt.Sprintf("network.retrieve.chunk.delivery.%x", p.BzzAddr.Over()[:16])
	metrics.GetOrRegisterCounter(peermetric, nil).Inc(1)

	peerPO := chunk.Proximity(p.BzzAddr.Over(), msg.Addr)
	po := chunk.Proximity(r.kad.BaseAddr(), msg.Addr)
	depth := r.kad.NeighbourhoodDepth()
	var mode chunk.ModePut
	// chunks within the area of responsibility should always sync
	// https://github.com/ethersphere/go-ethereum/pull/1282#discussion_r269406125
	if po >= depth || peerPO < po {
		mode = chunk.ModePutSync
	} else {
		// do not sync if peer that is sending us a chunk is closer to the chunk then we are
		mode = chunk.ModePutRequest
	}
	defer osp.Finish()

	_, err := r.netStore.Put(ctx, mode, storage.NewChunk(msg.Addr, msg.SData))
	if err != nil {
		p.logger.Error("netstore error putting chunk to localstore", "err", err)
		if err == storage.ErrChunkInvalid {
			p.Drop()
		}
	}
}

// RequestFromPeers sends a chunk retrieve request to the next found peer
func (r *Retrieval) RequestFromPeers(ctx context.Context, req *storage.Request, localID enode.ID) (*enode.ID, error) {
	r.logger.Debug("retrieval.requestFromPeers", "req.Addr", req.Addr, "localID", localID)
	metrics.GetOrRegisterCounter("network.retrieve.request_from_peers", nil).Inc(1)

	const maxFindPeerRetries = 5
	retries := 0

FINDPEER:
	sp, err := r.findPeer(ctx, req)
	if err != nil {
		r.logger.Error(err.Error())
		return nil, err
	}

	protoPeer := r.getPeer(sp.ID())
	if protoPeer == nil {
		r.logger.Warn("findPeer returned a peer to skip", "peer", sp.String(), "retry", retries, "ref", req.Addr)
		req.PeersToSkip.Store(sp.ID().String(), time.Now())
		retries++
		if retries == maxFindPeerRetries {
			r.logger.Error("max find peer retries reached", "max retries", maxFindPeerRetries, "ref", req.Addr)
			return nil, ErrNoPeerFound
		}

		goto FINDPEER
	}

	ret := &RetrieveRequest{
		Addr: req.Addr,
	}
	protoPeer.logger.Trace("sending retrieve request", "ref", ret.Addr, "origin", localID)
	err = protoPeer.Send(ctx, ret)
	if err != nil {
		protoPeer.logger.Error("error sending retrieve request to peer", "err", err)
		return nil, err
	}

	spID := protoPeer.ID()
	return &spID, nil
}

func (r *Retrieval) Start(server *p2p.Server) error {
	r.logger.Info("starting bzz-retrieve")
	return nil
}

func (r *Retrieval) Stop() error {
	r.logger.Info("shutting down bzz-retrieve")
	close(r.quit)
	return nil
}

func (r *Retrieval) Protocols() []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    r.spec.Name,
			Version: r.spec.Version,
			Length:  r.spec.Length(),
			Run:     r.runProtocol,
		},
	}
}

func (r *Retrieval) runProtocol(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	peer := protocols.NewPeer(p, rw, r.spec)
	bp := network.NewBzzPeer(peer)

	return r.Run(bp)
}

func (r *Retrieval) APIs() []rpc.API {
	return nil
}

func (r *Retrieval) Spec() *protocols.Spec {
	return r.spec
}
