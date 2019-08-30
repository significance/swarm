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

package newstream

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/network"
	bv "github.com/ethersphere/swarm/network/bitvector"
	"github.com/ethersphere/swarm/p2p/protocols"
	"github.com/ethersphere/swarm/state"
	"github.com/ethersphere/swarm/storage"
)

const (
	HashSize  = 32
	BatchSize = 32
)

var (
	// Compile time interface check
	_ node.Service = (*Registry)(nil)

	// Metrics
	processReceivedChunksMsgCount = metrics.GetOrRegisterCounter("network.stream.received_chunks_msg", nil)
	processReceivedChunksCount    = metrics.GetOrRegisterCounter("network.stream.received_chunks_handled", nil)
	streamSeenChunkDelivery       = metrics.GetOrRegisterCounter("network.stream.seen_chunk_delivery", nil)
	streamEmptyWantedHashes       = metrics.GetOrRegisterCounter("network.stream.empty_wanted_hashes", nil)
	streamWantedHashes            = metrics.GetOrRegisterCounter("network.stream.wanted_hashes", nil)

	streamBatchFail               = metrics.GetOrRegisterCounter("network.stream.batch_fail", nil)
	streamChunkDeliveryFail       = metrics.GetOrRegisterCounter("network.stream.delivery_fail", nil)
	streamRequestNextIntervalFail = metrics.GetOrRegisterCounter("network.stream.next_interval_fail", nil)

	headBatchSizeGauge    = metrics.GetOrRegisterGauge("network.stream.batch_size_head", nil)
	batchSizeGauge        = metrics.GetOrRegisterGauge("network.stream.batch_size", nil)
	lastReceivedChunksMsg = metrics.GetOrRegisterGauge("network.stream.received_chunks", nil)

	streamPeersCount = metrics.GetOrRegisterGauge("network.stream.peers", nil)

	collectBatchLiveTimer    = metrics.GetOrRegisterResettingTimer("network.stream.server_collect_batch_head.total-time", nil)
	collectBatchHistoryTimer = metrics.GetOrRegisterResettingTimer("network.stream.server_collect_batch.total-time", nil)
	actualGetTimer           = metrics.GetOrRegisterResettingTimer("network.stream.provider_get_time", nil)
	activeBatchTimeout       = 20 * time.Second

	// Protocol spec
	Spec = &protocols.Spec{
		Name:       "bzz-stream",
		Version:    8,
		MaxMsgSize: 10 * 1024 * 1024,
		Messages: []interface{}{
			StreamInfoReq{},
			StreamInfoRes{},
			GetRange{},
			OfferedHashes{},
			ChunkDelivery{},
			WantedHashes{},
		},
	}
)

// Registry is the base type that handles all client/server operations on a node
// it is instantiated once per stream protocol instance, that is, it should have
// one instance per node
type Registry struct {
	mtx            sync.RWMutex
	intervalsStore state.Store
	peers          map[enode.ID]*Peer
	baseKey        []byte

	providers map[string]StreamProvider

	spec *protocols.Spec

	handlersWg sync.WaitGroup // waits for all handlers to finish in Close method
	quit       chan struct{}

	logger log.Logger
}

// New creates a new stream protocol handler
func New(intervalsStore state.Store, baseKey []byte, providers ...StreamProvider) *Registry {
	r := &Registry{
		intervalsStore: intervalsStore,
		peers:          make(map[enode.ID]*Peer),
		providers:      make(map[string]StreamProvider),
		quit:           make(chan struct{}),
		baseKey:        baseKey,
		logger:         log.New("base", hex.EncodeToString(baseKey)),
		spec:           Spec,
	}
	for _, p := range providers {
		r.providers[p.StreamName()] = p
	}

	return r
}

// Run is being dispatched when 2 nodes connect
func (r *Registry) Run(bp *network.BzzPeer) error {
	sp := NewPeer(bp, r.baseKey, r.intervalsStore, r.providers)
	r.addPeer(sp)
	defer r.removePeer(sp)

	go sp.InitProviders()

	return sp.Peer.Run(r.HandleMsg(sp))
}

// HandleMsg is the main message handler for the stream protocol
func (r *Registry) HandleMsg(p *Peer) func(context.Context, interface{}) error {
	return func(ctx context.Context, msg interface{}) error {
		r.mtx.Lock() // ensure that quit read and handlersWg add are locked together
		defer r.mtx.Unlock()

		select {
		case <-r.quit:
			// no message handling if we quit
			return nil
		case <-p.quit:
			// peer has been removed, quit
			return nil
		default:
		}

		// handleMsgPauser should not be nil only in tests.
		// It does not use mutex lock protection and because of that
		// it must be set before the Registry is constructed and
		// reset when it is closed, in tests.
		// Production performance impact can be considered as
		// neglectable as nil check is a ns order operation.
		if handleMsgPauser != nil {
			handleMsgPauser.wait()
		}

		r.handlersWg.Add(1)
		go func() {
			defer r.handlersWg.Done()

			switch msg := msg.(type) {
			case *StreamInfoReq:
				r.serverHandleStreamInfoReq(ctx, p, msg)
			case *StreamInfoRes:
				if len(msg.Streams) == 0 {
					p.logger.Error("StreamInfo response is empty")
					p.Drop()
					return
				}

				r.clientHandleStreamInfoRes(ctx, p, msg)
			case *GetRange:

				provider := r.getProvider(msg.Stream)
				if provider == nil {
					p.logger.Error("unsupported provider", "stream", msg.Stream)
					p.Drop()
					return
				}
				if msg.To == nil {
					// handle live
					r.serverHandleGetRangeHead(ctx, p, msg, provider)
				} else {
					// handle bound range
					r.serverHandleGetRange(ctx, p, msg, provider)
				}
			case *OfferedHashes:
				w, exit := p.getWantOrDrop(msg.Ruid)
				if exit {
					return
				}
				provider := r.getProvider(w.stream)
				if provider == nil {
					p.logger.Error("unsupported provider", "stream", w.stream)
					p.Drop()
					return
				}
				r.clientHandleOfferedHashes(ctx, p, msg, w, provider)
			case *WantedHashes:
				o, exit := p.getOfferOrDrop(msg.Ruid)
				if exit {
					return
				}
				provider := r.getProvider(o.stream)
				if provider == nil {
					p.logger.Error("unsupported provider", "stream", o.stream)
					p.Drop()
					return
				}
				r.serverHandleWantedHashes(ctx, p, msg, o, provider)
			case *ChunkDelivery:
				w, exit := p.getWantOrDrop(msg.Ruid)
				if exit {
					streamChunkDeliveryFail.Inc(1)
					return
				}
				provider := r.getProvider(w.stream)
				if provider == nil {
					p.logger.Error("unsupported provider", "stream", w.stream)
					p.Drop()
					return
				}
				r.clientHandleChunkDelivery(ctx, p, msg, w, provider)
			}
		}()
		return nil
	}
}

// Used to pause any message handling in tests for
// synchronizing desired states.
var handleMsgPauser pauser

type pauser interface {
	pause()
	resume()
	wait()
}

// serverHandleStreamInfoReq handles the StreamInfoReq message on the server side (Peer is the client)
func (r *Registry) serverHandleStreamInfoReq(ctx context.Context, p *Peer, msg *StreamInfoReq) {
	if len(msg.Streams) == 0 {
		p.logger.Error("nil streams msg requested")
		p.Drop()
		return
	}

	streamRes := StreamInfoRes{}
	for _, v := range msg.Streams {
		v := v
		provider := r.getProvider(v)
		if provider == nil {
			p.logger.Error("unsupported provider", "stream", v)
			// TODO: tell the other peer we dont support this stream. this is non fatal
			// this might not be fatal as we might not support all providers.
			return
		}

		streamCursor, err := provider.Cursor(v.Key)
		if err != nil {
			p.logger.Error("error getting cursor for stream key", "name", v.Name, "key", v.Key, "err", err)
			p.Drop()
			return
		}
		descriptor := StreamDescriptor{
			Stream:  v,
			Cursor:  streamCursor,
			Bounded: provider.Boundedness(),
		}
		streamRes.Streams = append(streamRes.Streams, descriptor)
	}

	select {
	case <-r.quit:
		// shutdown
		return
	case <-p.quit:
		// peer has been removed, quit
		return
	default:
	}

	if err := p.Send(ctx, streamRes); err != nil {
		p.logger.Error("failed to send StreamInfoRes to client", "err", err)
		p.Drop()
	}
}

// clientHandleStreamInfoRes handles the StreamInfoRes message (Peer is the server)
func (r *Registry) clientHandleStreamInfoRes(ctx context.Context, p *Peer, msg *StreamInfoRes) {
	for _, s := range msg.Streams {
		s := s
		provider, exit := r.getProviderOrDrop(s.Stream)
		if exit {
			return
		}
		if !provider.WantStream(p, s.Stream) {
			if _, exists := p.getCursor(s.Stream); exists {
				p.logger.Debug("stream cursor exists but we don't want it - removing", "stream", s.Stream)
				p.deleteCursor(s.Stream)
			}
			continue
		}

		if _, exists := p.getCursor(s.Stream); exists {
			p.logger.Debug("stream cursor already exists, continue to next", "stream", s.Stream)
			continue
		}

		p.logger.Debug("setting stream cursor", "stream", s.Stream, "cursor", s.Cursor)
		p.setCursor(s.Stream, s.Cursor)

		if provider.Autostart() {
			if s.Cursor > 0 {
				p.logger.Debug("requesting history stream", "stream", s.Stream, "cursor", s.Cursor)

				// fetch everything from beginning till s.Cursor
				go func() {
					err := r.clientRequestStreamRange(ctx, p, provider, s.Stream, s.Cursor)
					if err != nil {
						p.logger.Error("had an error sending initial GetRange for historical stream", "stream", s.Stream, "err", err)
						p.Drop()
					}
				}()
			}

			// handle stream unboundedness
			if !s.Bounded {
				//constantly fetch the head of the stream
				go func() {
					p.logger.Debug("asking for live stream", "stream", s.Stream, "cursor", s.Cursor)

					// ask the tip (cursor + 1)
					err := r.clientRequestStreamHead(ctx, p, s.Stream, s.Cursor+1)
					// https://github.com/golang/go/issues/4373 - use of closed network connection
					if err != nil && err != p2p.ErrShuttingDown && !strings.Contains(err.Error(), "use of closed network connection") {
						p.logger.Error("had an error with initial stream head fetch", "stream", s.Stream, "cursor", s.Cursor+1, "err", err)
						p.Drop()
					}
				}()
			}
		}
	}
}

// clientRequestStreamHead sends a GetRange message to the server requesting
// new chunks from the supplied cursor position
func (r *Registry) clientRequestStreamHead(ctx context.Context, p *Peer, stream ID, from uint64) error {
	p.logger.Debug("clientRequestStreamHead", "stream", stream, "from", from)
	return r.clientCreateSendWant(ctx, p, stream, from, nil, true)
}

// clientRequestStreamRange sends a GetRange message to the server requesting
// a bound interval of chunks starting from the current stored interval in the
// interval store and ending at most in the supplied cursor position
func (r *Registry) clientRequestStreamRange(ctx context.Context, p *Peer, provider StreamProvider, stream ID, cursor uint64) error {
	p.logger.Debug("clientRequestStreamRange", "stream", stream, "cursor", cursor)
	from, _, empty, err := p.nextInterval(stream, 0)
	if err != nil {
		return err
	}
	if from > cursor || empty {
		p.logger.Debug("peer.requestStreamRange stream finished", "stream", stream, "cursor", cursor)
		// stream finished. quit
		return nil
	}
	return r.clientCreateSendWant(ctx, p, stream, from, &cursor, false)
}

func (r *Registry) clientCreateSendWant(ctx context.Context, p *Peer, stream ID, from uint64, to *uint64, head bool) error {
	g := GetRange{
		Ruid:      uint(rand.Uint32()),
		Stream:    stream,
		From:      from,
		To:        to,
		BatchSize: BatchSize,
		Roundtrip: true,
	}

	p.mtx.Lock()
	p.openWants[g.Ruid] = &want{
		ruid:   g.Ruid,
		stream: g.Stream,
		from:   g.From,
		to:     to,
		head:   head,
		hashes: make(map[string]bool),
		chunks: make(chan chunk.Chunk),
		closeC: make(chan error),

		requested: time.Now(),
	}
	p.mtx.Unlock()

	return p.Send(ctx, g)
}

func (r *Registry) serverHandleGetRangeHead(ctx context.Context, p *Peer, msg *GetRange, provider StreamProvider) {
	p.logger.Debug("serverHandleGetRangeHead", "ruid", msg.Ruid)
	start := time.Now()
	defer func(start time.Time) {
		metrics.GetOrRegisterResettingTimer("network.stream.handle_get_range_head.total-time", nil).UpdateSince(start)
	}(start)

	key, err := provider.ParseKey(msg.Stream.Key)
	if err != nil {
		p.logger.Error("erroring parsing stream key", "stream", msg.Stream, "err", err)
		p.Drop()
		return
	}
	h, _, t, e, err := r.serverCollectBatch(ctx, p, provider, key, msg.From, 0)
	if err != nil {
		p.logger.Error("erroring getting live batch for stream", "stream", msg.Stream, "err", err)
		p.Drop()
		return
	}

	if e {
		// prevent sending an empty batch that resulted from db shutdown or peer quit
		select {
		case <-r.quit:
			return
		case <-p.quit:
			return
		default:
			offered := OfferedHashes{
				Ruid:      msg.Ruid,
				LastIndex: msg.From,
				Hashes:    []byte{},
			}
			if err := p.Send(ctx, offered); err != nil {
				p.logger.Error("erroring sending empty live offered hashes", "ruid", msg.Ruid, "err", err)
			}
			return
		}
	}

	p.mtx.Lock()
	p.openOffers[msg.Ruid] = offer{
		ruid:      msg.Ruid,
		stream:    msg.Stream,
		hashes:    h,
		requested: time.Now(),
	}
	p.mtx.Unlock()

	offered := OfferedHashes{
		Ruid:      msg.Ruid,
		LastIndex: t,
		Hashes:    h,
	}
	l := len(h) / HashSize
	headBatchSizeGauge.Update(int64(l))
	if err := p.Send(ctx, offered); err != nil {
		p.logger.Error("erroring sending offered hashes", "ruid", msg.Ruid, "err", err)
		p.mtx.Lock()
		delete(p.openOffers, msg.Ruid)
		p.mtx.Unlock()
		p.Drop()
	}
}

// serverHandleGetRange is handled by the server and sends in response an OfferedHashes message
// in the case that for the specific interval no chunks exist - the server sends an empty OfferedHashes
// message so that the client could seal the interval and request the next
func (r *Registry) serverHandleGetRange(ctx context.Context, p *Peer, msg *GetRange, provider StreamProvider) {
	p.logger.Debug("serverHandleGetRange", "ruid", msg.Ruid, "from", msg.From, "to", msg.To, "stream", msg.Stream)
	start := time.Now()
	defer func(start time.Time) {
		metrics.GetOrRegisterResettingTimer("network.stream.handle_get_range.total-time", nil).UpdateSince(start)
	}(start)

	key, err := provider.ParseKey(msg.Stream.Key)
	if err != nil {
		p.logger.Error("erroring parsing stream key", "err", err, "stream", msg.Stream)
		p.Drop()
		return
	}
	h, f, t, e, err := r.serverCollectBatch(ctx, p, provider, key, msg.From, *msg.To)
	if err != nil {
		p.logger.Error("erroring getting batch for stream", "peer", p.ID(), "stream", msg.Stream, "err", err)
		p.Drop()
		return
	}
	l := len(h) / HashSize
	batchSizeGauge.Update(int64(l))

	// interval is empty for the requested range, return a message with an empty Hashes value
	if e {
		// prevent sending an empty batch that resulted from db shutdown or peer quits
		select {
		case <-r.quit:
			return
		case <-p.quit:
			return
		default:
			offered := OfferedHashes{
				Ruid:      msg.Ruid,
				LastIndex: msg.From, //TODO: INCORRECT
				Hashes:    []byte{},
			}
			if err := p.Send(ctx, offered); err != nil {
				p.logger.Error("erroring sending empty offered hashes", "ruid", msg.Ruid, "err", err)
				p.Drop()
			}
			return
		}
	}
	o := offer{
		ruid:      msg.Ruid,
		stream:    msg.Stream,
		hashes:    h,
		requested: time.Now(),
	}

	p.mtx.Lock()
	p.openOffers[msg.Ruid] = o
	p.mtx.Unlock()

	offered := OfferedHashes{
		Ruid:      msg.Ruid,
		LastIndex: t,
		Hashes:    h,
	}
	p.logger.Debug("server offering batch", "ruid", msg.Ruid, "requestFrom", msg.From, "From", f, "requestTo", msg.To, "hashes", l)
	if err := p.Send(ctx, offered); err != nil {
		p.logger.Error("erroring sending offered hashes", "ruid", msg.Ruid, "err", err)
		p.mtx.Lock()
		delete(p.openOffers, msg.Ruid)
		p.mtx.Unlock()
		p.Drop()
	}
}

// clientHandleOfferedHashes handles the OfferedHashes wire protocol message (Peer is the server)
func (r *Registry) clientHandleOfferedHashes(ctx context.Context, p *Peer, msg *OfferedHashes, w *want, provider StreamProvider) {
	p.logger.Debug("clientHandleOfferedHashes", "ruid", msg.Ruid, "msg.lastIndex", msg.LastIndex)
	start := time.Now()
	defer func(start time.Time) {
		metrics.GetOrRegisterResettingTimer("network.stream.handle_offered_hashes.total-time", nil).UpdateSince(start)
	}(start)

	var (
		hashes           = msg.Hashes
		lenHashes        = len(hashes)
		ctr       uint64 = 0                                         // the number of chunks wanted out of the batch
		addresses        = make([]chunk.Address, lenHashes/HashSize) // the address slice for MultiHas

		wantedHashesMsg = WantedHashes{Ruid: msg.Ruid}
		errc            <-chan error
	)

	if lenHashes%HashSize != 0 {
		p.logger.Error("invalid hashes length", "len", lenHashes, "ruid", msg.Ruid)
		p.Drop()
		return
	}

	w.to = &msg.LastIndex // now that we know the range of the batch we can set the upped bound of the interval to the open want

	// this code block handles the case of a gap on the interval on the server side
	// lenhashes == 0 means there's no hashes in the requested range with the upper bound of
	// the LastIndex on the incoming message. we should seal the interval and request the subsequent
	if lenHashes == 0 {
		if err := p.sealWant(w); err != nil {
			p.logger.Error("error persisting interval", "from", w.from, "to", w.to, "err", err)
			p.Drop()
			return
		}
		r.requestSubsequentRange(ctx, p, provider, w, msg.LastIndex)
		return
	}

	want, err := bv.New(lenHashes / HashSize)
	if err != nil {
		p.logger.Error("error initiaising bitvector", "len", lenHashes/HashSize, "ruid", msg.Ruid, "err", err)
		p.Drop()
		return
	}

	for i := 0; i < lenHashes; i += HashSize {
		hash := hashes[i : i+HashSize]
		addresses[i/HashSize] = hash
	}

	if hasses, err := provider.MultiNeedData(ctx, addresses...); err == nil {
		for i, has := range hasses {
			if !has {
				ctr++
				want.Set(i)
				w.hashes[addresses[i].Hex()] = true
			}
		}
	} else {
		p.logger.Error("multi need data returned an error, dropping peer", "err", err)
		p.Drop()
		return
	}

	atomic.AddUint64(&w.remaining, ctr)

	if ctr == 0 {
		// this handles the case that there are no hashes we are interested in
		// we then seal the current interval and request the next batch
		streamEmptyWantedHashes.Inc(1)
		wantedHashesMsg.BitVector = []byte{}
		if err := p.sealWant(w); err != nil {
			p.logger.Error("error persisting interval", "from", w.from, "to", w.to, "err", err)
			p.Drop()
			return
		}
		if err := p.Send(ctx, wantedHashesMsg); err != nil {
			p.logger.Error("error sending wanted hashes", "err", err)
			p.Drop()
			return
		}
		r.requestSubsequentRange(ctx, p, provider, w, msg.LastIndex)
		return
	} else {
		// there are some hashes in the offer and we want some
		streamWantedHashes.Inc(1)
		wantedHashesMsg.BitVector = want.Bytes()

		errc = r.clientSealBatch(ctx, p, provider, w) // poll for the completion of the batch in a separate goroutine
	}

	if err := p.Send(ctx, wantedHashesMsg); err != nil {
		p.logger.Error("error sending wanted hashes", "err", err)
		p.Drop()
		return
	}
	select {
	case err := <-errc:
		if err != nil {
			streamBatchFail.Inc(1)
			p.logger.Error("got an error while sealing batch", "from", w.from, "to", w.to, "err", err)
			p.Drop()
			return
		}
		if err := p.sealWant(w); err != nil {
			p.logger.Error("error persisting interval", "from", w.from, "to", w.to, "err", err)
			p.Drop()
			return
		}
	case <-time.After(activeBatchTimeout):
		p.logger.Error("batch has timed out", "ruid", w.ruid)
		close(w.closeC)
		p.mtx.Lock()
		delete(p.openWants, msg.Ruid)
		p.mtx.Unlock()
		p.Drop()
		return
	case <-r.quit:
		return
	case <-p.quit:
		return
	}
	r.requestSubsequentRange(ctx, p, provider, w, msg.LastIndex)
}

// serverHandleWantedHashes is handled on the server side (Peer is the client) and is dependent on a preceding OfferedHashes message
// the method is to ensure that all chunks in the requested batch is sent to the client
func (r *Registry) serverHandleWantedHashes(ctx context.Context, p *Peer, msg *WantedHashes, o offer, provider StreamProvider) {
	p.logger.Debug("serverHandleWantedHashes", "ruid", msg.Ruid)
	start := time.Now()
	defer func(start time.Time) {
		metrics.GetOrRegisterResettingTimer("network.stream.handle_wanted_hashes.total-time", nil).UpdateSince(start)
	}(start)

	defer func() {
		p.mtx.Lock()
		delete(p.openOffers, msg.Ruid)
		p.mtx.Unlock()
	}()

	l := len(o.hashes) / HashSize
	if len(msg.BitVector) == 0 {
		p.logger.Debug("peer does not want any hashes in this range", "ruid", o.ruid)
		return
	}
	want, err := bv.NewFromBytes(msg.BitVector, l)
	if err != nil {
		p.logger.Error("error initiaising bitvector", "l", l, "ll", len(o.hashes), "err", err)
		p.Drop()
		return
	}

	frameSize := 0
	var maxFrame = BatchSize / 4
	if maxFrame < 1 {
		maxFrame = 1
	}

	cd := &ChunkDelivery{
		Ruid: msg.Ruid,
	}
	wantHashes := []chunk.Address{}
	for i := 0; i < l; i++ {
		if want.Get(i) {
			metrics.GetOrRegisterCounter("network.stream.handle_wanted.want_get", nil).Inc(1)
			hash := o.hashes[i*HashSize : (i+1)*HashSize]
			wantHashes = append(wantHashes, hash)
		}
	}
	chunks, err := provider.Get(ctx, wantHashes...)
	if err != nil {
		p.logger.Error("handleWantedHashesMsg", "err", err)
		p.Drop()
		return
	}
	for _, v := range chunks {
		chunkD := DeliveredChunk{
			Addr: v.Address(),
			Data: v.Data(),
		}
		cd.Chunks = append(cd.Chunks, chunkD)

		//collect the chunk into the batch
		frameSize++

		if frameSize == maxFrame {
			//send the batch
			select {
			case <-p.quit:
				return
			case <-r.quit:
				return
			default:
			}
			if err := p.Send(ctx, cd); err != nil {
				p.logger.Error("error sending chunk delivery frame", "ruid", msg.Ruid, "error", err)
				p.Drop()
				return
			}
			frameSize = 0
			cd = &ChunkDelivery{
				Ruid: msg.Ruid,
			}
		}
	}

	// send anything that we might have left in the batch
	if frameSize > 0 {
		if err := p.Send(ctx, cd); err != nil {
			p.logger.Error("error sending chunk delivery frame", "ruid", msg.Ruid, "error", err)
			p.Drop()
		}
	}

	var addrs []chunk.Address
	for i := 0; i < l; i++ {
		if want.Get(i) {
			addrs = append(addrs, o.hashes[i*HashSize:(i+1)*HashSize])
		}
	}
	err = provider.Set(ctx, addrs...)
	if err != nil {
		p.logger.Error("error setting chunk as synced", "addrs", addrs, "err", err)
		p.Drop()
		return
	}
}

func (r *Registry) clientHandleChunkDelivery(ctx context.Context, p *Peer, msg *ChunkDelivery, w *want, provider StreamProvider) {
	p.logger.Debug("clientHandleChunkDelivery", "ruid", msg.Ruid)
	processReceivedChunksMsgCount.Inc(1)
	lastReceivedChunksMsg.Update(time.Now().UnixNano())
	start := time.Now()
	defer func(start time.Time) {
		metrics.GetOrRegisterResettingTimer("network.stream.handle_chunk_delivery.total-time", nil).UpdateSince(start)
	}(start)

	chunks := make([]chunk.Chunk, len(msg.Chunks))
	for i, dc := range msg.Chunks {
		chunks[i] = chunk.NewChunk(dc.Addr, dc.Data)
	}

	seen, err := provider.Put(ctx, chunks...)
	if err != nil {
		if err == storage.ErrChunkInvalid {
			streamChunkDeliveryFail.Inc(1)
			p.Drop()
			return
		}
		p.logger.Error("clientHandleChunkDelivery error putting chunk", "err", err)
		return
	}
	for _, v := range seen {
		if v {
			//this is possible when the same chunk is asked from multiple peers, we currently do not limit this
			streamSeenChunkDelivery.Inc(1)
		}
	}

	for _, dc := range chunks {
		select {
		case w.chunks <- dc:
			// send the chunk to the goroutine that polls batch completion
		case <-w.closeC:
			// batch timeout
			return
		case <-r.quit:
			return
		case <-p.quit:
			return
		}
	}
}

func (r *Registry) clientSealBatch(ctx context.Context, p *Peer, provider StreamProvider, w *want) <-chan error {
	p.logger.Debug("clientSealBatch", "stream", w.stream, "ruid", w.ruid, "from", w.from, "to", *w.to)
	errc := make(chan error)
	go func() {
		start := time.Now()
		defer func(start time.Time) {
			metrics.GetOrRegisterResettingTimer("network.stream.client_seal_batch.total-time", nil).UpdateSince(start)
		}(start)
		for {
			select {
			case c, ok := <-w.chunks:
				if !ok {
					return
				}
				processReceivedChunksCount.Inc(1)
				p.mtx.RLock()
				if wants, ok := w.hashes[c.Address().Hex()]; !ok || !wants {
					p.logger.Error("got an unsolicited chunk from peer!", "peer", p.ID(), "caddr", c.Address())
					streamChunkDeliveryFail.Inc(1)
					p.Drop()
					p.mtx.RLock()
					return
				}
				p.mtx.RUnlock()
				p.mtx.Lock()
				delete(w.hashes, c.Address().Hex())
				p.mtx.Unlock()
				v := atomic.AddUint64(&w.remaining, ^uint64(0))
				if v == 0 {
					p.logger.Trace("done receiving chunks for open want", "ruid", w.ruid)
					close(errc)
					return
				}
			case <-p.quit:
				return
			case <-w.closeC:
				// batch timeout was signalled
				return
			case <-r.quit:
				return
			}
		}
	}()
	return errc
}

func (r *Registry) serverCollectBatch(ctx context.Context, p *Peer, provider StreamProvider, key interface{}, from, to uint64) (hashes []byte, f, t uint64, empty bool, err error) {
	p.logger.Debug("serverCollectBatch", "from", from, "to", to)
	batchStart := time.Now()

	descriptors, stop := provider.Subscribe(ctx, key, from, to)
	defer stop()

	const batchTimeout = 1 * time.Second

	var (
		batch        []byte
		batchSize    int
		batchStartID *uint64
		batchEndID   uint64
		timer        *time.Timer
		timerC       <-chan time.Time
	)

	defer func(start time.Time) {
		if to == 0 {
			collectBatchLiveTimer.UpdateSince(start)
		} else {
			collectBatchHistoryTimer.UpdateSince(start)
		}
		if timer != nil {
			timer.Stop()
		}
	}(batchStart)

	for iterate := true; iterate; {
		select {
		case d, ok := <-descriptors:
			if !ok {
				iterate = false
				break
			}
			batch = append(batch, d.Address[:]...)
			batchSize++
			if batchStartID == nil {
				// set batch start id only if
				// this is the first iteration
				batchStartID = &d.BinID
			}
			batchEndID = d.BinID
			if batchSize >= BatchSize {
				iterate = false
				metrics.GetOrRegisterCounter("network.stream.server_collect_batch.full-batch", nil).Inc(1)
			}
			if timer == nil {
				timer = time.NewTimer(batchTimeout)
			} else {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(batchTimeout)
			}
			timerC = timer.C
		case <-timerC:
			// return batch if new chunks are not received after some time
			iterate = false
			metrics.GetOrRegisterCounter("network.stream.server_collect_batch.timer-expire", nil).Inc(1)
		case <-p.quit:
			iterate = false
		case <-r.quit:
			iterate = false
		}
	}
	if batchStartID == nil {
		// if batch start id is not set, it means we timed out
		return nil, 0, 0, true, nil
	}
	return batch, *batchStartID, batchEndID, false, nil
}

func (r *Registry) requestSubsequentRange(ctx context.Context, p *Peer, provider StreamProvider, w *want, lastIndex uint64) {
	cur, ok := p.getCursor(w.stream)
	if !ok {
		metrics.GetOrRegisterCounter("network.stream.quit_unwanted", nil).Inc(1)
		p.logger.Debug("no longer interested in stream. quitting", "stream", w.stream)
		p.mtx.Lock()
		delete(p.openWants, w.ruid)
		p.mtx.Unlock()
		return
	}
	if w.head {
		if err := r.clientRequestStreamHead(ctx, p, w.stream, lastIndex+1); err != nil {
			streamRequestNextIntervalFail.Inc(1)
			p.logger.Error("error requesting next interval from peer", "err", err)
			p.Drop()
			return
		}
	} else {
		if err := r.clientRequestStreamRange(ctx, p, provider, w.stream, cur); err != nil {
			streamRequestNextIntervalFail.Inc(1)
			p.logger.Error("error requesting next interval from peer", "err", err)
			p.Drop()
			return
		}
	}
}

// getProviderOrDrop gets a StreamProvider from the Registry or drops the peer in case it is not supported
func (r *Registry) getProviderOrDrop(stream ID) (provider StreamProvider, shouldDrop bool) {
	provider = r.getProvider(stream)
	if provider == nil {
		// at this point of the message exchange unsupported providers are illegal. drop peer
		return nil, true
	}
	return provider, false
}

func (r *Registry) getProvider(stream ID) StreamProvider {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	return r.providers[stream.Name]
}

func (r *Registry) getPeer(id enode.ID) *Peer {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	p := r.peers[id]
	return p
}

func (r *Registry) addPeer(p *Peer) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.peers[p.ID()] = p

	streamPeersCount.Update(int64(len(r.peers)))
}

func (r *Registry) removePeer(p *Peer) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if _, found := r.peers[p.ID()]; found {
		p.logger.Error("removing peer")
		delete(r.peers, p.ID())
		close(p.quit)
	}
	streamPeersCount.Update(int64(len(r.peers)))
}

// PeerCurosrs returns a JSON response in which the queried node's
// peer cursors are returned
func (r *Registry) PeerCursors() string {
	type peerCurs struct {
		Peer    string            `json:"peer"` // the peer address
		Cursors map[string]uint64 `json:"cursors"`
	}
	curs := struct {
		Base  string     `json:"base"` // our node's base address
		Peers []peerCurs `json:"peers"`
	}{
		Base: hex.EncodeToString(r.baseKey)[:16],
	}

	for _, p := range r.peers {
		pcur := peerCurs{
			Peer:    hex.EncodeToString(p.OAddr)[:16],
			Cursors: p.getCursorsCopy(),
		}
		curs.Peers = append(curs.Peers, pcur)
	}
	pc, err := json.Marshal(&curs)
	if err != nil {
		return ""
	}
	return string(pc)
}

func (r *Registry) Protocols() []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    "bzz-stream",
			Version: 1,
			Length:  10 * 1024 * 1024,
			Run:     r.runProtocol,
		},
	}
}

func (r *Registry) runProtocol(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	peer := protocols.NewPeer(p, rw, r.spec)
	// TODO: fix, used in tests only. Incorrect, as we do not have access to the overlay address
	bp := network.NewBzzPeer(peer)

	return r.Run(bp)
}

func (r *Registry) APIs() []rpc.API {
	return nil
}

func (r *Registry) Start(server *p2p.Server) error {
	r.logger.Debug("stream registry starting")

	return nil
}

func (r *Registry) Stop() error {
	log.Debug("stream registry stopping")
	r.mtx.Lock()
	defer r.mtx.Unlock()

	close(r.quit)
	// wait for all handlers to finish
	done := make(chan struct{})
	go func() {
		r.handlersWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Error("stream closed with still active handlers")
	}

	for _, v := range r.providers {
		v.Close()
	}

	return nil
}
