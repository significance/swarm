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

package pushsync

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/spancontext"
	"github.com/ethersphere/swarm/storage"
	olog "github.com/opentracing/opentracing-go/log"
)

// Store is the storage interface to save chunks
// NetStore implements this interface
type Store interface {
	Put(context.Context, chunk.ModePut, ...chunk.Chunk) ([]bool, error)
}

// Storer is the object used by the push-sync server side protocol
type Storer struct {
	store      Store      // store to put chunks in, and retrieve them from
	ps         PubSub     // pubsub interface to receive chunks and send receipts
	deregister func()     // deregister the registered handler when Storer is closed
	logger     log.Logger // custom logger
}

// NewStorer constructs a Storer
// Storer runs on storer nodes to handle the reception of push-synced chunks
// that fall within their area of responsibility.
// The protocol makes sure that
// - the chunks are stored and synced to their nearest neighbours and
// - a statement of custody receipt is sent as a response to the originator
// it sets a cancel function that deregisters the handler
func NewStorer(store Store, ps PubSub) *Storer {
	s := &Storer{
		store:  store,
		ps:     ps,
		logger: log.New("self", label(ps.BaseAddr())),
	}
	s.deregister = ps.Register(pssChunkTopic, true, func(msg []byte, _ *p2p.Peer) error {
		return s.handleChunkMsg(msg)
	})
	return s
}

// Close needs to be called to deregister the handler
func (s *Storer) Close() {
	s.deregister()
}

// handleChunkMsg is called by the pss dispatcher on pssChunkTopic msgs
// - deserialises chunkMsg and
// - calls storer.processChunkMsg function
func (s *Storer) handleChunkMsg(msg []byte) error {
	chmsg, err := decodeChunkMsg(msg)
	if err != nil {
		return err
	}

	ctx, osp := spancontext.StartSpan(context.Background(), "handle.chunk.msg")
	defer osp.Finish()
	osp.LogFields(olog.String("ref", hex.EncodeToString(chmsg.Addr)))
	osp.SetTag("addr", hex.EncodeToString(chmsg.Addr))
	return s.processChunkMsg(ctx, chmsg)
}

// processChunkMsg processes a chunk received via pss pssChunkTopic
// these chunk messages are sent to their address as destination
// using neighbourhood addressing. Therefore nodes only handle
// chunks that fall within their area of responsibility.
// Upon receiving the chunk is saved and a statement of custody
// receipt message is sent as a response to the originator.
func (s *Storer) processChunkMsg(ctx context.Context, chmsg *chunkMsg) error {
	s.logger.Trace("Storer.processChunkMsg", "chunk", fmt.Sprintf("%x", chmsg.Addr), "origin", label(chmsg.Origin))
	// TODO: double check if it falls in area of responsibility
	ch := storage.NewChunk(chmsg.Addr, chmsg.Data)
	if _, err := s.store.Put(ctx, chunk.ModePutSync, ch); err != nil {
		return err
	}

	// if self is closest peer then send back a receipt
	if s.ps.IsClosestTo(chmsg.Addr) {
		s.logger.Trace("storer.isClosestTo", "ref", fmt.Sprintf("%x", chmsg.Addr))
		return s.sendReceiptMsg(ctx, chmsg)
	}
	return nil
}

// sendReceiptMsg sends a statement of custody receipt message
// to the originator of a push-synced chunk message.
// Including a unique nonce makes the receipt immune to deduplication cache
func (s *Storer) sendReceiptMsg(ctx context.Context, chmsg *chunkMsg) error {
	ctx, osp := spancontext.StartSpan(ctx, "send.receipt")
	defer osp.Finish()
	osp.LogFields(olog.String("ref", hex.EncodeToString(chmsg.Addr)))
	osp.SetTag("addr", hex.EncodeToString(chmsg.Addr))
	osp.LogFields(olog.String("origin", hex.EncodeToString(chmsg.Origin)))

	rmsg := &receiptMsg{
		Addr:  chmsg.Addr,
		Nonce: newNonce(),
	}
	msg, err := rlp.EncodeToBytes(rmsg)
	if err != nil {
		return err
	}
	to := chmsg.Origin
	s.logger.Trace("send receipt", "addr", fmt.Sprintf("%x", rmsg.Addr), "to", label(to))
	return s.ps.Send(to, pssReceiptTopic, msg)
}
