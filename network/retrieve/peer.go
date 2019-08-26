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

package retrieve

import (
	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/network"
)

// Peer is the Peer extension for the streaming protocol
type Peer struct {
	*network.BzzPeer
}

// NewPeer is the constructor for Peer
func NewPeer(peer *network.BzzPeer) *Peer {
	p := &Peer{
		BzzPeer: peer,
	}
	return p
}
func (p *Peer) logError(msg string, ctx ...interface{}) {
	ctxs := []interface{}{
		"peer",
		p.ID(),
	}
	ctxs = append(ctxs, ctx...)
	log.Error(msg, ctxs...)
}

func (p *Peer) logDebug(msg string, ctx ...interface{}) {
	ctxs := []interface{}{
		"peer",
		p.ID(),
	}
	ctxs = append(ctxs, ctx...)
	log.Debug(msg, ctxs...)
}

func (p *Peer) logTrace(msg string, ctx ...interface{}) {
	ctxs := []interface{}{
		"peer",
		p.ID(),
	}
	ctxs = append(ctxs, ctx...)
	log.Trace(msg, ctxs...)
}
