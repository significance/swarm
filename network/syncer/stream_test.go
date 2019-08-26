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

package syncer

import (
	"context"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethersphere/swarm/network"
	"github.com/ethersphere/swarm/network/simulation"
	"github.com/ethersphere/swarm/storage"
	"github.com/ethersphere/swarm/storage/localstore"
	"github.com/ethersphere/swarm/storage/mock"
)

var (
	loglevel = flag.Int("loglevel", 5, "verbosity of logs")
)

func init() {
	flag.Parse()

	log.PrintOrigins(true)
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(*loglevel), log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
}
func newTestLocalStore(id enode.ID, addr *network.BzzAddr, globalStore mock.GlobalStorer) (localStore *localstore.DB, cleanup func(), err error) {
	dir, err := ioutil.TempDir("", "swarm-stream-")
	if err != nil {
		return nil, nil, err
	}
	cleanup = func() {
		os.RemoveAll(dir)
	}

	var mockStore *mock.NodeStore
	if globalStore != nil {
		mockStore = globalStore.NewNodeStore(common.BytesToAddress(id.Bytes()))
	}

	localStore, err = localstore.New(dir, addr.Over(), &localstore.Options{
		MockStore: mockStore,
	})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return localStore, cleanup, nil
}

func TestNodesCanTalk(t *testing.T) {
	nodeCount := 2

	// create a standard sim
	sim := simulation.New(map[string]simulation.ServiceFunc{
		"bzz-sync": func(ctx *adapters.ServiceContext, bucket *sync.Map) (s node.Service, cleanup func(), err error) {
			n := ctx.Config.Node()
			addr := network.NewAddr(n)

			localStore, localStoreCleanup, err := newTestLocalStore(n.ID(), addr, nil)
			if err != nil {
				return nil, nil, err
			}

			netStore := storage.NewNetStore(localStore, enode.ID{})

			kad := network.NewKademlia(addr.Over(), network.NewKadParams())
			o := NewSwarmSyncer(enode.ID{}, nil, kad, netStore)
			cleanup = func() {
				localStore.Close()
				localStoreCleanup()
			}

			return o, cleanup, nil
		},
	})
	defer sim.Close()

	// create context for simulation run
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	// defer cancel should come before defer simulation teardown
	defer cancel()
	_, err := sim.AddNodesAndConnectStar(nodeCount)
	if err != nil {
		t.Fatal(err)
	}

	// setup the filter for SubscribeMsg
	msgs := sim.PeerEvents(
		context.Background(),
		sim.UpNodeIDs(),
		simulation.NewPeerEventsFilter().ReceivedMessages().Protocol("bzz-sync"),
	)

	// strategy: listen to all SubscribeMsg events; after every event we wait
	// if after `waitDuration` no more messages are being received, we assume the
	// subscription phase has terminated!

	// the loop in this go routine will either wait for new message events
	// or times out after 1 second, which signals that we are not receiving
	// any new subscriptions any more
	go func() {
		//for long running sims, waiting 1 sec will not be enough
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-msgs: // just reset the loop
				if m.Error != nil {
					log.Error("syncer message errored", "err", m.Error)
					continue
				}
				log.Trace("syncer message", "node", m.NodeID, "peer", m.PeerID)

			}
		}
	}()

	//run the simulation
	result := sim.Run(ctx, func(ctx context.Context, sim *simulation.Simulation) error {
		log.Info("Simulation running")
		_ = sim.Net.Nodes

		//wait until all subscriptions are done
		select {
		case <-ctx.Done():
			return errors.New("Context timed out")
		}

		return nil
	})
	if result.Error != nil {
		t.Fatal(result.Error)
	}
}
