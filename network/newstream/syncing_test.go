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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/log"
	"github.com/ethersphere/swarm/network"
	"github.com/ethersphere/swarm/network/simulation"
	"github.com/ethersphere/swarm/p2p/protocols"
	"github.com/ethersphere/swarm/pot"
	"github.com/ethersphere/swarm/storage"
	"github.com/ethersphere/swarm/storage/localstore"
	"github.com/ethersphere/swarm/testutil"
)

var timeout = 90 * time.Second

// TestTwoNodesSyncWithGaps tests that syncing works with gaps in the localstore intervals
func TestTwoNodesSyncWithGaps(t *testing.T) {
	removeChunks := func(t *testing.T, ctx context.Context, store chunk.Store, gaps [][2]uint64, chunks []chunk.Address) (removedCount uint64) {
		t.Helper()

		for _, gap := range gaps {
			for i := gap[0]; i < gap[1]; i++ {
				c := chunks[i]
				if err := store.Set(ctx, chunk.ModeSetRemove, c); err != nil {
					t.Fatal(err)
				}
				removedCount++
			}
		}
		return removedCount
	}

	for _, tc := range []struct {
		name           string
		chunkCount     uint64
		gaps           [][2]uint64
		liveChunkCount uint64
		liveGaps       [][2]uint64
	}{
		{
			name:       "no gaps",
			chunkCount: 100,
			gaps:       nil,
		},
		{
			name:       "first chunk removed",
			chunkCount: 100,
			gaps:       [][2]uint64{{0, 1}},
		},
		{
			name:       "one chunk removed",
			chunkCount: 100,
			gaps:       [][2]uint64{{60, 61}},
		},
		{
			name:       "single gap at start",
			chunkCount: 100,
			gaps:       [][2]uint64{{0, 5}},
		},
		{
			name:       "single gap",
			chunkCount: 100,
			gaps:       [][2]uint64{{5, 10}},
		},
		{
			name:       "multiple gaps",
			chunkCount: 100,
			gaps:       [][2]uint64{{0, 1}, {10, 21}},
		},
		{
			name:       "big gaps",
			chunkCount: 100,
			gaps:       [][2]uint64{{0, 1}, {10, 21}, {50, 91}},
		},
		{
			name:       "remove all",
			chunkCount: 100,
			gaps:       [][2]uint64{{0, 100}},
		},
		{
			name:       "large db",
			chunkCount: 4000,
		},
		{
			name:       "large db with gap",
			chunkCount: 4000,
			gaps:       [][2]uint64{{1000, 3000}},
		},
		{
			name:           "live",
			liveChunkCount: 100,
		},
		{
			name:           "live and history",
			chunkCount:     100,
			liveChunkCount: 100,
		},
		{
			name:           "live and history with history gap",
			chunkCount:     100,
			gaps:           [][2]uint64{{5, 10}},
			liveChunkCount: 100,
		},
		//{
		//name:           "live and history with live gap",
		//chunkCount:     100,
		//liveChunkCount: 100,
		//liveGaps:       [][2]uint64{{105, 110}},
		//},
		{
			name:           "live and history with gaps",
			chunkCount:     100,
			gaps:           [][2]uint64{{5, 10}},
			liveChunkCount: 100,
			liveGaps:       [][2]uint64{{105, 110}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim := simulation.NewBzzInProc(map[string]simulation.ServiceFunc{
				"bzz-sync": newSyncSimServiceFunc(nil),
			})
			defer sim.Close()
			defer catchDuplicateChunkSync(t)()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			uploadNode, err := sim.AddNode()
			if err != nil {
				t.Fatal(err)
			}

			uploadStore := sim.MustNodeItem(uploadNode, bucketKeyFileStore).(chunk.Store)

			chunks := mustUploadChunks(ctx, t, uploadStore, tc.chunkCount)

			totalChunkCount, err := getChunkCount(uploadStore)
			if err != nil {
				t.Fatal(err)
			}

			if totalChunkCount != tc.chunkCount {
				t.Errorf("uploaded %v chunks, want %v", totalChunkCount, tc.chunkCount)
			}

			removedCount := removeChunks(t, ctx, uploadStore, tc.gaps, chunks)

			syncNode, err := sim.AddNode()
			if err != nil {
				t.Fatal(err)
			}
			err = sim.Net.Connect(uploadNode, syncNode)
			if err != nil {
				t.Fatal(err)
			}

			syncStore := sim.MustNodeItem(syncNode, bucketKeyFileStore).(chunk.Store)

			err = waitChunks(syncStore, totalChunkCount-removedCount, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}

			if tc.liveChunkCount > 0 {
				chunks = append(chunks, mustUploadChunks(ctx, t, uploadStore, tc.liveChunkCount)...)

				totalChunkCount, err = getChunkCount(uploadStore)
				if err != nil {
					t.Fatal(err)
				}

				if want := tc.chunkCount + tc.liveChunkCount; totalChunkCount != want {
					t.Errorf("uploaded %v chunks, want %v", totalChunkCount, want)
				}

				removedCount += removeChunks(t, ctx, uploadStore, tc.liveGaps, chunks)

				err = waitChunks(syncStore, totalChunkCount-removedCount, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestTheeNodesUnionHistoricalSync brings up three nodes, uploads content too all of them and then
// asserts that all of them have the union of all 3 local stores (depth is assumed to be 0)
func TestThreeNodesUnionHistoricalSync(t *testing.T) {
	nodes := 3
	chunkCount := 1000
	sim := simulation.NewBzzInProc(map[string]simulation.ServiceFunc{
		"bzz-sync": newSyncSimServiceFunc(nil),
	})
	defer sim.Close()
	union := make(map[string]struct{})
	nodeIDs := []enode.ID{}
	for i := 0; i < nodes; i++ {
		node, err := sim.AddNode()
		if err != nil {
			t.Fatal(err)
		}
		nodeIDs = append(nodeIDs, node)
		nodeStore := sim.MustNodeItem(node, bucketKeyFileStore).(*storage.FileStore)
		mustUploadChunks(context.Background(), t, nodeStore, uint64(chunkCount))

		uploadedChunks, err := getChunks(nodeStore.ChunkStore)
		if err != nil {
			t.Fatal(err)
		}
		for k := range uploadedChunks {
			if _, ok := union[k]; ok {
				t.Fatal("chunk already exists in union")
			}
			union[k] = struct{}{}
		}
	}

	err := sim.Net.ConnectNodesFull(nodeIDs)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodeIDs {
		nodeStore := sim.MustNodeItem(n, bucketKeyFileStore).(*storage.FileStore)
		if err := waitChunks(nodeStore, uint64(len(union)), 10*time.Second); err != nil {
			t.Fatal(err)
		}
	}
}

// TestFullSync performs a series of subtests where a number of nodes are
// connected to the single (chunk uploading) node.
func TestFullSync(t *testing.T) {
	for _, tc := range []struct {
		name          string
		chunkCount    uint64
		syncNodeCount int
		history       bool
		live          bool
	}{
		{
			name:          "sync to two nodes history",
			chunkCount:    5000,
			syncNodeCount: 2,
			history:       true,
		},
		{
			name:          "sync to two nodes live",
			chunkCount:    5000,
			syncNodeCount: 2,
			live:          true,
		},
		{
			name:          "sync to two nodes history and live",
			chunkCount:    2500,
			syncNodeCount: 2,
			history:       true,
			live:          true,
		},
		{
			name:          "sync to 50 nodes history",
			chunkCount:    500,
			syncNodeCount: 50,
			history:       true,
		},
		{
			name:          "sync to 50 nodes live",
			chunkCount:    500,
			syncNodeCount: 50,
			live:          true,
		},
		{
			name:          "sync to 50 nodes history and live",
			chunkCount:    250,
			syncNodeCount: 50,
			history:       true,
			live:          true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim := simulation.NewInProc(map[string]simulation.ServiceFunc{
				"bzz-sync": newSyncSimServiceFunc(nil),
			})
			defer sim.Close()

			defer catchDuplicateChunkSync(t)()

			uploaderNode, err := sim.AddNode()
			if err != nil {
				t.Fatal(err)
			}
			uploaderNodeStore := sim.MustNodeItem(uploaderNode, bucketKeyFileStore).(*storage.FileStore)

			if tc.history {
				mustUploadChunks(context.Background(), t, uploaderNodeStore, tc.chunkCount)
			}

			// add nodes to sync to
			ids, err := sim.AddNodes(tc.syncNodeCount)
			if err != nil {
				t.Fatal(err)
			}
			// connect every new node to the uploading one, so
			// every node will have depth 0 as only uploading node
			// will be in their kademlia tables
			err = sim.Net.ConnectNodesStar(ids, uploaderNode)
			if err != nil {
				t.Fatal(err)
			}

			// count the content in the bins again
			uploadedChunks, err := getChunks(uploaderNodeStore.ChunkStore)
			if err != nil {
				t.Fatal(err)
			}
			if tc.history && len(uploadedChunks) == 0 {
				t.Errorf("got empty uploader chunk store")
			}
			if !tc.history && len(uploadedChunks) != 0 {
				t.Errorf("got non empty uploader chunk store")
			}

			historicalChunks := make(map[enode.ID]map[string]struct{})
			for _, id := range ids {
				wantChunks := make(map[string]struct{}, len(uploadedChunks))
				for k, v := range uploadedChunks {
					wantChunks[k] = v
				}
				// wait for all chunks to be synced
				store := sim.MustNodeItem(id, bucketKeyFileStore).(chunk.Store)
				if err := waitChunks(store, uint64(len(wantChunks)), 10*time.Second); err != nil {
					t.Fatal(err)
				}

				// validate that all and only all chunks are synced
				syncedChunks, err := getChunks(store)
				if err != nil {
					t.Fatal(err)
				}
				historicalChunks[id] = make(map[string]struct{})
				for c := range wantChunks {
					if _, ok := syncedChunks[c]; !ok {
						t.Errorf("missing chunk %v", c)
					}
					delete(wantChunks, c)
					delete(syncedChunks, c)
					historicalChunks[id][c] = struct{}{}
				}
				if len(wantChunks) != 0 {
					t.Errorf("some of the uploaded chunks are not synced")
				}
				if len(syncedChunks) != 0 {
					t.Errorf("some of the synced chunks are not of uploaded ones")
				}
			}

			if tc.live {
				mustUploadChunks(context.Background(), t, uploaderNodeStore, tc.chunkCount)
			}

			uploadedChunks, err = getChunks(uploaderNodeStore.ChunkStore)
			if err != nil {
				t.Fatal(err)
			}

			for _, id := range ids {
				wantChunks := make(map[string]struct{}, len(uploadedChunks))
				for k, v := range uploadedChunks {
					wantChunks[k] = v
				}
				store := sim.MustNodeItem(id, bucketKeyFileStore).(chunk.Store)
				// wait for all chunks to be synced
				if err := waitChunks(store, uint64(len(wantChunks)), 10*time.Second); err != nil {
					t.Fatal(err)
				}

				// get all chunks from the syncing node
				syncedChunks, err := getChunks(store)
				if err != nil {
					t.Fatal(err)
				}
				// remove historical chunks from total uploaded and synced chunks
				for c := range historicalChunks[id] {
					if _, ok := wantChunks[c]; !ok {
						t.Errorf("missing uploaded historical chunk: %s", c)
					}
					delete(wantChunks, c)
					if _, ok := syncedChunks[c]; !ok {
						t.Errorf("missing synced historical chunk: %s", c)
					}
					delete(syncedChunks, c)
				}
				// validate that all and only all live chunks are synced
				for c := range wantChunks {
					if _, ok := syncedChunks[c]; !ok {
						t.Errorf("missing chunk %v", c)
					}
					delete(wantChunks, c)
					delete(syncedChunks, c)
				}
				if len(wantChunks) != 0 {
					t.Errorf("some of the uploaded live chunks are not synced")
				}
				if len(syncedChunks) != 0 {
					t.Errorf("some of the synced live chunks are not of uploaded ones")
				}
			}
		})
	}
}

func waitChunks(store chunk.Store, want uint64, staledTimeout time.Duration) (err error) {
	start := time.Now()
	var (
		count  uint64        // total number of chunks
		prev   uint64        // total number of chunks in previous check
		sleep  time.Duration // duration until the next check
		staled time.Duration // duration for when the number of chunks is the same
	)
	for staled < staledTimeout { // wait for some time while staled
		count, err = getChunkCount(store)
		if err != nil {
			return err
		}
		if count >= want {
			break
		}
		if count == prev {
			staled += sleep
		} else {
			staled = 0
		}
		prev = count
		if count > 0 {
			// Calculate sleep time only if there is at least 1% of chunks available,
			// less may produce unreliable result.
			if count > want/100 {
				// Calculate the time required to pass for missing chunks to be available,
				// and divide it by half to perform a check earlier.
				sleep = time.Duration(float64(time.Since(start)) * float64(want-count) / float64(count) / 2)
				log.Debug("expecting all chunks", "in", sleep*2, "want", want, "have", count)
			}
		}
		switch {
		case sleep > time.Minute:
			// next check and speed calculation in some shorter time
			sleep = 500 * time.Millisecond
		case sleep > 5*time.Second:
			// upper limit for the check, do not check too slow
			sleep = 5 * time.Second
		case sleep < 50*time.Millisecond:
			// lower limit for the check, do not check too frequently
			sleep = 50 * time.Millisecond
			if staled > 0 {
				// slow down if chunks are stuck near the want value
				sleep *= 10
			}
		}
		time.Sleep(sleep)
	}

	if count != want {
		return fmt.Errorf("got synced chunks %d, want %d", count, want)
	}
	return nil
}

func getChunkCount(store chunk.Store) (c uint64, err error) {
	for po := 0; po <= chunk.MaxPO; po++ {
		last, err := store.LastPullSubscriptionBinID(uint8(po))
		if err != nil {
			return 0, err
		}
		c += last
	}
	return c, nil
}

func getChunks(store chunk.Store) (chunks map[string]struct{}, err error) {
	chunks = make(map[string]struct{})
	for po := uint8(0); po <= chunk.MaxPO; po++ {
		last, err := store.LastPullSubscriptionBinID(uint8(po))
		if err != nil {
			return nil, err
		}
		if last == 0 {
			continue
		}
		ch, _ := store.SubscribePull(context.Background(), po, 0, last)
		for c := range ch {
			addr := c.Address.Hex()
			if _, ok := chunks[addr]; ok {
				return nil, fmt.Errorf("duplicate chunk %s", addr)
			}
			chunks[addr] = struct{}{}
		}
	}
	return chunks, nil
}

/*
 go test -v -bench . -run BenchmarkHistoricalStream -loglevel 0 -benchtime 10x
BenchmarkHistoricalStream_1000-4    	      10	 119487009 ns/op
BenchmarkHistoricalStream_2000-4    	      10	 236469752 ns/op
BenchmarkHistoricalStream_3000-4    	      10	 371934729 ns/op
BenchmarkHistoricalStream_5000-4    	      10	 638317966 ns/op
BenchmarkHistoricalStream_10000-4   	      10	1359858063 ns/op
BenchmarkHistoricalStream_15000-4   	      10	2485790336 ns/op
BenchmarkHistoricalStream_20000-4   	      10	3382260295 ns/op
*/
func BenchmarkHistoricalStream_1000(b *testing.B)  { benchmarkHistoricalStream(b, 1000) }
func BenchmarkHistoricalStream_2000(b *testing.B)  { benchmarkHistoricalStream(b, 2000) }
func BenchmarkHistoricalStream_3000(b *testing.B)  { benchmarkHistoricalStream(b, 3000) }
func BenchmarkHistoricalStream_5000(b *testing.B)  { benchmarkHistoricalStream(b, 5000) }
func BenchmarkHistoricalStream_10000(b *testing.B) { benchmarkHistoricalStream(b, 10000) }
func BenchmarkHistoricalStream_15000(b *testing.B) { benchmarkHistoricalStream(b, 15000) }
func BenchmarkHistoricalStream_20000(b *testing.B) { benchmarkHistoricalStream(b, 20000) }

func benchmarkHistoricalStream(b *testing.B, chunks uint64) {
	b.StopTimer()
	sim := simulation.NewBzzInProc(map[string]simulation.ServiceFunc{
		"bzz-sync": newSyncSimServiceFunc(nil),
	})

	defer sim.Close()
	uploaderNode, err := sim.AddNode()
	if err != nil {
		b.Fatal(err)
	}

	if err != nil {
		b.Fatal(err)
	}

	uploaderNodeStore := sim.MustNodeItem(uploaderNode, bucketKeyFileStore).(*storage.FileStore)
	uploadedChunks, err := getChunks(uploaderNodeStore.ChunkStore)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		syncingNode, err := sim.AddNode()
		if err != nil {
			b.Fatal(err)
		}

		mustUploadChunks(context.Background(), b, nodeFileStore(sim, syncingNode), chunks)

		err = sim.Net.Connect(syncingNode, uploaderNode)
		if err != nil {
			b.Fatal(err)
		}
		syncingNodeStore := sim.MustNodeItem(syncingNode, bucketKeyFileStore).(chunk.Store)
		if err := waitChunks(syncingNodeStore, uint64(len(uploadedChunks)), 10*time.Second); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		err = sim.Net.Stop(syncingNode)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Function that uses putSeenTestHook to record and report
// if there were duplicate chunk synced between Node id
func catchDuplicateChunkSync(t *testing.T) (validate func()) {
	m := make(map[enode.ID]map[string]int)
	var mu sync.Mutex
	putSeenTestHook = func(addr chunk.Address, id enode.ID) {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := m[id]; !ok {
			m[id] = make(map[string]int)
		}
		m[id][addr.Hex()]++
	}
	return func() {
		// reset the test hook
		putSeenTestHook = nil
		// do the validation
		mu.Lock()
		defer mu.Unlock()
		for nodeID, addrs := range m {
			for addr, count := range addrs {
				t.Errorf("chunk synced %v times to node %s: %v", count, nodeID, addr)
			}
		}
	}
}

// TestStarNetworkSyncWithBogusNodes ests that syncing works on a more elaborate network topology
// the test creates three real nodes in a star topology, then adds bogus nodes to the pivot (instead of using real nodes
// this is in order to make the simulation be more CI friendly)
// the pivot node will have neighbourhood depth > 0, which in turn means that from each
// connected node, the pivot node should have only part of its chunks
// The test checks that EVERY chunk that exists a node which is not the pivot, according to
// its PO, and kademlia table of the pivot - exists on the pivot node and does not exist on other nodes
func TestStarNetworkSyncWithBogusNodes(t *testing.T) {
	var (
		chunkCount    = 500
		nodeCount     = 12
		minPivotDepth = 1
		chunkSize     = 4096
		simTimeout    = 60 * time.Second
		syncTime      = 2 * time.Second
		filesize      = chunkCount * chunkSize
	)
	sim := simulation.NewBzzInProc(map[string]simulation.ServiceFunc{
		"bzz-sync": newSyncSimServiceFunc(&SyncSimServiceOptions{SyncOnlyWithinDepth: false}),
	})
	defer sim.Close()

	ctx, cancel := context.WithTimeout(context.Background(), simTimeout)
	defer cancel()

	pivot, err := sim.AddNode()
	if err != nil {
		t.Fatal(err)
	}
	pivotKad := sim.MustNodeItem(pivot, simulation.BucketKeyKademlia).(*network.Kademlia)
	pivotBase := pivotKad.BaseAddr()

	log.Debug("started pivot node", "addr", hex.EncodeToString(pivotBase))

	newNode, err := sim.AddNode()
	if err != nil {
		t.Fatal(err)
	}
	err = sim.Net.Connect(pivot, newNode)
	if err != nil {
		t.Fatal(err)
	}

	newNode2, err := sim.AddNode()
	if err != nil {
		t.Fatal(err)
	}
	err = sim.Net.Connect(pivot, newNode2)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	log.Trace(sim.MustNodeItem(newNode, simulation.BucketKeyKademlia).(*network.Kademlia).String())
	pivotKad = sim.MustNodeItem(pivot, simulation.BucketKeyKademlia).(*network.Kademlia)
	pivotAddr := pot.NewAddressFromBytes(pivotBase)
	// add a few fictional nodes at higher POs to uploader so that uploader depth goes > 0
	for i := 0; i < nodeCount; i++ {
		rw := &p2p.MsgPipeRW{}
		ptpPeer := p2p.NewPeer(enode.ID{}, "im just a lazy hobo", []p2p.Cap{})
		protoPeer := protocols.NewPeer(ptpPeer, rw, &protocols.Spec{})
		peerAddr := pot.RandomAddressAt(pivotAddr, i)
		bzzPeer := &network.BzzPeer{
			Peer: protoPeer,
			BzzAddr: &network.BzzAddr{
				OAddr: peerAddr.Bytes(),
				UAddr: []byte(fmt.Sprintf("%x", peerAddr[:])),
			},
		}
		peer := network.NewPeer(bzzPeer, pivotKad)
		pivotKad.On(peer)
	}
	time.Sleep(50 * time.Millisecond)

	log.Trace(pivotKad.String())

	if d := pivotKad.NeighbourhoodDepth(); d < minPivotDepth {
		t.Skipf("too shallow. depth %d want %d", d, minPivotDepth)
	}
	pivotDepth := pivotKad.NeighbourhoodDepth()

	chunkProx := make(map[string]chunkProxData)
	result := sim.Run(ctx, func(ctx context.Context, sim *simulation.Simulation) (err error) {
		nodeIDs := sim.UpNodeIDs()
		for _, node := range nodeIDs {
			node := node
			if bytes.Equal(pivot.Bytes(), node.Bytes()) {
				continue
			}
			nodeKad := sim.MustNodeItem(node, simulation.BucketKeyKademlia).(*network.Kademlia)
			nodePo := chunk.Proximity(nodeKad.BaseAddr(), pivotKad.BaseAddr())
			seed := int(time.Now().UnixNano())
			randomBytes := testutil.RandomBytes(seed, filesize)
			log.Debug("putting chunks to ephemeral localstore")
			chunkAddrs, err := getAllRefs(randomBytes[:])
			if err != nil {
				return err
			}

			for _, c := range chunkAddrs {
				proxData := chunkProxData{
					addr:                      c,
					uploaderNodeToPivotNodePO: nodePo,
					chunkToUploaderPO:         chunk.Proximity(nodeKad.BaseAddr(), c),
					pivotPO:                   chunk.Proximity(c, pivotKad.BaseAddr()),
					uploaderNode:              node,
				}
				log.Debug("test putting chunk", "node", node, "addr", hex.EncodeToString(c), "uploaderToPivotPO", proxData.uploaderNodeToPivotNodePO, "c2uploaderPO", proxData.chunkToUploaderPO, "pivotDepth", pivotDepth)
				if _, ok := chunkProx[hex.EncodeToString(c)]; ok {
					return fmt.Errorf("chunk already found on another node %s", hex.EncodeToString(c))
				}
				chunkProx[hex.EncodeToString(c)] = proxData
			}

			fs := sim.MustNodeItem(node, bucketKeyFileStore).(*storage.FileStore)
			reader := bytes.NewReader(randomBytes[:])
			_, wait1, err := fs.Store(ctx, reader, int64(len(randomBytes)), false)
			if err != nil {
				return fmt.Errorf("fileStore.Store: %v", err)
			}

			if err := wait1(ctx); err != nil {
				return err
			}
		}
		//according to old pull sync - if the node is outside of depth - it should have all chunks where po(chunk)==po(node)
		time.Sleep(syncTime)

		pivotLs := sim.MustNodeItem(pivot, bucketKeyLocalStore).(*localstore.DB)
		return verifyCorrectChunksOnPivot(chunkProx, pivotDepth, pivotLs)
	})

	if result.Error != nil {
		t.Fatal(result.Error)
	}
}

func verifyCorrectChunksOnPivot(chunkProx map[string]chunkProxData, pivotDepth int, pivotLs *localstore.DB) error {
	for _, v := range chunkProx {
		// outside of depth
		if v.uploaderNodeToPivotNodePO < pivotDepth {
			// chunk PO to uploader == uploader node PO to pivot (i.e. chunk should be synced) - inclusive test
			if v.chunkToUploaderPO == v.uploaderNodeToPivotNodePO {
				//check that the chunk exists on the pivot when the chunkPo == uploaderPo
				_, err := pivotLs.Get(context.Background(), chunk.ModeGetRequest, v.addr)
				if err != nil {
					log.Error("chunk errored", "uploaderNode", v.uploaderNode, "poUploader", v.chunkToUploaderPO, "uploaderToPivotPo", v.uploaderNodeToPivotNodePO, "chunk", hex.EncodeToString(v.addr))
					return err
				}
			} else {
				//chunk should not be synced - exclusion test
				_, err := pivotLs.Get(context.Background(), chunk.ModeGetRequest, v.addr)
				if err == nil {
					log.Error("chunk did not error but should have", "uploaderNode", v.uploaderNode, "poUploader", v.chunkToUploaderPO, "uploaderToPivotPo", v.uploaderNodeToPivotNodePO, "chunk", hex.EncodeToString(v.addr))
					return err
				}
			}
		}
	}
	return nil
}

type chunkProxData struct {
	addr                      chunk.Address
	uploaderNodeToPivotNodePO int
	chunkToUploaderPO         int
	uploaderNode              enode.ID
	pivotPO                   int
}

func getAllRefs(testData []byte) (storage.AddressCollection, error) {
	datadir, err := ioutil.TempDir("", "chunk-debug")
	if err != nil {
		return nil, fmt.Errorf("unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(datadir)
	fileStore, cleanup, err := storage.NewLocalFileStore(datadir, make([]byte, 32), chunk.NewTags())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	reader := bytes.NewReader(testData)
	return fileStore.GetAllReferences(context.Background(), reader, false)
}
