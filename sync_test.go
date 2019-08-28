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

package swarm

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethersphere/swarm/api"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/network/simulation"
	"github.com/ethersphere/swarm/storage"
)

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))

	bucketKeyLocalStore = "localstore"
	bucketKeyAPI        = "api"
	bucketKeyInspector  = "inspector"
)

// TestSync a long running test.
// Make sure to run it with go test -timeout 60m or some longer timeout period.
// Example: go test -timeout 60m github.com/ethersphere/swarm -run TestSync -v -count=1 -longrunning
func TestSync(t *testing.T) {
	if !*longrunning {
		t.Skip("longrunning test")
	}
	const (
		fileSize            = 50 * 1024 * 1024
		nodeCount           = 4
		iterations          = 10
		randomUploadingNode = true
	)

	sim := simulation.NewInProc(map[string]simulation.ServiceFunc{
		"bootnode": newServiceFunc(true),
		"swarm":    newServiceFunc(false),
	})
	defer sim.Close()

	bootnode, err := sim.AddNode(simulation.AddNodeWithService("bootnode"))
	if err != nil {
		t.Fatal(err)
	}

	nodes, err := sim.AddNodes(nodeCount, simulation.AddNodeWithService("swarm"))
	if err != nil {
		t.Fatal(err)
	}

	if err := sim.Net.ConnectNodesStar(nodes, bootnode); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= iterations; i++ {
		nodeIndex := 0
		if randomUploadingNode {
			nodeIndex = random.Intn(len(nodes))
		}
		log.Warn("FILTER test start", "iteration", i, "uploadingNode", nodeIndex)

		startUpload := time.Now()
		addr, checksum := uploadRandomFile(t, sim.MustNodeItem(nodes[nodeIndex], bucketKeyAPI).(*api.API), fileSize)
		log.Warn("FILTER test upload", "iteration", i, "upload", time.Since(startUpload), "checksum", checksum)

		startSyncing := time.Now()

		time.Sleep(1 * time.Second)

		for syncing := true; syncing; {
			time.Sleep(100 * time.Millisecond)
			syncing = false
			for _, n := range nodes {
				if sim.MustNodeItem(n, bucketKeyInspector).(*api.Inspector).IsPullSyncing() {
					syncing = true
				}
			}
		}
		log.Warn("FILTER test syncing", "iteration", i, "syncing", time.Since(startSyncing)-api.InspectorIsPullSyncingTolerance)

		retrievalStart := time.Now()
		for _, n := range nodes {
			checkFile(t, sim.MustNodeItem(n, bucketKeyAPI).(*api.API), addr, checksum)
		}
		log.Warn("FILTER test retrieval", "iteration", i, "retrieval", time.Since(retrievalStart))
		log.Warn("FILTER test done", "iteration", i, "duration", time.Since(startUpload)-api.InspectorIsPullSyncingTolerance)
	}
}

func newServiceFunc(bootnode bool) func(ctx *adapters.ServiceContext, bucket *sync.Map) (s node.Service, cleanup func(), err error) {
	return func(ctx *adapters.ServiceContext, bucket *sync.Map) (s node.Service, cleanup func(), err error) {
		config := api.NewConfig()

		config.BootnodeMode = bootnode

		dir, err := ioutil.TempDir("", "swarm-sync-test-node")
		if err != nil {
			return nil, nil, err
		}
		cleanup = func() {
			err := os.RemoveAll(dir)
			if err != nil {
				log.Error("cleaning up swarm temp dir", "err", err)
			}
		}

		config.Path = dir

		privkey, err := crypto.GenerateKey()
		if err != nil {
			return nil, cleanup, err
		}
		nodekey, err := crypto.GenerateKey()
		if err != nil {
			return nil, cleanup, err
		}

		config.Init(privkey, nodekey)
		config.Port = ""

		swarm, err := NewSwarm(config, nil)
		if err != nil {
			return nil, cleanup, err
		}
		bucket.Store(simulation.BucketKeyKademlia, swarm.bzz.Hive.Kademlia)
		bucket.Store(bucketKeyLocalStore, swarm.netStore.Store)
		bucket.Store(bucketKeyAPI, swarm.api)
		bucket.Store(bucketKeyInspector, api.NewInspector(swarm.api, swarm.bzz.Hive, swarm.netStore, swarm.newstreamer))
		log.Info("new swarm", "bzzKey", config.BzzKey, "baseAddr", fmt.Sprintf("%x", swarm.bzz.BaseAddr()))
		return swarm, cleanup, nil
	}
}

func uploadRandomFile(t *testing.T, a *api.API, length int64) (chunk.Address, string) {
	t.Helper()

	ctx := context.Background()

	hasher := md5.New()

	key, wait, err := a.Store(
		ctx,
		io.TeeReader(io.LimitReader(random, length), hasher),
		length,
		false,
	)
	if err != nil {
		t.Fatalf("store file: %v", err)
	}

	if err := wait(ctx); err != nil {
		t.Fatalf("wait for file to be stored: %v", err)
	}

	return key, hex.EncodeToString(hasher.Sum(nil))
}

func storeFile(ctx context.Context, a *api.API, r io.Reader, length int64, contentType string, toEncrypt bool) (k storage.Address, wait func(context.Context) error, err error) {
	key, wait, err := a.Store(ctx, r, length, toEncrypt)
	if err != nil {
		return nil, nil, err
	}
	return key, wait, nil
}

func checkFile(t *testing.T, a *api.API, addr chunk.Address, checksum string) {
	t.Helper()

	r, _ := a.Retrieve(context.Background(), addr)

	hasher := md5.New()

	n, err := io.Copy(hasher, r)
	if err != nil {
		t.Fatal(err)
	}

	got := hex.EncodeToString(hasher.Sum(nil))

	if got != checksum {
		t.Fatalf("got file checksum %s (length %v), want %s", got, n, checksum)
	}
}
