// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package downloader

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/internal/era/onedb"
)

// newEraTester creates a full sync downloader tester whose chain database is
// backed by a file-based freezer in datadir, and thus by the era store in it.
func newEraTester(t *testing.T, datadir string, success func()) *downloadTester {
	db, err := rawdb.Open(rawdb.NewMemoryDatabase(), rawdb.OpenOptions{Ancient: datadir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	chain, err := core.NewBlockChain(db, testGspec, ethash.NewFaker(), core.DefaultConfig().WithArchive(true))
	if err != nil {
		t.Fatal(err)
	}
	tester := &downloadTester{
		db:    db,
		chain: chain,
		peers: make(map[string]*downloadTesterPeer),
	}
	tester.downloader = New(db, FullSync, chain, tester.dropPeer, success, false)
	return tester
}

// writeEraFile exports the given blocks, starting at genesis, as the first era1
// epoch into the era store of the datadir.
func writeEraFile(t *testing.T, datadir string, source *core.BlockChain, blocks []*types.Block) {
	dir := filepath.Join(datadir, rawdb.ChainFreezerName, "era")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	var (
		builder = onedb.NewBuilder(f)
		td      = new(big.Int)
	)
	for _, block := range blocks {
		td.Add(td, block.Difficulty())
		receipts := source.GetReceiptsByHash(block.Hash())
		if receipts == nil {
			receipts = types.Receipts{}
		}
		if err := builder.Add(block, receipts, new(big.Int).Set(td)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := builder.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, onedb.Filename("mainnet", 0, root))); err != nil {
		t.Fatal(err)
	}
}

// Tests that full sync executes the chain segment covered by the era files in the
// datadir from there, without retrieving those block bodies from the network.
func TestEraFullSync(t *testing.T) {
	var (
		datadir = t.TempDir()
		chain   = testChainBase.shorten(blockCacheMaxItems - 15)
		source  = newTestBlockchain(chain.blocks[1:])
		eraLen  = len(chain.blocks) / 2
	)
	writeEraFile(t, datadir, source, chain.blocks[:eraLen])

	success := make(chan struct{})
	tester := newEraTester(t, datadir, func() { close(success) })
	defer tester.terminate()

	// The only peer refuses to serve the bodies covered by the era file.
	peer := tester.newPeerWithChain("peer", eth.ETH69, source, nil)
	peer.withholdBelow = uint64(eraLen)

	if err := tester.downloader.BeaconSync(chain.blocks[len(chain.blocks)-1].Header(), nil); err != nil {
		t.Fatalf("failed to beacon-sync chain: %v", err)
	}
	select {
	case <-success:
	case <-time.After(10 * time.Second):
		t.Fatalf("failed to sync chain in ten seconds, head %d", tester.chain.CurrentBlock().Number)
	}
	assertOwnChain(t, tester, len(chain.blocks))

	// The chain was executed in archive mode, every historical state must be there.
	for _, block := range chain.blocks {
		if have := tester.chain.GetCanonicalHash(block.NumberU64()); have != block.Hash() {
			t.Fatalf("block %d: canonical hash mismatch: have %x, want %x", block.NumberU64(), have, block.Hash())
		}
		if !tester.chain.HasState(block.Root()) {
			t.Fatalf("block %d: missing state %x", block.NumberU64(), block.Root())
		}
	}
	if have, want := tester.chain.CurrentBlock().Root, source.CurrentBlock().Root; have != want {
		t.Fatalf("head state root mismatch: have %x, want %x", have, want)
	}
}

// Tests that block bodies in the era files which don't match the canonical chain
// are ignored, and the chain segment is retrieved from the network instead.
func TestEraFullSyncInvalidBody(t *testing.T) {
	var (
		datadir = t.TempDir()
		chain   = testChainBase.shorten(blockCacheMaxItems - 15)
		source  = newTestBlockchain(chain.blocks[1:])
		eraLen  = len(chain.blocks) / 2
		bad     = 23 // block with a transaction, see testChain.generate
	)
	blocks := append([]*types.Block{}, chain.blocks[:eraLen]...)
	blocks[bad] = types.NewBlockWithHeader(blocks[bad].Header()).WithBody(types.Body{})
	writeEraFile(t, datadir, source, blocks)

	success := make(chan struct{})
	tester := newEraTester(t, datadir, func() { close(success) })
	defer tester.terminate()

	peer := tester.newPeerWithChain("peer", eth.ETH69, source, nil)

	if err := tester.downloader.BeaconSync(chain.blocks[len(chain.blocks)-1].Header(), nil); err != nil {
		t.Fatalf("failed to beacon-sync chain: %v", err)
	}
	select {
	case <-success:
	case <-time.After(10 * time.Second):
		t.Fatalf("failed to sync chain in ten seconds, head %d", tester.chain.CurrentBlock().Number)
	}
	assertOwnChain(t, tester, len(chain.blocks))

	// Everything below the invalid body was imported from the era file, while
	// the invalid block and its descendants came from the network.
	if have := peer.bodiesServedBelow(uint64(bad)); have != 0 {
		t.Fatalf("bodies below the invalid era block retrieved from the network: %d", have)
	}
	if peer.bodiesServedBelow(uint64(eraLen)) == 0 {
		t.Fatal("bodies above the invalid era block not retrieved from the network")
	}
	if have := tester.chain.GetBlockByNumber(uint64(bad)); have.Hash() != chain.blocks[bad].Hash() || len(have.Transactions()) == 0 {
		t.Fatalf("invalid block imported: %v", have.Hash())
	}
	if rawdb.ReadBadBlock(tester.db, chain.blocks[bad].Hash()) != nil {
		t.Fatal("canonical block reported as bad")
	}
}
