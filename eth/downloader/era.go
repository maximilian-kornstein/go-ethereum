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
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

// importEraBlocks executes the chain segment directly above origin from the era
// files in the local datadir, instead of downloading the block bodies from the
// network. It is only used in full sync, and returns the number of the last block
// imported, which becomes the new origin for the network sync.
//
// Era files are untrusted: headers are always taken from the beacon-guided skeleton
// chain and a body is only used if it matches its canonical header. The import
// stops at the first block which is not available in, or doesn't match, the era
// files and the remainder of the chain is retrieved from the network as usual.
func (d *Downloader) importEraBlocks(origin uint64) (uint64, error) {
	var (
		start  = time.Now()
		first  = origin + 1
		hasher = trie.NewStackTrie(nil)
	)
	for {
		select {
		case <-d.cancelCh:
			return origin, errCanceled
		default:
		}
		blocks := make([]*types.Block, 0, maxResultsProcess)
		for number := origin + 1; len(blocks) < maxResultsProcess; number++ {
			header := d.skeleton.Header(number)
			if header == nil {
				break
			}
			block, err := readEraBlock(d.stateDB, header, hasher)
			if err != nil {
				log.Warn("Unusable block in era files", "number", number, "hash", header.Hash(), "err", err)
			}
			if block == nil {
				break
			}
			blocks = append(blocks, block)
		}
		if len(blocks) == 0 {
			break
		}
		if origin+1 == first {
			log.Info("Importing chain segment from era files", "from", first)
		}
		if index, err := d.blockchain.InsertChain(blocks); err != nil {
			if index < len(blocks) && d.badBlock != nil {
				if head, _, _, err := d.skeleton.Bounds(); err == nil {
					d.badBlock(blocks[index].Header(), head)
				}
			}
			return origin, fmt.Errorf("%w: %v", errInvalidChain, err)
		}
		// The chain insertion may have been interrupted by a sync cancellation, in
		// which case only a part of the batch made it into the chain.
		last := blocks[len(blocks)-1]
		if d.blockchain.CurrentBlock().Hash() != last.Hash() {
			return origin, errCanceled
		}
		importEraBlocksMeter.Mark(int64(len(blocks)))
		origin = last.NumberU64()
	}
	if origin >= first {
		log.Info("Imported chain segment from era files", "blocks", origin-first+1, "number", origin,
			"hash", d.blockchain.CurrentBlock().Hash(), "elapsed", common.PrettyDuration(time.Since(start)))
	}
	return origin, nil
}

// readEraBlock assembles the block of the given canonical header with the body
// stored in the local era files. Nil is returned if the block isn't available in
// the era files, and an error if the stored body doesn't match the header.
func readEraBlock(db ethdb.Reader, header *types.Header, hasher *trie.StackTrie) (*types.Block, error) {
	data, err := rawdb.ReadEraBodyRLP(db, header.Number.Uint64())
	if err != nil || len(data) == 0 {
		return nil, err
	}
	body := new(types.Body)
	if err := rlp.DecodeBytes(data, body); err != nil {
		return nil, fmt.Errorf("invalid body: %v", err)
	}
	if err := verifyEraBody(header, body, hasher); err != nil {
		return nil, err
	}
	return types.NewBlockWithHeader(header).WithBody(*body), nil
}

// verifyEraBody checks whether a block body matches the given header.
func verifyEraBody(header *types.Header, body *types.Body, hasher *trie.StackTrie) error {
	if hash := types.CalcUncleHash(body.Uncles); hash != header.UncleHash {
		return fmt.Errorf("uncle root mismatch: have %x, want %x", hash, header.UncleHash)
	}
	if hash := types.DeriveSha(types.Transactions(body.Transactions), hasher); hash != header.TxHash {
		return fmt.Errorf("transaction root mismatch: have %x, want %x", hash, header.TxHash)
	}
	if header.WithdrawalsHash == nil {
		if body.Withdrawals != nil {
			return errors.New("unexpected withdrawals")
		}
		return nil
	}
	if body.Withdrawals == nil {
		return errors.New("missing withdrawals")
	}
	if hash := types.DeriveSha(types.Withdrawals(body.Withdrawals), hasher); hash != *header.WithdrawalsHash {
		return fmt.Errorf("withdrawal root mismatch: have %x, want %x", hash, *header.WithdrawalsHash)
	}
	return nil
}

const (
	// eraFillOwner marks the parts of the skeleton scratch space which were
	// filled from the local era files instead of a remote peer.
	eraFillOwner = "era-files"

	// eraFillTasks is the maximum number of skeleton scratch space tasks to fill
	// from the local era files at once, keeping the skeleton runloop responsive.
	eraFillTasks = 16
)

// fillFromEra fills the free parts of the skeleton scratch space with headers
// read from the local era files, instead of retrieving them from the network,
// and consumes them into the primary subchain. The returned filled flag reports
// whether any headers were taken from the era files, in which case more might be
// available after the scratch space was consumed.
//
// Era files are untrusted: the headers go through the same validation as the
// ones delivered by peers, i.e. each must hash-link to the trusted subchain above
// it, otherwise the era files are disregarded for the rest of the sync cycle and
// the headers are retrieved from the network.
func (s *skeleton) fillFromEra() (filled bool, linked bool, merged bool) {
	if s.eraFailed {
		return false, false, false
	}
	var tasks int
	for task, owner := range s.scratchOwners {
		if tasks == eraFillTasks {
			break
		}
		// Skip any tasks already filling, stop when reaching the genesis
		if owner != "" {
			continue
		}
		if uint64(task*requestHeaders) >= s.scratchHead {
			break
		}
		head := s.scratchHead - uint64(task*requestHeaders)
		if head == s.eraMiss {
			break
		}
		headers, err := s.readEraHeaders(head)
		if err != nil {
			log.Warn("Unusable headers in era files", "head", head, "err", err)
			s.revertEraTasks()
			return false, false, false
		}
		if headers == nil {
			s.eraMiss = head
			break
		}
		if !s.eraLogged {
			log.Info("Filling beacon headers from era files", "from", head)
			s.eraLogged = true
		}
		copy(s.scratchSpace[task*requestHeaders:], headers)
		s.scratchOwners[task] = eraFillOwner
		tasks++
	}
	if tasks == 0 {
		return false, false, false
	}
	linked, merged = s.consumeScratch()
	return true, linked, merged
}

// readEraHeaders reads the batch of headers starting at head and going backwards
// from the local era files, checking their hash progression. Similarly to header
// requests sent to peers, the genesis header is never included. Nil is returned
// if the era files don't cover the entire batch.
func (s *skeleton) readEraHeaders(head uint64) ([]*types.Header, error) {
	count := uint64(requestHeaders)
	if head < requestHeaders {
		count = head
	}
	headers := make([]*types.Header, 0, count)
	for number := head; number > head-count; number-- {
		data, err := rawdb.ReadEraHeaderRLP(s.db, number)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, nil
		}
		header := new(types.Header)
		if err := rlp.DecodeBytes(data, header); err != nil {
			return nil, fmt.Errorf("invalid header %d: %v", number, err)
		}
		if header.Number == nil || header.Number.Uint64() != number {
			return nil, fmt.Errorf("header %d has wrong number %v", number, header.Number)
		}
		if n := len(headers); n > 0 && headers[n-1].ParentHash != header.Hash() {
			return nil, fmt.Errorf("invalid hash progression at header %d", number)
		}
		headers = append(headers, header)
	}
	return headers, nil
}

// revertEraTasks drops the parts of the scratch space filled from the era files,
// and disables filling from them for the rest of the sync cycle.
func (s *skeleton) revertEraTasks() {
	s.eraFailed = true
	for task, owner := range s.scratchOwners {
		if owner != eraFillOwner {
			continue
		}
		for i := task * requestHeaders; i < (task+1)*requestHeaders; i++ {
			s.scratchSpace[i] = nil
		}
		s.scratchOwners[task] = ""
	}
}
