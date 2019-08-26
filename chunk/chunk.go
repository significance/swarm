// Copyright 2019 The go-ethereum Authors
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

package chunk

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

const (
	DefaultSize   = 4096
	MaxPO         = 16
	AddressLength = 32
)

var (
	ErrChunkNotFound = errors.New("chunk not found")
	ErrChunkInvalid  = errors.New("invalid chunk")
)

type Chunk interface {
	Address() Address
	Data() []byte
	PinCounter() uint64
	WithPinCounter(p uint64) Chunk
}

type chunk struct {
	addr       Address
	sdata      []byte
	pinCounter uint64
}

func NewChunk(addr Address, data []byte) Chunk {
	return &chunk{
		addr:  addr,
		sdata: data,
	}
}

func (c *chunk) WithPinCounter(p uint64) Chunk {
	c.pinCounter = p
	return c
}

func (c *chunk) Address() Address {
	return c.addr
}

func (c *chunk) Data() []byte {
	return c.sdata
}

func (c *chunk) PinCounter() uint64 {
	return c.pinCounter
}

func (self *chunk) String() string {
	return fmt.Sprintf("Address: %v Chunksize: %v", self.addr.Log(), len(self.sdata))
}

type Address []byte

var ZeroAddr = Address(common.Hash{}.Bytes())

func (a Address) Hex() string {
	return hex.EncodeToString(a)
}

func (a Address) Log() string {
	if len(a) < 8 {
		return hex.EncodeToString(a)
	}
	return hex.EncodeToString(a[:8])
}

func (a Address) String() string {
	return hex.EncodeToString(a)
}

func (a Address) MarshalJSON() (out []byte, err error) {
	return []byte(`"` + a.String() + `"`), nil
}

func (a *Address) UnmarshalJSON(value []byte) error {
	s := string(value)
	*a = make([]byte, 32)
	h := common.Hex2Bytes(s[1 : len(s)-1])
	copy(*a, h)
	return nil
}

// Proximity returns the proximity order of the MSB distance between x and y
//
// The distance metric MSB(x, y) of two equal length byte sequences x an y is the
// value of the binary integer cast of the x^y, ie., x and y bitwise xor-ed.
// the binary cast is big endian: most significant bit first (=MSB).
//
// Proximity(x, y) is a discrete logarithmic scaling of the MSB distance.
// It is defined as the reverse rank of the integer part of the base 2
// logarithm of the distance.
// It is calculated by counting the number of common leading zeros in the (MSB)
// binary representation of the x^y.
//
// (0 farthest, 255 closest, 256 self)
func Proximity(one, other []byte) (ret int) {
	b := (MaxPO-1)/8 + 1
	if b > len(one) {
		b = len(one)
	}
	m := 8
	for i := 0; i < b; i++ {
		oxo := one[i] ^ other[i]
		for j := 0; j < m; j++ {
			if (oxo>>uint8(7-j))&0x01 != 0 {
				return i*8 + j
			}
		}
	}
	return MaxPO
}

// ModeGet enumerates different Getter modes.
type ModeGet int

func (m ModeGet) String() string {
	switch m {
	case ModeGetRequest:
		return "Request"
	case ModeGetSync:
		return "Sync"
	case ModeGetLookup:
		return "Lookup"
	case ModeGetPin:
		return "PinLookup"
	default:
		return "Unknown"
	}
}

// Getter modes.
const (
	// ModeGetRequest: when accessed for retrieval
	ModeGetRequest ModeGet = iota
	// ModeGetSync: when accessed for syncing or proof of custody request
	ModeGetSync
	// ModeGetLookup: when accessed to lookup a a chunk in feeds or other places
	ModeGetLookup
	// ModeGetPin: used when a pinned chunk is accessed
	ModeGetPin
)

// ModePut enumerates different Putter modes.
type ModePut int

func (m ModePut) String() string {
	switch m {
	case ModePutRequest:
		return "Request"
	case ModePutSync:
		return "Sync"
	case ModePutUpload:
		return "Upload"
	default:
		return "Unknown"
	}
}

// Putter modes.
const (
	// ModePutRequest: when a chunk is received as a result of retrieve request and delivery
	ModePutRequest ModePut = iota
	// ModePutSync: when a chunk is received via syncing
	ModePutSync
	// ModePutUpload: when a chunk is created by local upload
	ModePutUpload
)

// ModeSet enumerates different Setter modes.
type ModeSet int

func (m ModeSet) String() string {
	switch m {
	case ModeSetAccess:
		return "Access"
	case ModeSetSync:
		return "Sync"
	case ModeSetRemove:
		return "Remove"
	case ModeSetPin:
		return "ModeSetPin"
	case ModeSetUnpin:
		return "ModeSetUnpin"
	default:
		return "Unknown"
	}
}

// Setter modes.
const (
	// ModeSetAccess: when an update request is received for a chunk or chunk is retrieved for delivery
	ModeSetAccess ModeSet = iota
	// ModeSetSync: when a chunk is added to a pull sync batch or when a push sync receipt is received
	ModeSetSync
	// ModeSetRemove: when a chunk is removed
	ModeSetRemove
	// ModeSetPin: when a chunk is pinned during upload or separately
	ModeSetPin
	// ModeSetUnpin: when a chunk is unpinned using a command locally
	ModeSetUnpin
)

// Descriptor holds information required for Pull syncing. This struct
// is provided by subscribing to pull index.
type Descriptor struct {
	Address Address
	BinID   uint64
}

func (d *Descriptor) String() string {
	if d == nil {
		return ""
	}
	return fmt.Sprintf("%s bin id %v", d.Address.Hex(), d.BinID)
}

type Store interface {
	Get(ctx context.Context, mode ModeGet, addr Address) (ch Chunk, err error)
	GetMulti(ctx context.Context, mode ModeGet, addrs ...Address) (ch []Chunk, err error)
	Put(ctx context.Context, mode ModePut, chs ...Chunk) (exist []bool, err error)
	Has(ctx context.Context, addr Address) (yes bool, err error)
	HasMulti(ctx context.Context, addrs ...Address) (yes []bool, err error)
	Set(ctx context.Context, mode ModeSet, addrs ...Address) (err error)
	LastPullSubscriptionBinID(bin uint8) (id uint64, err error)
	SubscribePull(ctx context.Context, bin uint8, since, until uint64) (c <-chan Descriptor, stop func())
	Close() (err error)
}

// Validator validates a chunk.
type Validator interface {
	Validate(ch Chunk) bool
}

// ValidatorStore encapsulates Store by decorating the Put method
// with validators check.
type ValidatorStore struct {
	Store
	validators []Validator
}

// NewValidatorStore returns a new ValidatorStore which uses
// provided validators to validate chunks on Put.
func NewValidatorStore(store Store, validators ...Validator) (s *ValidatorStore) {
	return &ValidatorStore{
		Store:      store,
		validators: validators,
	}
}

// Put overrides Store put method with validators check. For Put to succeed,
// all provided chunks must be validated with true by one of the validators.
func (s *ValidatorStore) Put(ctx context.Context, mode ModePut, chs ...Chunk) (exist []bool, err error) {
	for _, ch := range chs {
		if !s.validate(ch) {
			return nil, ErrChunkInvalid
		}
	}
	return s.Store.Put(ctx, mode, chs...)
}

// validate returns true if one of the validators
// return true. If all validators return false,
// the chunk is considered invalid.
func (s *ValidatorStore) validate(ch Chunk) bool {
	for _, v := range s.validators {
		if v.Validate(ch) {
			return true
		}
	}
	return false
}
