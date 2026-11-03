package mempool

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

// Ensure mempoolTx implements MempoolTx.
var _ MempoolTx = (*mempoolTx)(nil)

// mempoolTx is an entry in the mempool.
type mempoolTx struct {
	height    int64    // height that this tx had been validated in
	gasWanted int64    // amount of gas this tx states it will require
	tx        types.Tx // validated by the application
	lane      LaneID
	seq       int64
	timestamp time.Time // time when entry was created

	// ids of peers who've sent us this tx (as a map for quick lookups).
	// senders: PeerID -> bool
	senders sync.Map
}

// NewMempoolTx creates a new mempoolTx using the builder pattern.
func NewMempoolTx(tx types.Tx) MempoolTx {
	return NewMempoolTxBuilder().
		WithTx(tx).
		Build()
}

func (memTx *mempoolTx) Tx() types.Tx {
	return memTx.tx
}

func (memTx *mempoolTx) Height() int64 {
	return atomic.LoadInt64(&memTx.height)
}

func (memTx *mempoolTx) GasWanted() int64 {
	return atomic.LoadInt64(&memTx.gasWanted)
}

func (memTx *mempoolTx) IsSender(peerID p2p.ID) bool {
	_, ok := memTx.senders.Load(peerID)
	return ok
}

func (memTx *mempoolTx) AddSender(peerID p2p.ID) bool {
	return memTx.addSender(peerID)
}

// Add the peer ID to the list of senders. Return true iff it exists already in the list.
func (memTx *mempoolTx) addSender(peerID p2p.ID) bool {
	if len(peerID) == 0 {
		return false
	}
	if _, loaded := memTx.senders.LoadOrStore(peerID, struct{}{}); loaded {
		return true
	}
	return false
}

func (memTx *mempoolTx) Senders() []p2p.ID {
	senders := make([]p2p.ID, 0)
	memTx.senders.Range(func(key, _ any) bool {
		senders = append(senders, key.(p2p.ID))
		return true
	})
	return senders
}

// MempoolTxBuilder is a builder for creating mempoolTx instances.
//
//nolint:revive // exported name is part of the API that injective-core uses
type MempoolTxBuilder struct {
	height    int64
	gasWanted int64
	tx        types.Tx
	senders   []p2p.ID
}

// NewMempoolTxBuilder creates a new builder for mempoolTx.
func NewMempoolTxBuilder() *MempoolTxBuilder {
	return &MempoolTxBuilder{
		senders: make([]p2p.ID, 0),
	}
}

// WithHeight sets the height for the mempoolTx.
func (b *MempoolTxBuilder) WithHeight(height int64) *MempoolTxBuilder {
	b.height = height
	return b
}

// WithGasWanted sets the gas wanted for the mempoolTx.
func (b *MempoolTxBuilder) WithGasWanted(gasWanted int64) *MempoolTxBuilder {
	b.gasWanted = gasWanted
	return b
}

// WithTx sets the transaction for the mempoolTx.
func (b *MempoolTxBuilder) WithTx(tx types.Tx) *MempoolTxBuilder {
	b.tx = tx
	return b
}

// WithSender adds a peer to the senders of the mempoolTx.
func (b *MempoolTxBuilder) WithSender(sender p2p.ID) *MempoolTxBuilder {
	b.senders = append(b.senders, sender)
	return b
}

// Build creates the final mempoolTx instance.
func (b *MempoolTxBuilder) Build() MempoolTx {
	memTx := &mempoolTx{
		height:    b.height,
		gasWanted: b.gasWanted,
		tx:        b.tx,
	}
	for _, sender := range b.senders {
		memTx.senders.Store(sender, struct{}{})
	}
	return memTx
}
