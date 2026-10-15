package mempool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	protomem "github.com/cometbft/cometbft/api/cometbft/mempool/v2"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

// MempoolInterfaceReactor handles mempool tx broadcasting amongst peers for
// the proxy mempool. It keeps one broadcast channel per peer and doesn't
// gossip a tx back to the peers it was received from.
//
//nolint:revive // named after NewMempoolInterfaceReactor, next to the flood Reactor
type MempoolInterfaceReactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool Mempool

	waitSync   atomic.Bool
	waitSyncCh chan struct{} // for signaling when to start receiving and sending txs

	// Semaphores to keep track of how many connections to peers are active for broadcasting
	// transactions. Each semaphore has a capacity that puts an upper bound on the number of
	// connections for different groups of peers.
	activePersistentPeersSemaphore    *semaphore.Weighted
	activeNonPersistentPeersSemaphore *semaphore.Weighted

	// Map of peer ID to their broadcast channel
	peerBroadcastChannels sync.Map

	// Stream for getting transactions to broadcast
	txStream TxBroadcastStream
}

// NewMempoolInterfaceReactor returns a new MempoolInterfaceReactor with the given config and mempool.
func NewMempoolInterfaceReactor(config *cfg.MempoolConfig, mempool Mempool, txStream TxBroadcastStream, waitSync bool) *MempoolInterfaceReactor {
	memR := &MempoolInterfaceReactor{
		config:                config,
		mempool:               mempool,
		peerBroadcastChannels: sync.Map{},
		txStream:              txStream,
		waitSync:              atomic.Bool{},
	}
	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)
	if waitSync {
		memR.waitSync.Store(true)
		memR.waitSyncCh = make(chan struct{})
	}
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))

	return memR
}

// InitPeer implements Reactor by creating a state for the peer.
func (*MempoolInterfaceReactor) InitPeer(peer p2p.Peer) p2p.Peer {
	return peer
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *MempoolInterfaceReactor) SetLogger(l log.Logger) {
	memR.Logger = l
}

// OnStart implements p2p.BaseReactor.
func (memR *MempoolInterfaceReactor) OnStart() error {
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	} else {
		go memR.broadcastTxRoutine()
	}
	return nil
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *MempoolInterfaceReactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{
		Sum: &protomem.Message_Txs{
			Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
		},
	}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *MempoolInterfaceReactor) AddPeer(peer p2p.Peer) {
	if memR.config.Broadcast {
		go func() {
			// Always forward transactions to unconditional peers.
			if !memR.Switch.IsPeerUnconditional(peer.ID()) {
				// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
				var peerSemaphore *semaphore.Weighted
				if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
					peerSemaphore = memR.activePersistentPeersSemaphore
				} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
					peerSemaphore = memR.activeNonPersistentPeersSemaphore
				}

				if peerSemaphore != nil {
					acquired := false
					for peer.IsRunning() {
						// Block on the semaphore until a slot is available to start gossiping with this peer.
						// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
						ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
						// Block sending transactions to peer until one of the connections become
						// available in the semaphore.
						err := peerSemaphore.Acquire(ctxTimeout, 1)
						cancel()

						if err == nil {
							acquired = true
							break
						}
					}

					if acquired {
						// Release semaphore to allow other peer to start sending transactions.
						defer peerSemaphore.Release(1)
					}
				}
			}

			// Check if peer is still running after semaphore acquisition
			if !peer.IsRunning() {
				return
			}

			peerChan := make(chan MempoolTx, memR.config.Size)

			// Store the channel atomically. The new channel was never shared, so it
			// is simply dropped if another routine already serves this peer.
			if _, loaded := memR.peerBroadcastChannels.LoadOrStore(peer.ID(), peerChan); loaded {
				return
			}
			// Remove our own entry on exit, so a stale entry left by a peer that
			// disconnected before LoadOrStore can't block a reconnecting peer. A
			// newer channel stored for the same peer ID is left untouched.
			defer memR.peerBroadcastChannels.CompareAndDelete(peer.ID(), peerChan)

			// Start the broadcast routine
			memR.broadcastTxPeerRoutine(peer, peerChan)
		}()
	}
}

// RemovePeer implements Reactor.
//
// The peer channel is deliberately not closed: broadcastTxRoutine may have
// already loaded it from peerBroadcastChannels and a send on a closed channel
// panics. broadcastTxPeerRoutine exits on peer.Quit() and the unreferenced
// channel is garbage collected.
func (memR *MempoolInterfaceReactor) RemovePeer(peer p2p.Peer, _ any) {
	memR.peerBroadcastChannels.Delete(peer.ID())
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *MempoolInterfaceReactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	switch msg := e.Message.(type) {
	case *protomem.Txs:
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("received empty txs from peer", "src", e.Src)
			return
		}

		var err error
		for _, tx := range protoTxs {
			ntx := types.Tx(tx)
			_, err = memR.mempool.CheckTx(ntx, e.Src.ID())
			if err != nil {
				switch {
				case errors.Is(err, ErrTxInCache):
					memR.Logger.Debug("Tx already exists in cache", "tx", ntx.String())
				case errors.As(err, &ErrMempoolIsFull{}):
					// using debug level to avoid flooding when traffic is high
					memR.Logger.Debug(err.Error())
				default:
					memR.Logger.Info("Could not check tx", "tx", ntx.String(), "err", err)
				}
			}
		}
	default:
		memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

func (memR *MempoolInterfaceReactor) EnableInOutTxs() {
	memR.Logger.Info("Enabling inbound and outbound transactions")
	if !memR.waitSync.CompareAndSwap(true, false) {
		return
	}

	// Releases all the blocked broadcastTxRoutine instances.
	if memR.config.Broadcast {
		close(memR.waitSyncCh)
	}
}

func (memR *MempoolInterfaceReactor) WaitSync() bool {
	return memR.waitSync.Load()
}

// Send new mempool txs to peer.
func (memR *MempoolInterfaceReactor) broadcastTxPeerRoutine(peer p2p.Peer, peerChan chan MempoolTx) {
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		// Make sure the peer is up to date.
		peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
		if !ok {
			// Peer does not have a state yet. We set it in the consensus reactor, but
			// when we add peer in Switch, the order we call reactors#AddPeer is
			// different every time due to us using a map. Sometimes other reactors
			// will be initialized before the consensus reactor. We should wait a few
			// milliseconds and retry.
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		select {
		case memTx, ok := <-peerChan:
			if !ok {
				return
			}

			if peerState.GetHeight() < memTx.Height()-1 {
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}

			if !memTx.IsSender(peer.ID()) {
				success := peer.Send(p2p.Envelope{
					ChannelID: MempoolChannel,
					Message:   &protomem.Txs{Txs: [][]byte{memTx.Tx()}},
				})
				if !success {
					time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
					continue
				}
			}
		case <-peer.Quit():
			return
		}
	}
}

// broadcastTxRoutine broadcasts transactions from the mempool to all peers.
func (memR *MempoolInterfaceReactor) broadcastTxRoutine() {
	// If the node is catching up, don't start this routine immediately.
	if memR.WaitSync() {
		select {
		case <-memR.waitSyncCh:
			// EnableInOutTxs() has set WaitSync() to false.
		case <-memR.Quit():
			return
		}
	}

	// Check if txStream is set
	if memR.txStream == nil {
		memR.Logger.Error("txStream is not set, broadcasting is disabled")
		return
	}

	txChan := memR.txStream.GetTxChannel()

	for {
		if !memR.IsRunning() {
			return
		}

		select {
		case tx := <-txChan:
			if tx != nil {
				memR.peerBroadcastChannels.Range(func(key, value any) bool {
					peerID := key.(p2p.ID)
					ch := value.(chan MempoolTx)

					select {
					case ch <- tx:
					default:
						memR.Logger.Debug("peer broadcast channel is full", "peerID", peerID)
					}
					return true
				})
			}
		case <-memR.Quit():
			return
		}
	}
}
