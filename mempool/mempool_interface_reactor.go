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
// the proxy mempool. It keeps one broadcast queue per peer and doesn't
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

	// Map of peer ID to its *peerTxQueue.
	peerQueues sync.Map

	// Stream for getting transactions to broadcast
	txStream TxBroadcastStream

	// Highest tx height seen on txStream, used as the height of the txs read
	// from the mempool directly.
	streamHeight atomic.Int64
}

// NewMempoolInterfaceReactor returns a new MempoolInterfaceReactor with the given config and mempool.
func NewMempoolInterfaceReactor(config *cfg.MempoolConfig, mempool Mempool, txStream TxBroadcastStream, waitSync bool) *MempoolInterfaceReactor {
	memR := &MempoolInterfaceReactor{
		config:     config,
		mempool:    mempool,
		peerQueues: sync.Map{},
		txStream:   txStream,
		waitSync:   atomic.Bool{},
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

			queue := newPeerTxQueue(peer, memR.config.Size)
			if !memR.storePeerQueue(queue) {
				return
			}
			// Remove our own entry on exit. An entry stored for a newer peer
			// with the same ID is left untouched.
			defer memR.peerQueues.CompareAndDelete(peer.ID(), queue)

			// Start the broadcast routine
			memR.broadcastTxPeerRoutine(peer, queue)
		}()
	}
}

// storePeerQueue stores queue for its peer. It replaces the entry of an
// earlier peer with the same ID: a peer can stop and reconnect before the
// routine of the earlier peer has removed its entry. It returns false if
// another routine already serves this peer.
func (memR *MempoolInterfaceReactor) storePeerQueue(queue *peerTxQueue) bool {
	for {
		existing, loaded := memR.peerQueues.LoadOrStore(queue.peer.ID(), queue)
		if !loaded {
			return true
		}
		if existing.(*peerTxQueue).peer == queue.peer {
			return false
		}
		if memR.peerQueues.CompareAndSwap(queue.peer.ID(), existing, queue) {
			return true
		}
	}
}

// RemovePeer implements Reactor. It removes the queue of peer, but not one
// that a newer peer with the same ID already stored.
//
// broadcastTxPeerRoutine exits on peer.Quit(), and the queue is garbage
// collected once broadcastTxRoutine doesn't reference it anymore.
func (memR *MempoolInterfaceReactor) RemovePeer(peer p2p.Peer, _ any) {
	if v, ok := memR.peerQueues.Load(peer.ID()); ok && v.(*peerTxQueue).peer == peer {
		memR.peerQueues.CompareAndDelete(peer.ID(), v)
	}
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *MempoolInterfaceReactor) Receive(e p2p.Envelope) {
	if memR.WaitSync() {
		memR.Logger.Debug("Ignore message received while syncing", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		return
	}

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

// broadcastTxPeerRoutine sends txs to peer: the txs in the mempool when the
// routine starts and whenever the queue overflowed, and the txs that
// broadcastTxRoutine adds to queue otherwise.
func (memR *MempoolInterfaceReactor) broadcastTxPeerRoutine(peer p2p.Peer, queue *peerTxQueue) {
	// If the node is catching up, don't start sending txs to the peer.
	if memR.WaitSync() {
		select {
		case <-memR.waitSyncCh:
			// EnableInOutTxs() has set WaitSync() to false.
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}

	var (
		// Txs read from the mempool that remain to be sent.
		pending []MempoolTx
		// Keys of the txs sent to the peer, so that a tx in both the mempool
		// and the queue is sent once. It is pruned to the txs still in the
		// mempool on each resync, and reset if it grows too large between
		// resyncs, in which case a few txs may be sent twice.
		sent = make(map[types.TxKey]struct{})
	)
	for {
		if queue.takeResync() {
			// The mempool is read after the resync flag is cleared, so every tx
			// streamed in between is in the mempool or in the queue.
			pending, sent = memR.mempoolTxs(sent)
		}

		var memTx MempoolTx
		if len(pending) > 0 {
			memTx, pending = pending[0], pending[1:]
		} else {
			memTx = queue.pop()
			if memTx == nil {
				select {
				case <-queue.notify:
				case <-peer.Quit():
					return
				case <-memR.Quit():
					return
				}
				continue
			}
		}

		key := memTx.Tx().Key()
		if _, ok := sent[key]; ok {
			continue
		}
		if !memR.sendTx(peer, memTx) {
			return
		}

		if len(sent) >= 2*queue.capacity {
			clear(sent)
		}
		sent[key] = struct{}{}
	}
}

// mempoolTxs returns the txs currently in the mempool, with their senders,
// except the ones whose key is in sent. It also returns the keys in sent of
// the txs still in the mempool.
//
// The height the txs were validated at is unknown, so it is set to the latest
// height seen on the tx stream.
func (memR *MempoolInterfaceReactor) mempoolTxs(sent map[types.TxKey]struct{}) ([]MempoolTx, map[types.TxKey]struct{}) {
	height := memR.streamHeight.Load()
	txs := memR.mempool.ReapMaxTxs(-1)
	memTxs := make([]MempoolTx, 0, len(txs))
	stillSent := make(map[types.TxKey]struct{})
	for _, tx := range txs {
		key := tx.Key()
		if _, ok := sent[key]; ok {
			stillSent[key] = struct{}{}
			continue
		}
		b := NewMempoolTxBuilder().WithTx(tx).WithHeight(height)
		senders, err := memR.mempool.GetSenders(key)
		if err == nil {
			for _, sender := range senders {
				b.WithSender(sender)
			}
		}
		memTxs = append(memTxs, b.Build())
	}
	return memTxs, stillSent
}

// sendTx sends memTx to peer unless the peer sent it to us. It retries until
// the peer has caught up with the tx height and has room for the message. It
// returns false if the peer or the reactor stopped before the tx was sent.
func (memR *MempoolInterfaceReactor) sendTx(peer p2p.Peer, memTx MempoolTx) bool {
	if memTx.IsSender(peer.ID()) {
		return true
	}

	for {
		// The peer may not have a state yet. We set it in the consensus
		// reactor, but when we add peer in Switch, the order we call
		// reactors#AddPeer is different every time due to us using a map.
		// Sometimes other reactors will be initialized before the consensus
		// reactor. We wait a few milliseconds and retry.
		//
		// If the peer is lagging behind by more than one block, we don't send
		// the tx yet, as the peer can't validate it. See [RFC 103].
		//
		// [RFC 103]: https://github.com/CometBFT/cometbft/blob/main/docs/references/rfc/rfc-103-incoming-txs-when-catching-up.md
		peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
		if ok && peerState.GetHeight()+1 >= memTx.Height() {
			if peer.Send(p2p.Envelope{
				ChannelID: MempoolChannel,
				Message:   &protomem.Txs{Txs: [][]byte{memTx.Tx()}},
			}) {
				return true
			}
		}

		select {
		case <-time.After(PeerCatchupSleepIntervalMS * time.Millisecond):
		case <-peer.Quit():
			return false
		case <-memR.Quit():
			return false
		}
	}
}

// broadcastTxRoutine fans the txs from txStream out to the broadcast channels
// of all peers.
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
		select {
		case tx, ok := <-txChan:
			if !ok {
				memR.Logger.Error("Tx broadcast stream closed, broadcasting is disabled")
				return
			}
			if tx == nil {
				continue
			}
			if h := tx.Height(); h > memR.streamHeight.Load() {
				memR.streamHeight.Store(h)
			}
			memR.peerQueues.Range(func(key, value any) bool {
				if value.(*peerTxQueue).push(tx) {
					memR.Logger.Debug("Peer broadcast queue is full, resending the mempool", "peer", key)
				}
				return true
			})
		case <-memR.Quit():
			return
		}
	}
}

// peerTxQueue holds the streamed txs that remain to be sent to a peer. When
// it is full, it drops its txs and asks the peer routine to resync, that is
// to send all txs in the mempool, instead of dropping new txs. The dropped
// txs are in the mempool, since the stream only carries txs added to it.
type peerTxQueue struct {
	peer     p2p.Peer
	capacity int
	notify   chan struct{} // signaled when a tx is added or a resync is requested

	mtx    sync.Mutex
	txs    []MempoolTx
	resync bool
}

// newPeerTxQueue returns a queue for peer that holds up to capacity txs. It
// starts with a resync, so that the peer gets the txs that are already in the
// mempool.
func newPeerTxQueue(peer p2p.Peer, capacity int) *peerTxQueue {
	return &peerTxQueue{
		peer:     peer,
		capacity: max(capacity, 1),
		notify:   make(chan struct{}, 1),
		resync:   true,
	}
}

// push adds tx to the queue. If the queue is full, it drops all txs and
// requests a resync instead; it returns true in that case.
func (q *peerTxQueue) push(tx MempoolTx) (overflowed bool) {
	q.mtx.Lock()
	switch {
	case len(q.txs) >= q.capacity:
		q.txs = nil
		q.resync = true
		overflowed = true
	default:
		q.txs = append(q.txs, tx)
	}
	q.mtx.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return overflowed
}

// pop removes and returns the oldest tx in the queue, or nil if it is empty.
func (q *peerTxQueue) pop() MempoolTx {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	if len(q.txs) == 0 {
		return nil
	}
	tx := q.txs[0]
	q.txs[0] = nil
	q.txs = q.txs[1:]
	return tx
}

// takeResync reports whether a resync was requested, and clears the request.
func (q *peerTxQueue) takeResync() bool {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	resync := q.resync
	q.resync = false
	return resync
}
