package mempool

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	protomem "github.com/cometbft/cometbft/api/cometbft/mempool/v2"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/internal/test"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/mock"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

const (
	testNumTxsMempoolInterface  = 100
	testTimeoutMempoolInterface = 120 * time.Second
)

// mockTxBroadcastStream is a mock implementation of TxBroadcastStream for testing.
type mockTxBroadcastStream struct {
	txChan chan MempoolTx
	logger log.Logger
}

// newMockTxBroadcastStream creates a new mockTxBroadcastStream.
func newMockTxBroadcastStream(logger log.Logger) *mockTxBroadcastStream {
	return &mockTxBroadcastStream{
		txChan: make(chan MempoolTx, testNumTxsMempoolInterface),
		logger: logger,
	}
}

// GetTxChannel returns the transaction channel.
func (m *mockTxBroadcastStream) GetTxChannel() <-chan MempoolTx {
	m.logger.Debug("mockTxBroadcastStream.GetTxChannel called")
	return m.txChan
}

// sendTx sends a transaction to the channel. This is a helper for tests.
func (m *mockTxBroadcastStream) sendTx(tx MempoolTx) {
	m.logger.Debug("mockTxBroadcastStream.sendTx called", "txHash", tx.Tx().Hash(), "height", tx.Height())
	select {
	case m.txChan <- tx:
		m.logger.Debug("mockTxBroadcastStream.sendTx finished sending tx", "txHash", tx.Tx().Hash())
	case <-time.After(5 * time.Second):
		m.logger.Error("mockTxBroadcastStream.sendTx timeout", "txHash", tx.Tx().Hash())
	}
}

// reactorTestPeerState is a mock peer state for testing.
type reactorTestPeerState struct {
	height int64
}

func (ps reactorTestPeerState) GetHeight() int64 {
	return ps.height
}

// newMempoolInterfaceWithAppAndConfig is a helper to create a CListMempool for MempoolInterface tests.
func newMempoolInterfaceWithAppAndConfig(cc proxy.ClientCreator) (*CListMempool, func()) {
	conf := test.ResetTestRoot("mempool_interface_test")

	appConnMem, _ := cc.NewABCIMempoolClient()
	appConnMem.SetLogger(log.TestingLogger().With("module", "abci-client"))
	if err := appConnMem.Start(); err != nil {
		panic(err)
	}

	mp := NewCListMempool(conf.Mempool, appConnMem, nil, 0)
	mp.SetLogger(log.TestingLogger().With("module", "mempool"))

	return mp, func() { os.RemoveAll(conf.RootDir) }
}

// makeAndConnectMempoolInterfaceReactors creates and connects N MempoolInterfaceReactors.
func makeAndConnectMempoolInterfaceReactors(
	t *testing.T,
	conf *cfg.Config,
	n int,
) ([]*MempoolInterfaceReactor, []*p2p.Switch, []*mockTxBroadcastStream, []Mempool) {
	t.Helper()
	reactors := make([]*MempoolInterfaceReactor, n)
	txStreams := make([]*mockTxBroadcastStream, n)
	mempools := make([]Mempool, n)
	logger := log.NewTMLogger(log.NewSyncWriter(os.Stdout)).With("test", "makeAndConnectMempoolInterfaceReactors")

	for i := 0; i < n; i++ {
		app := kvstore.NewInMemoryApplication()

		cc := proxy.NewLocalClientCreator(app)
		mempool, cleanup := newMempoolInterfaceWithAppAndConfig(cc)

		t.Cleanup(cleanup)
		mempool.SetLogger(logger.With("validator", i, "module", "mempool"))
		mempools[i] = mempool

		txStreams[i] = newMockTxBroadcastStream(logger.With("validator", i, "module", "txstream"))

		reactors[i] = NewMempoolInterfaceReactor(conf.Mempool, mempools[i], txStreams[i], false)
		reactors[i].SetLogger(logger.With("validator", i, "module", "mempool-reactor"))
	}

	switches := p2p.MakeConnectedSwitches(conf.P2P, n, func(idx int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("MEMPOOL", reactors[idx])
		s.SetLogger(logger.With("validator", idx, "module", "p2p"))
		return s
	}, p2p.Connect2Switches)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Go(func() {
			for switches[i].Peers().Size() < (n - 1) {
				time.Sleep(100 * time.Millisecond)
			}
		})
	}

	waitTimeout := time.After(15 * time.Second)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-waitTimeout:
		t.Fatalf("Timed out waiting for %d peers to connect (got %d for switch 0, %d for switch 1 if N=2)", n-1, switches[0].Peers().Size(), func() int {
			if n > 1 {
				return switches[1].Peers().Size()
			}
			return 0
		}())
	case <-done:
	}

	return reactors, switches, txStreams, mempools
}

func addRandomTxsToMempoolAndStream(
	t *testing.T,
	mempool Mempool,
	numTxs int,
	senderID p2p.ID,
) (types.Txs, []MempoolTx) {
	t.Helper()
	txs := make(types.Txs, numTxs)
	mempoolTxs := make([]MempoolTx, numTxs)
	for i := 0; i < numTxs; i++ {
		txKey := fmt.Sprintf("key_mempool_interface_%s_%d_%d", senderID, time.Now().UnixNano(), i)
		txValue := fmt.Sprintf("value_%d", i)
		tx := types.Tx(fmt.Sprintf("%s=%s", txKey, txValue))
		txs[i] = tx

		reqres, err := mempool.CheckTx(tx, senderID)
		require.NoError(t, err, "mempool.CheckTx returned an error for tx %X. Error: %v", tx, err)
		reqres.Wait()
		res := reqres.Response.GetCheckTx()

		if res.IsErr() {
			t.Logf("CheckTx callback failed for tx %X: %s, code: %d, log: %s, info: %s", tx, res.Log, res.Code, res.Log, res.Info)
		}
		require.EqualValuesf(t, abci.CodeTypeOK, res.Code, "CheckTx callback response code is not OK for tx %X. Got %d", tx, res.Code)

		mempoolTx := NewMempoolTxBuilder().
			WithHeight(1).
			WithTx(tx).
			WithSender(senderID).
			Build()

		mempoolTxs[i] = mempoolTx
	}
	return txs, mempoolTxs
}

// checkTxsInOrderOnMempoolInterface checks if the mempool of a given reactor contains the expected transactions.
// For N=2, it also checks the order for the receiving reactor.
func checkTxsInOrderOnMempoolInterface(t *testing.T, expectedTxs types.Txs, reactor *MempoolInterfaceReactor, reactorIndex int, numReactors int) {
	t.Helper()
	currentMempool := reactor.mempool
	// Wait for mempool to have enough transactions
	for currentMempool.Size() < len(expectedTxs) {
		if !reactor.IsRunning() {
			t.Logf("Reactor %d is not running, stopping wait for txs", reactorIndex)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	reapedTxs := currentMempool.ReapMaxTxs(len(expectedTxs))
	require.Lenf(t, reapedTxs, len(expectedTxs), "Mempool (reactor %d) did not reap the expected number of txs. Expected %d, got %d. Mempool size: %d", reactorIndex, len(expectedTxs), len(reapedTxs), currentMempool.Size())

	expectedTxsMap := make(map[string]struct{})
	for _, tx := range expectedTxs {
		expectedTxsMap[string(tx)] = struct{}{}
	}

	reapedTxsMap := make(map[string]struct{})
	for _, tx := range reapedTxs {
		reapedTxsMap[string(tx)] = struct{}{}
	}

	for _, tx := range expectedTxs {
		_, ok := reapedTxsMap[string(tx)]
		assert.Truef(t, ok, "Expected tx %X not found in mempool of reactor %d", tx, reactorIndex)
	}

	// For N=2, the order of transactions received by the second reactor should match the broadcast order.
	// The first reactor (sender) will have them in the order they were added.
	if numReactors == 2 && reactorIndex == 1 { // Check order for the receiver in a 2-node setup
		for j, tx := range expectedTxs {
			assert.Equalf(t, tx, reapedTxs[j],
				"txs at index %d on reactor %d don't match: expected %X, got %X", j, reactorIndex, tx, reapedTxs[j])
		}
	} else if reactorIndex == 0 { // For the sender, order should always match
		for j, tx := range expectedTxs {
			assert.Equalf(t, tx, reapedTxs[j],
				"txs at index %d on reactor %d (sender) don't match: expected %X, got %X", j, reactorIndex, tx, reapedTxs[j])
		}
	}
}

// waitForTxsOnMempoolsInterface waits for all transactions to appear on all specified reactors' mempools.
// It's modeled after waitForTxsOnReactors from reactor_test.go.
func waitForTxsOnMempoolsInterface(t *testing.T, txs types.Txs, reactors []*MempoolInterfaceReactor) {
	t.Helper()
	wg := new(sync.WaitGroup)
	for i, reactor := range reactors {
		wg.Go(func() {
			checkTxsInOrderOnMempoolInterface(t, txs, reactor, i, len(reactors))
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.After(testTimeoutMempoolInterface) // Use the specific timeout for this test suite
	select {
	case <-timer:
		for i, r := range reactors {
			t.Logf("Timeout: Reactor %d mempool size: %d, expected: %d", i, r.mempool.Size(), len(txs))
		}
		t.Fatal("Timed out waiting for txs on mempools")
	case <-done:
	}
}

func TestMempoolInterfaceReactor_BroadcastTxsMessage(t *testing.T) {
	config := cfg.TestConfig()
	config.Mempool.Broadcast = true
	const n = 2

	reactors, switches, txStreams, _ := makeAndConnectMempoolInterfaceReactors(t, config, n) // mempools slice is not directly used here

	defer func() {
		for _, sw := range switches {
			if sw.IsRunning() {
				if err := sw.Stop(); err != nil {
					t.Logf("Error stopping switch: %v", err)
				}
			}
		}
	}()

	for _, r := range reactors {
		r.Switch.Peers().ForEach(func(peer p2p.Peer) {
			peer.Set(types.PeerStateKey, reactorTestPeerState{height: 1})
		})
	}

	// Use the mempool from the first reactor
	addedTxs, mempoolTxsToSend := addRandomTxsToMempoolAndStream(t, reactors[0].mempool, testNumTxsMempoolInterface, p2p.ID("xyz"))

	for _, memTx := range mempoolTxsToSend {
		// Send to the txStream associated with the first reactor
		txStreams[0].sendTx(memTx)
	}

	// Pass the slice of reactors to the wait function
	waitForTxsOnMempoolsInterface(t, addedTxs, reactors)
}

// TestMempoolInterfaceReactor_RemovePeerDuringBroadcast removes peers while
// broadcastTxRoutine fans transactions out to their queues. RemovePeer used
// to close the peer channel, which panicked when the broadcaster had already
// loaded it from the peer map.
func TestMempoolInterfaceReactor_RemovePeerDuringBroadcast(t *testing.T) {
	config := cfg.TestConfig()
	config.Mempool.Broadcast = true

	txStream := newMockTxBroadcastStream(log.TestingLogger())
	reactor := NewMempoolInterfaceReactor(config.Mempool, nil, txStream, false)
	reactor.SetLogger(log.TestingLogger())
	require.NoError(t, reactor.Start())
	t.Cleanup(func() { _ = reactor.Stop() })

	memTx := NewMempoolTxBuilder().WithHeight(1).WithTx(types.Tx("k=v")).Build()

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			case txStream.txChan <- memTx:
			}
		}
	})

	for i := 0; i < 2000; i++ {
		peer := mock.NewPeer(nil)
		reactor.peerQueues.Store(peer.ID(), newPeerTxQueue(peer, 1))
		reactor.RemovePeer(peer, nil)
	}

	close(done)
	wg.Wait()

	remaining := 0
	reactor.peerQueues.Range(func(_, _ any) bool {
		remaining++
		return true
	})
	require.Zero(t, remaining)
}

// recordingPeer is a mock peer that records the txs sent on the mempool
// channel. Its first failSends sends fail, as with a full send queue.
type recordingPeer struct {
	*mock.Peer
	id p2p.ID

	mtx       sync.Mutex
	kv        map[string]any
	failSends int
	attempts  int
	sent      types.Txs
}

func newRecordingPeer() *recordingPeer {
	p := mock.NewPeer(nil)
	return &recordingPeer{Peer: p, id: p.ID(), kv: make(map[string]any)}
}

func (p *recordingPeer) ID() p2p.ID { return p.id }

func (p *recordingPeer) Get(key string) any {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.kv[key]
}

func (p *recordingPeer) Set(key string, value any) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.kv[key] = value
}

func (p *recordingPeer) setHeight(h int64) { p.Set(types.PeerStateKey, reactorTestPeerState{height: h}) }

func (p *recordingPeer) Send(e p2p.Envelope) bool {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.attempts++
	if p.failSends > 0 {
		p.failSends--
		return false
	}
	for _, tx := range e.Message.(*protomem.Txs).Txs {
		p.sent = append(p.sent, tx)
	}
	return true
}

func (p *recordingPeer) sentTxs() types.Txs {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return append(types.Txs(nil), p.sent...)
}

func (p *recordingPeer) waitSent(t *testing.T, n int) types.Txs {
	t.Helper()
	require.Eventually(t, func() bool { return len(p.sentTxs()) >= n }, 5*time.Second, 10*time.Millisecond,
		"peer received %d txs, want %d", len(p.sentTxs()), n)
	return p.sentTxs()
}

// newTestMempoolInterfaceReactor starts a broadcasting reactor on a kvstore
// CListMempool, without a p2p network.
func newTestMempoolInterfaceReactor(
	t *testing.T,
	stream TxBroadcastStream,
	waitSync bool,
	logger log.Logger,
	opts ...func(*cfg.MempoolConfig),
) (*MempoolInterfaceReactor, *CListMempool) {
	t.Helper()
	config := cfg.TestConfig()
	config.Mempool.Broadcast = true
	for _, opt := range opts {
		opt(config.Mempool)
	}

	mp, cleanup := newMempoolInterfaceWithAppAndConfig(proxy.NewLocalClientCreator(kvstore.NewInMemoryApplication()))
	t.Cleanup(cleanup)

	reactor := NewMempoolInterfaceReactor(config.Mempool, mp, stream, waitSync)
	reactor.SetLogger(logger)
	reactor.SetSwitch(p2p.NewSwitch(config.P2P, nil))
	require.NoError(t, reactor.Start())
	t.Cleanup(func() { _ = reactor.Stop() })
	return reactor, mp
}

// addPeer adds peer to reactor and waits until it is registered for
// broadcasting, so that it gets all txs streamed afterwards.
func addPeer(t *testing.T, reactor *MempoolInterfaceReactor, peer p2p.Peer) {
	t.Helper()
	reactor.AddPeer(peer)
	require.Eventually(t, func() bool {
		q, ok := reactor.peerQueues.Load(peer.ID())
		return ok && q.(*peerTxQueue).peer == peer
	}, 5*time.Second, time.Millisecond)
}

func checkTxs(t *testing.T, mp Mempool, sender p2p.ID, txs ...types.Tx) {
	t.Helper()
	for _, tx := range txs {
		_, err := mp.CheckTx(tx, sender)
		require.NoError(t, err)
	}
}

// TestMempoolInterfaceReactor_SendsMempoolToNewPeer checks that a new peer
// gets the txs that were in the mempool before it connected, except the ones
// it sent us, followed by the streamed txs.
func TestMempoolInterfaceReactor_SendsMempoolToNewPeer(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())

	peer := newRecordingPeer()
	peer.setHeight(1)
	checkTxs(t, mp, "", types.Tx("a=1"), types.Tx("b=2"))
	checkTxs(t, mp, peer.ID(), types.Tx("c=3"))
	checkTxs(t, mp, "", types.Tx("d=4"))

	addPeer(t, reactor, peer)
	require.Equal(t, types.Txs{types.Tx("a=1"), types.Tx("b=2"), types.Tx("d=4")}, peer.waitSent(t, 3))

	checkTxs(t, mp, "", types.Tx("e=5"))
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(types.Tx("e=5")).Build())
	require.Equal(t, types.Tx("e=5"), peer.waitSent(t, 4)[3])
}

// TestMempoolInterfaceReactor_LaggingPeerKeepsTx checks that a tx read from
// the peer channel while the peer is lagging behind is sent once the peer
// catches up, before the next tx, instead of being dropped.
func TestMempoolInterfaceReactor_LaggingPeerKeepsTx(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())

	peer := newRecordingPeer()
	peer.setHeight(1)
	addPeer(t, reactor, peer)

	high := types.Tx("high=1")
	low := types.Tx("low=1")
	checkTxs(t, mp, "", high, low)
	stream.sendTx(NewMempoolTxBuilder().WithHeight(10).WithTx(high).Build())
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(low).Build())

	time.Sleep(3 * PeerCatchupSleepIntervalMS * time.Millisecond)
	require.Empty(t, peer.sentTxs(), "tx sent to a lagging peer")

	peer.setHeight(9)
	require.Equal(t, types.Txs{high, low}, peer.waitSent(t, 2))
}

// TestMempoolInterfaceReactor_RetriesFailedSend checks that a tx is retried
// when peer.Send fails, instead of being dropped.
func TestMempoolInterfaceReactor_RetriesFailedSend(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())

	peer := newRecordingPeer()
	peer.setHeight(1)
	peer.failSends = 2
	addPeer(t, reactor, peer)

	tx := types.Tx("retry=1")
	checkTxs(t, mp, "", tx)
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(tx).Build())
	require.Equal(t, types.Txs{tx}, peer.waitSent(t, 1))

	peer.mtx.Lock()
	defer peer.mtx.Unlock()
	require.Equal(t, 3, peer.attempts)
}

// TestMempoolInterfaceReactor_WaitSync checks that while the node is syncing
// the reactor neither accepts txs from peers nor sends txs to them.
func TestMempoolInterfaceReactor_WaitSync(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, true, log.TestingLogger())

	peer := newRecordingPeer()
	peer.setHeight(1)
	checkTxs(t, mp, "", types.Tx("local=1"))
	addPeer(t, reactor, peer)

	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: MempoolChannel,
		Message:   &protomem.Txs{Txs: [][]byte{types.Tx("remote=1")}},
	})
	require.Equal(t, 1, mp.Size(), "tx from peer accepted while syncing")

	time.Sleep(3 * PeerCatchupSleepIntervalMS * time.Millisecond)
	require.Empty(t, peer.sentTxs(), "tx sent while syncing")

	reactor.EnableInOutTxs()
	require.Equal(t, types.Txs{types.Tx("local=1")}, peer.waitSent(t, 1))

	reactor.Receive(p2p.Envelope{
		Src:       peer,
		ChannelID: MempoolChannel,
		Message:   &protomem.Txs{Txs: [][]byte{types.Tx("remote=1")}},
	})
	require.Equal(t, 2, mp.Size())
}

// TestMempoolInterfaceReactor_StreamClosed checks that broadcastTxRoutine
// stops when the tx stream is closed, instead of spinning on the closed
// channel.
func TestMempoolInterfaceReactor_StreamClosed(t *testing.T) {
	var buf syncBuffer
	stream := newMockTxBroadcastStream(log.TestingLogger())
	newTestMempoolInterfaceReactor(t, stream, false, log.NewTMLogger(&buf))

	close(stream.txChan)
	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "Tx broadcast stream closed")
	}, 5*time.Second, 10*time.Millisecond)
}

type syncBuffer struct {
	mtx sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.buf.String()
}

// TestMempoolInterfaceReactor_QueueOverflow checks that a peer whose queue
// overflowed while it was lagging still gets every tx in the mempool, instead
// of losing the txs that didn't fit in the queue.
func TestMempoolInterfaceReactor_QueueOverflow(t *testing.T) {
	const queueSize = 3
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger(),
		func(c *cfg.MempoolConfig) { c.Size = queueSize })
	reactor.SetLatestHeightFunc(func() int64 { return 10 })

	peer := newRecordingPeer()
	peer.setHeight(1)
	addPeer(t, reactor, peer)

	// The peer is too far behind for the streamed txs, so the peer routine
	// blocks on the first one while the queue overflows. As with the proxy
	// mempool, the stream only carries txs added to the mempool.
	var txs types.Txs
	for i := 0; i < 4*queueSize+1; i++ {
		tx := types.Tx(fmt.Sprintf("tx%d=%d", i, i))
		txs = append(txs, tx)
		checkTxs(t, mp, "", tx)
		stream.sendTx(NewMempoolTxBuilder().WithHeight(10).WithTx(tx).Build())
	}
	// Let broadcastTxRoutine fan all streamed txs out to the full queue.
	require.Eventually(t, func() bool { return len(stream.txChan) == 0 }, 5*time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.Empty(t, peer.sentTxs())

	peer.setHeight(9)
	require.Eventually(t, func() bool { return len(peer.sentTxs()) >= len(txs) }, 5*time.Second, 10*time.Millisecond,
		"txs were lost")
	time.Sleep(3 * PeerCatchupSleepIntervalMS * time.Millisecond)

	// The tx the routine was blocked on is sent from the queue and again on
	// resync; the others are sent once.
	sent := peer.sentTxs()
	require.LessOrEqual(t, len(sent), len(txs)+1)
	unique := make(map[string]struct{})
	for _, tx := range sent {
		unique[string(tx)] = struct{}{}
	}
	require.Len(t, unique, len(txs))
	for _, tx := range txs {
		require.Contains(t, unique, string(tx))
	}
}

// TestMempoolInterfaceReactor_ReconnectReplacesStaleQueue checks that a peer
// reconnecting while the queue of its previous connection is still stored
// replaces it and gets txs. A stale entry used to keep the new peer from
// getting a broadcast routine.
func TestMempoolInterfaceReactor_ReconnectReplacesStaleQueue(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())

	// The AddPeer routine of the old connection stored its queue after
	// RemovePeer ran, and hasn't exited yet.
	oldPeer := newRecordingPeer()
	require.NoError(t, oldPeer.Stop())
	reactor.peerQueues.Store(oldPeer.ID(), newPeerTxQueue(oldPeer, 10))

	newPeer := newRecordingPeer()
	newPeer.id = oldPeer.ID()
	newPeer.setHeight(1)
	addPeer(t, reactor, newPeer)

	// A late RemovePeer for the old connection keeps the new queue.
	reactor.RemovePeer(oldPeer, nil)

	tx := types.Tx("reconnect=1")
	checkTxs(t, mp, "", tx)
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(tx).Build())
	require.Equal(t, types.Txs{tx}, newPeer.waitSent(t, 1))
}

// TestMempoolInterfaceReactor_MempoolTxsWaitForLaggingPeer checks that the
// txs already in the mempool when a peer connects get the latest height, so
// they aren't sent before the peer has caught up. They used to get height 0
// and were sent to a peer that was still syncing.
func TestMempoolInterfaceReactor_MempoolTxsWaitForLaggingPeer(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())
	reactor.SetLatestHeightFunc(func() int64 { return 10 })

	tx := types.Tx("existing=1")
	checkTxs(t, mp, "", tx)

	peer := newRecordingPeer()
	peer.setHeight(1)
	addPeer(t, reactor, peer)

	time.Sleep(3 * PeerCatchupSleepIntervalMS * time.Millisecond)
	require.Empty(t, peer.sentTxs(), "tx sent to a lagging peer")

	peer.setHeight(9)
	require.Equal(t, types.Txs{tx}, peer.waitSent(t, 1))
}

// TestMempoolInterfaceReactor_ReaddedTxIsSentAgain checks that a tx that left
// the mempool and was added again is broadcast again. Txs used to be
// deduplicated by key for the lifetime of the connection.
func TestMempoolInterfaceReactor_ReaddedTxIsSentAgain(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())

	tx := types.Tx("readded=1")
	checkTxs(t, mp, "", tx)

	peer := newRecordingPeer()
	peer.setHeight(1)
	addPeer(t, reactor, peer)
	require.Equal(t, types.Txs{tx}, peer.waitSent(t, 1))

	require.NoError(t, mp.RemoveTxByKey(tx.Key()))
	mp.forceRemoveFromCache(tx)
	checkTxs(t, mp, "", tx)
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(tx).Build())
	require.Equal(t, types.Txs{tx, tx}, peer.waitSent(t, 2))
}

// TestMempoolInterfaceReactor_SkipsTxRemovedWhileWaiting checks that a tx
// removed from the mempool while the peer routine waits for the peer to catch
// up is not sent, and doesn't hold back the next tx.
func TestMempoolInterfaceReactor_SkipsTxRemovedWhileWaiting(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())

	peer := newRecordingPeer()
	peer.setHeight(1)
	addPeer(t, reactor, peer)

	removed := types.Tx("removed=1")
	kept := types.Tx("kept=1")
	checkTxs(t, mp, "", removed, kept)
	stream.sendTx(NewMempoolTxBuilder().WithHeight(10).WithTx(removed).Build())
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(kept).Build())

	time.Sleep(3 * PeerCatchupSleepIntervalMS * time.Millisecond)
	require.Empty(t, peer.sentTxs())

	require.NoError(t, mp.RemoveTxByKey(removed.Key()))
	require.Equal(t, types.Txs{kept}, peer.waitSent(t, 1))
	time.Sleep(3 * PeerCatchupSleepIntervalMS * time.Millisecond)
	require.Equal(t, types.Txs{kept}, peer.sentTxs())
}

// TestMempoolInterfaceReactor_SkipsTxRemovedBeforeSend checks that a queued tx
// that left the mempool before the peer routine got to it is not sent, even
// to a peer that is caught up.
func TestMempoolInterfaceReactor_SkipsTxRemovedBeforeSend(t *testing.T) {
	stream := newMockTxBroadcastStream(log.TestingLogger())
	reactor, mp := newTestMempoolInterfaceReactor(t, stream, false, log.TestingLogger())

	peer := newRecordingPeer()
	peer.setHeight(1)
	addPeer(t, reactor, peer)

	removed := types.Tx("removed=1")
	kept := types.Tx("kept=1")
	checkTxs(t, mp, "", removed, kept)
	require.NoError(t, mp.RemoveTxByKey(removed.Key()))
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(removed).Build())
	stream.sendTx(NewMempoolTxBuilder().WithHeight(1).WithTx(kept).Build())

	require.Equal(t, types.Txs{kept}, peer.waitSent(t, 1))
	time.Sleep(3 * PeerCatchupSleepIntervalMS * time.Millisecond)
	require.Equal(t, types.Txs{kept}, peer.sentTxs())
}
