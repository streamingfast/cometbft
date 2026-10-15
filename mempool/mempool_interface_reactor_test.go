package mempool

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
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
		res := reqres.Response.GetCheckTx()

		if res.IsErr() {
			t.Logf("CheckTx callback failed for tx %X: %s, code: %d, log: %s, info: %s", tx, res.Log, res.Code, res.Log, res.Info)
		}
		require.NoError(t, err, "mempool.CheckTx returned an error for tx %X. Error: %v", tx, err)
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
// broadcastTxRoutine fans transactions out to their channels. RemovePeer used
// to close the peer channel, which panicked when the broadcaster had already
// loaded it from peerBroadcastChannels.
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
		reactor.peerBroadcastChannels.Store(peer.ID(), make(chan MempoolTx, 1))
		reactor.RemovePeer(peer, nil)
	}

	close(done)
	wg.Wait()

	remaining := 0
	reactor.peerBroadcastChannels.Range(func(_, _ any) bool {
		remaining++
		return true
	})
	require.Zero(t, remaining)
}
