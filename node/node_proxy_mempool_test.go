package node

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	"github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/libs/log"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

// txsAvailableMempool records whether EnableTxsAvailable was called.
type txsAvailableMempool struct {
	mempl.NopMempool
	enabled bool
}

func (m *txsAvailableMempool) EnableTxsAvailable() { m.enabled = true }

type chanTxBroadcastStream chan mempl.MempoolTx

func (s chanTxBroadcastStream) GetTxChannel() <-chan mempl.MempoolTx { return s }

func newProxyMempoolNode(t *testing.T, config *cfg.Config, options ...Option) (*Node, error) {
	t.Helper()
	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)
	pv, err := privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile(), nil)
	require.NoError(t, err)
	return NewNode(
		context.Background(),
		config,
		pv,
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		DefaultGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
		nil,
		options...,
	)
}

func proxyMempoolTestConfig(t *testing.T, name string) *cfg.Config {
	t.Helper()
	config := withFreeAddrs(t, test.ResetTestRoot(name))
	t.Cleanup(func() { os.RemoveAll(config.RootDir) })
	config.Mempool.Type = cfg.MempoolTypeProxy
	config.Mempool.DOGProtocolEnabled = false
	require.NoError(t, config.Mempool.ValidateBasic())
	return config
}

// TestNodeNewNodeProxyMempool creates a node with the proxy mempool and
// create_empty_blocks = false. Enabling TxsAvailable used to panic because the
// custom mempool was injected only after the node had enabled it.
func TestNodeNewNodeProxyMempool(t *testing.T) {
	config := proxyMempoolTestConfig(t, "node_new_node_proxy_mempool_test")
	config.Consensus.CreateEmptyBlocks = false
	require.True(t, config.Consensus.WaitForTxs())

	customMempool := &txsAvailableMempool{}
	n, err := newProxyMempoolNode(t, config,
		WithCustomMempoolOnProxyMempool(customMempool, make(chanTxBroadcastStream)))
	require.NoError(t, err)

	assert.True(t, customMempool.enabled, "EnableTxsAvailable was not forwarded to the custom mempool")
	assert.IsType(t, &mempl.MempoolInterfaceReactor{}, n.Switch().Reactor("MEMPOOL"))

	// The proxy mempool reactor registers only the mempool channel, so the
	// node must not advertise the DOG control channel.
	channels := n.NodeInfo().(p2p.DefaultNodeInfo).Channels
	assert.Contains(t, channels, mempl.MempoolChannel)
	assert.NotContains(t, channels, mempl.MempoolControlChannel)

	require.NoError(t, n.Start())
	require.NoError(t, n.Stop())
}

// TestNodeNewNodeProxyMempoolNotInjected checks that a proxy mempool node
// fails to build, instead of panicking later, when no custom mempool or
// broadcast stream is injected.
func TestNodeNewNodeProxyMempoolNotInjected(t *testing.T) {
	config := proxyMempoolTestConfig(t, "node_new_node_proxy_mempool_not_injected_test")
	config.Consensus.CreateEmptyBlocks = false

	_, err := newProxyMempoolNode(t, config)
	require.ErrorContains(t, err, "no custom mempool")

	config.Mempool.Broadcast = true
	_, err = newProxyMempoolNode(t, config, WithCustomMempoolOnProxyMempool(&txsAvailableMempool{}, nil))
	require.ErrorContains(t, err, "no tx broadcast stream")
}

// TestProxyMempoolEnableTxsAvailable checks that EnableTxsAvailable reaches
// the underlying mempool whether it is set before or after the call.
func TestProxyMempoolEnableTxsAvailable(t *testing.T) {
	before := &txsAvailableMempool{}
	pm := &mempl.ProxyMempool{}
	pm.SetMempool(before)
	pm.EnableTxsAvailable()
	assert.True(t, before.enabled)

	after := &txsAvailableMempool{}
	pm = &mempl.ProxyMempool{}
	pm.EnableTxsAvailable()
	pm.SetMempool(after)
	assert.True(t, after.enabled)

	notEnabled := &txsAvailableMempool{}
	pm = &mempl.ProxyMempool{}
	pm.SetMempool(notEnabled)
	assert.False(t, notEnabled.enabled)
}

// stateSyncedApp is a kvstore that reports a state at the given height, as an
// application restored from a state sync snapshot does.
type stateSyncedApp struct {
	*kvstore.Application
	height  int64
	appHash []byte
}

func (app *stateSyncedApp) Info(ctx context.Context, req *abci.InfoRequest) (*abci.InfoResponse, error) {
	res, err := app.Application.Info(ctx, req)
	if err != nil {
		return nil, err
	}
	res.LastBlockHeight = app.height
	res.LastBlockAppHash = app.appHash
	return res, nil
}

// TestNodeProxyMempoolLatestHeightAfterStateSync bootstraps the stores as
// offline state sync does, without any block, and checks that the proxy
// mempool reactor uses the synced height. It used to take the block store
// height, which stays 0 until a block is saved, so the txs it read from the
// mempool were sent to peers regardless of their height.
func TestNodeProxyMempoolLatestHeightAfterStateSync(t *testing.T) {
	const height = 5
	config := proxyMempoolTestConfig(t, "node_proxy_mempool_latest_height_test")
	config.DBBackend = "goleveldb" // the stores are reopened by NewNode

	pv, err := privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile(), nil)
	require.NoError(t, err)

	// Bootstrap the stores at height, like BootstrapState.
	blockStoreDB, stateDB, err := initDBs(config, cfg.DefaultDBProvider)
	require.NoError(t, err)
	genState, genDoc, err := LoadStateFromDBOrGenesisDocProvider(stateDB, DefaultGenesisDocProviderFunc(config), "")
	require.NoError(t, err)

	state := genState.Copy()
	state.LastBlockHeight = height
	state.LastBlockID = types.BlockID{
		Hash:          tmhash.Sum([]byte("block")),
		PartSetHeader: types.PartSetHeader{Total: 1, Hash: tmhash.Sum([]byte("parts"))},
	}
	state.LastBlockTime = cmttime.Now()
	state.LastValidators = state.Validators.Copy()
	state.AppHash = tmhash.Sum([]byte("app"))
	commit, err := test.MakeCommit(state.LastBlockID, height, 0, state.LastValidators,
		[]types.PrivValidator{pv}, genDoc.ChainID, state.LastBlockTime)
	require.NoError(t, err)

	stateStore := sm.NewStore(stateDB, sm.StoreOptions{DBKeyLayout: config.Storage.ExperimentalKeyLayout})
	require.NoError(t, stateStore.Bootstrap(state))
	blockStore := store.NewBlockStore(blockStoreDB, store.WithDBKeyLayout(config.Storage.ExperimentalKeyLayout))
	require.NoError(t, blockStore.SaveSeenCommit(height, commit))
	require.NoError(t, stateStore.SetOfflineStateSyncHeight(height))
	require.NoError(t, blockStore.Close())
	require.NoError(t, stateStore.Close())

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)
	app := &stateSyncedApp{Application: kvstore.NewInMemoryApplication(), height: height, appHash: state.AppHash}
	n, err := NewNode(
		context.Background(),
		config,
		pv,
		nodeKey,
		proxy.NewLocalClientCreator(app),
		DefaultGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
		nil,
		WithCustomMempoolOnProxyMempool(&txsAvailableMempool{}, make(chanTxBroadcastStream)),
	)
	require.NoError(t, err)

	require.Zero(t, n.BlockStore().Height())
	reactor, ok := n.Switch().Reactor("MEMPOOL").(*mempl.MempoolInterfaceReactor)
	require.True(t, ok)
	require.EqualValues(t, height, reactor.LatestHeight())
}
