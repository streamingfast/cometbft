package node

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/libs/log"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
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
