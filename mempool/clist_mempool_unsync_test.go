package mempool

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

// reverseRecheckApp accepts every new tx and rejects every rechecked tx. A
// recheck of the i-th tx is delayed so that later txs complete first.
type reverseRecheckApp struct {
	abci.BaseApplication
	numTxs int
}

func (app *reverseRecheckApp) CheckTx(_ context.Context, req *abci.CheckTxRequest) (*abci.CheckTxResponse, error) {
	if req.Type != abci.CHECK_TX_TYPE_RECHECK {
		return &abci.CheckTxResponse{Code: abci.CodeTypeOK, GasWanted: 1}, nil
	}
	i, err := strconv.Atoi(string(req.Tx))
	if err != nil {
		return nil, err
	}
	time.Sleep(time.Duration(app.numTxs-i) * 2 * time.Millisecond)
	return &abci.CheckTxResponse{Code: 1}, nil
}

// TestCListMempoolRecheckWithUnsyncClient rechecks txs through the unsync
// local client while the application completes the rechecks in reverse order.
// The responses must still be processed in mempool order: an out-of-order
// response used to advance the recheck cursor past earlier txs, which then
// stayed in the mempool even though the application rejected them.
func TestCListMempoolRecheckWithUnsyncClient(t *testing.T) {
	const numTxs = 20
	conf := test.ResetTestRoot("mempool_unsync_test")
	conf.Mempool.RecheckTimeout = 10 * time.Second
	mp, cleanup := newMempoolWithAppAndConfig(proxy.NewUnsyncLocalClientCreator(&reverseRecheckApp{numTxs: numTxs}), conf)
	t.Cleanup(cleanup)

	txs := make(types.Txs, numTxs)
	for i := range txs {
		txs[i] = types.Tx(strconv.Itoa(i))
		rr, err := mp.CheckTx(txs[i], "")
		require.NoError(t, err)
		rr.Wait()
		require.NoError(t, rr.Error())
	}
	require.Equal(t, numTxs, mp.Size())

	// Commit an unrelated tx to trigger a recheck of all txs in the mempool.
	doUpdate(t, mp, 1, types.Txs{types.Tx("committed")})

	require.True(t, mp.recheck.done())
	require.Zero(t, mp.Size(), "txs rejected on recheck are still in the mempool")
}

// panickingApp panics in CheckTx.
type panickingApp struct {
	abci.BaseApplication
}

func (*panickingApp) CheckTx(context.Context, *abci.CheckTxRequest) (*abci.CheckTxResponse, error) {
	panic("malicious tx")
}

// TestCListMempoolCheckTxPanicWithUnsyncClient checks that a CheckTx panic in
// the application rejects the tx instead of crashing the node: with the
// unsync client, CheckTx runs on a goroutine that nothing else recovers.
func TestCListMempoolCheckTxPanicWithUnsyncClient(t *testing.T) {
	conf := test.ResetTestRoot("mempool_unsync_panic_test")
	mp, cleanup := newMempoolWithAppAndConfig(proxy.NewUnsyncLocalClientCreator(&panickingApp{}), conf)
	t.Cleanup(cleanup)

	rr, err := mp.CheckTx(types.Tx("tx"), "")
	require.NoError(t, err)
	rr.Wait()
	require.ErrorAs(t, rr.Error(), &ErrInvalidTx{})
	require.Zero(t, mp.Size())
}
