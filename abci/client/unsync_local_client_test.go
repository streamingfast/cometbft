package abcicli_test //nolint:revive // same test package name as the other client tests

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abcicli "github.com/cometbft/cometbft/abci/client"
	"github.com/cometbft/cometbft/abci/types"
)

// gatedCheckTxApp blocks CheckTx for a tx until its gate is opened, so tests
// control the order in which application calls complete.
type gatedCheckTxApp struct {
	types.BaseApplication

	mtx     sync.Mutex
	gates   map[string]chan struct{}
	started chan string
	err     error
	fail    func() // called before returning err
}

func newGatedCheckTxApp() *gatedCheckTxApp {
	return &gatedCheckTxApp{
		gates:   make(map[string]chan struct{}),
		started: make(chan string, 100),
	}
}

func (app *gatedCheckTxApp) gate(tx string) chan struct{} {
	app.mtx.Lock()
	defer app.mtx.Unlock()
	g, ok := app.gates[tx]
	if !ok {
		g = make(chan struct{})
		app.gates[tx] = g
	}
	return g
}

func (app *gatedCheckTxApp) open(tx string) { close(app.gate(tx)) }

func (app *gatedCheckTxApp) CheckTx(_ context.Context, req *types.CheckTxRequest) (*types.CheckTxResponse, error) {
	app.started <- string(req.Tx)
	<-app.gate(string(req.Tx))
	if app.err != nil {
		if app.fail != nil {
			app.fail()
		}
		return nil, app.err
	}
	return &types.CheckTxResponse{Code: types.CodeTypeOK, Data: req.Tx}, nil
}

func newStartedUnsyncClient(t *testing.T, app types.Application) abcicli.Client {
	t.Helper()
	c := abcicli.NewUnsyncLocalClient(app)
	require.NoError(t, c.Start())
	t.Cleanup(func() { _ = c.Stop() })
	return c
}

func waitStarted(t *testing.T, app *gatedCheckTxApp, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-app.started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d CheckTx calls started", i, n)
		}
	}
}

// TestUnsyncLocalClientCheckTxAsyncRunsInParallel checks that CheckTxAsync
// returns before the application call completes and that application calls
// run concurrently.
func TestUnsyncLocalClientCheckTxAsyncRunsInParallel(t *testing.T) {
	app := newGatedCheckTxApp()
	c := newStartedUnsyncClient(t, app)

	txs := []string{"a", "b", "c"}
	reqResps := make([]*abcicli.ReqRes, len(txs))
	for i, tx := range txs {
		rr, err := c.CheckTxAsync(context.Background(), &types.CheckTxRequest{Tx: []byte(tx)})
		require.NoError(t, err)
		reqResps[i] = rr
	}
	// All three calls are in the application at the same time.
	waitStarted(t, app, len(txs))

	for _, tx := range txs {
		app.open(tx)
	}
	for i, rr := range reqResps {
		rr.Wait()
		assert.Equal(t, []byte(txs[i]), rr.Response.GetCheckTx().Data)
	}
}

// TestUnsyncLocalClientCallbacksInRequestOrder completes application calls in
// reverse order and checks that callbacks are still delivered one at a time
// and in request order.
func TestUnsyncLocalClientCallbacksInRequestOrder(t *testing.T) {
	app := newGatedCheckTxApp()
	c := newStartedUnsyncClient(t, app)

	var (
		mtx      sync.Mutex
		order    []string
		inFlight atomic.Int32
	)
	record := func(tx string) {
		assert.Equal(t, int32(1), inFlight.Add(1), "callbacks delivered concurrently")
		time.Sleep(time.Millisecond) // widen the window for concurrent delivery
		mtx.Lock()
		order = append(order, tx)
		mtx.Unlock()
		inFlight.Add(-1)
	}

	var globalOrder []string
	c.SetResponseCallback(func(req *types.Request, _ *types.Response) {
		globalOrder = append(globalOrder, string(req.GetCheckTx().Tx))
	})

	txs := []string{"1", "2", "3", "4", "5"}
	for _, tx := range txs {
		rr, err := c.CheckTxAsync(context.Background(), &types.CheckTxRequest{Tx: []byte(tx)})
		require.NoError(t, err)
		rr.SetCallback(func(res *types.Response) error {
			record(string(res.GetCheckTx().Data))
			return nil
		})
	}
	waitStarted(t, app, len(txs))

	for i := len(txs) - 1; i >= 0; i-- {
		app.open(txs[i])
		time.Sleep(5 * time.Millisecond) // let the later request finish first
	}
	require.NoError(t, c.Flush(context.Background()))

	assert.Equal(t, txs, order)
	assert.Equal(t, txs, globalOrder)
}

// TestUnsyncLocalClientFlushWaitsForCallbacks checks that Flush returns only
// after the in-flight application calls and their callbacks have completed.
func TestUnsyncLocalClientFlushWaitsForCallbacks(t *testing.T) {
	app := newGatedCheckTxApp()
	c := newStartedUnsyncClient(t, app)

	var called atomic.Bool
	rr, err := c.CheckTxAsync(context.Background(), &types.CheckTxRequest{Tx: []byte("slow")})
	require.NoError(t, err)
	rr.SetCallback(func(*types.Response) error {
		called.Store(true)
		return nil
	})
	waitStarted(t, app, 1)

	flushed := make(chan error, 1)
	go func() { flushed <- c.Flush(context.Background()) }()

	select {
	case <-flushed:
		t.Fatal("Flush returned while CheckTx was still running")
	case <-time.After(50 * time.Millisecond):
	}

	app.open("slow")
	select {
	case err := <-flushed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not return after CheckTx completed")
	}
	assert.True(t, called.Load(), "Flush returned before the callback was delivered")

	// Flush returns early when the context is done.
	_, err = c.CheckTxAsync(context.Background(), &types.CheckTxRequest{Tx: []byte("stuck")})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.Flush(ctx), context.DeadlineExceeded)
	app.open("stuck")
	require.NoError(t, c.Flush(context.Background()))
}

// TestUnsyncLocalClientCheckTxAsyncAppError checks that an application error
// or panic is reported as a rejected tx and that waiters are released. A panic
// in the CheckTxAsync goroutine used to crash the process.
func TestUnsyncLocalClientCheckTxAsyncAppError(t *testing.T) {
	for name, fail := range map[string]func(){
		"error": func() {},
		"panic": func() { panic("boom") },
	} {
		t.Run(name, func(t *testing.T) {
			app := newGatedCheckTxApp()
			app.err = errors.New("boom")
			app.fail = fail
			c := newStartedUnsyncClient(t, app)

			app.open("x")
			rr, err := c.CheckTxAsync(context.Background(), &types.CheckTxRequest{Tx: []byte("x")})
			require.NoError(t, err)
			rr.Wait()
			res := rr.Response.GetCheckTx()
			require.NotNil(t, res)
			assert.Equal(t, abcicli.CodeTypeCheckTxFailed, res.Code)
			assert.Equal(t, abcicli.CodespaceCheckTxFailed, res.Codespace)
			assert.Contains(t, res.Log, "boom")
		})
	}
}

// TestUnsyncLocalClientCallbackErrorVisibleAfterWait checks that the callback
// result is available once Wait returns, as BroadcastTxSync relies on.
func TestUnsyncLocalClientCallbackErrorVisibleAfterWait(t *testing.T) {
	app := newGatedCheckTxApp()
	c := newStartedUnsyncClient(t, app)

	cbErr := errors.New("rejected")
	rr, err := c.CheckTxAsync(context.Background(), &types.CheckTxRequest{Tx: []byte("x")})
	require.NoError(t, err)
	rr.SetCallback(func(*types.Response) error { return cbErr })
	app.open("x")
	rr.Wait()
	require.ErrorIs(t, rr.Error(), cbErr)
}

// TestUnsyncLocalClientErrorDuringCallback reads the callback error while the
// callback runs on another goroutine, as BroadcastTxAsync does right after
// CheckTx returns. Run with -race: reads of ReqRes.Error used to race with the
// callback writing it.
func TestUnsyncLocalClientErrorDuringCallback(t *testing.T) {
	cbErr := errors.New("rejected")
	for _, setEarly := range []bool{true, false} {
		app := newGatedCheckTxApp()
		c := newStartedUnsyncClient(t, app)

		rr, err := c.CheckTxAsync(context.Background(), &types.CheckTxRequest{Tx: []byte("x")})
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			defer close(done)
			for rr.Error() == nil {
				runtime.Gosched()
			}
		}()

		cb := func(*types.Response) error { return cbErr }
		if setEarly {
			rr.SetCallback(cb)
			app.open("x")
		} else {
			app.open("x")
			require.NoError(t, c.Flush(context.Background()))
			rr.SetCallback(cb)
		}

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("callback error never became visible")
		}
		require.ErrorIs(t, rr.Error(), cbErr)
	}
}
