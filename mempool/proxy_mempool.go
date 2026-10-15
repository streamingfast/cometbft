package mempool

// ProxyMempool is a wrapper around a Mempool and a TxBroadcastStream.
// It allows setting the underlying Mempool and TxBroadcastStream dynamically.
type ProxyMempool struct {
	Mempool
	TxBroadcastStream

	// txsAvailableEnabled records that EnableTxsAvailable was called, so that
	// it also applies to a Mempool set afterwards. The node enables it before
	// the node options inject the custom mempool.
	txsAvailableEnabled bool
}

var (
	_ Mempool           = (*ProxyMempool)(nil)
	_ TxBroadcastStream = (*ProxyMempool)(nil)
)

// SetMempool sets the underlying Mempool. If EnableTxsAvailable was called
// before, it is called on mp too.
func (m *ProxyMempool) SetMempool(mp Mempool) {
	m.Mempool = mp
	if m.txsAvailableEnabled && mp != nil {
		mp.EnableTxsAvailable()
	}
}

func (m *ProxyMempool) SetTxBroadcastStream(stream TxBroadcastStream) {
	m.TxBroadcastStream = stream
}

// EnableTxsAvailable implements Mempool. It can be called before the
// underlying Mempool is set.
func (m *ProxyMempool) EnableTxsAvailable() {
	m.txsAvailableEnabled = true
	if m.Mempool != nil {
		m.Mempool.EnableTxsAvailable()
	}
}
