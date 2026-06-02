package commands

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/privval"
)

func Test_ResetAll(t *testing.T) {
	config := cfg.TestConfig()
	dir := t.TempDir()
	config.SetRoot(dir)
	cfg.EnsureRoot(dir)
	require.NoError(t, initFilesWithConfig(config))
	pv := privval.LoadFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile())
	pv.LastSignState.Height = 10
	pv.Save()
	require.NoError(t, resetAll(config.DBDir(), config.P2P.AddrBookFile(), config.PrivValidatorKeyFile(),
		config.PrivValidatorStateFile(), logger))
	require.DirExists(t, config.DBDir())
	require.NoFileExists(t, filepath.Join(config.DBDir(), "block.db"))
	require.NoFileExists(t, filepath.Join(config.DBDir(), "state.db"))
	require.NoFileExists(t, filepath.Join(config.DBDir(), "evidence.db"))
	require.NoFileExists(t, filepath.Join(config.DBDir(), "tx_index.db"))
	require.FileExists(t, config.PrivValidatorStateFile())
	pv = privval.LoadFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile())
	require.Equal(t, int64(0), pv.LastSignState.Height)
}

func Test_ResetState(t *testing.T) {
	config := cfg.TestConfig()
	dir := t.TempDir()
	config.SetRoot(dir)
	cfg.EnsureRoot(dir)
	require.NoError(t, initFilesWithConfig(config))
	pv := privval.LoadFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile())
	pv.LastSignState.Height = 10
	pv.Save()
	require.NoError(t, resetState(config.DBDir(), logger))
	require.DirExists(t, config.DBDir())
	require.NoFileExists(t, filepath.Join(config.DBDir(), "block.db"))
	require.NoFileExists(t, filepath.Join(config.DBDir(), "state.db"))
	require.NoFileExists(t, filepath.Join(config.DBDir(), "evidence.db"))
	require.NoFileExists(t, filepath.Join(config.DBDir(), "tx_index.db"))
	require.FileExists(t, config.PrivValidatorStateFile())
	pv = privval.LoadFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile())
	// private validator state should still be in tact.
	require.Equal(t, int64(10), pv.LastSignState.Height)
}

func TestRollbackFilePVState(t *testing.T) {
	config := cfg.TestConfig()
	dir := t.TempDir()
	config.SetRoot(dir)
	cfg.EnsureRoot(dir)
	require.NoError(t, initFilesWithConfig(config))

	pv := privval.LoadFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile())
	pv.LastSignState.Height = 21
	pv.LastSignState.Round = 2
	pv.LastSignState.Step = 3
	pv.LastSignState.Signature = []byte{1, 2, 3}
	pv.LastSignState.SignBytes = []byte{4, 5, 6}
	pv.Save()

	require.NoError(t, rollbackFilePVState(config, 16))

	pv = privval.LoadFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile())
	require.Equal(t, int64(16), pv.LastSignState.Height)
	require.Equal(t, int32(0), pv.LastSignState.Round)
	require.Equal(t, int8(0), pv.LastSignState.Step)
	require.Nil(t, pv.LastSignState.Signature)
	require.Nil(t, pv.LastSignState.SignBytes)
}
