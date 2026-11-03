package armor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArmor(t *testing.T) {
	blockType := "MINT TEST"
	data := []byte("somedata")
	armorStr, err := EncodeArmor(blockType, nil, data)
	require.NoError(t, err, "%+v", err)

	// Decode armorStr and test for equivalence.
	blockType2, _, data2, err := DecodeArmor(armorStr)
	require.NoError(t, err, "%+v", err)
	assert.Equal(t, blockType, blockType2)
	assert.Equal(t, data, data2)
}

// DecodeArmor verifies the optional CRC-24 checksum line like the previous
// golang.org/x/crypto/openpgp/armor decoder did.
func TestDecodeArmorChecksum(t *testing.T) {
	data := []byte("some key bytes that get armored")
	headers := map[string]string{"kdf": "bcrypt", "type": "secp256k1"}
	armored, err := EncodeArmor("TENDERMINT PRIVATE KEY", headers, data)
	require.NoError(t, err)

	lines := strings.Split(armored, "\n")
	checksumIdx := -1
	for i, line := range lines {
		if len(line) == 5 && line[0] == '=' {
			checksumIdx = i
		}
	}
	require.NotEqual(t, -1, checksumIdx, "EncodeArmor must write a checksum line")

	// Valid checksum.
	blockType, gotHeaders, got, err := DecodeArmor(armored)
	require.NoError(t, err)
	require.Equal(t, "TENDERMINT PRIVATE KEY", blockType)
	require.Equal(t, headers, gotHeaders)
	require.Equal(t, data, got)

	// The checksum line is optional.
	withoutChecksum := strings.Join(append(append([]string{}, lines[:checksumIdx]...), lines[checksumIdx+1:]...), "\n")
	_, _, got, err = DecodeArmor(withoutChecksum)
	require.NoError(t, err)
	require.Equal(t, data, got)

	// A checksum that doesn't match the body is rejected.
	corrupted := append([]string{}, lines...)
	corrupted[checksumIdx] = "=AAAA"
	_, _, _, err = DecodeArmor(strings.Join(corrupted, "\n"))
	require.EqualError(t, err, "openpgp: invalid data: armor invalid")
}
