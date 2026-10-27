package armor

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/ProtonMail/go-crypto/openpgp/armor"
)

// ErrEncode represents an error from calling [EncodeArmor].
type ErrEncode struct {
	Err error
}

func (e ErrEncode) Error() string {
	return fmt.Sprintf("armor: could not encode ASCII armor: %v", e.Err)
}

func (e ErrEncode) Unwrap() error {
	return e.Err
}

func EncodeArmor(blockType string, headers map[string]string, data []byte) (string, error) {
	buf := new(bytes.Buffer)
	w, err := armor.Encode(buf, blockType, headers)
	if err != nil {
		return "", ErrEncode{Err: err}
	}
	_, err = w.Write(data)
	if err != nil {
		return "", ErrEncode{Err: err}
	}
	err = w.Close()
	if err != nil {
		return "", ErrEncode{Err: err}
	}
	return buf.String(), nil
}

func DecodeArmor(armorStr string) (blockType string, headers map[string]string, data []byte, err error) {
	buf := bytes.NewBufferString(armorStr)
	block, err := armor.Decode(buf)
	if err != nil {
		return "", nil, nil, err
	}
	data, err = io.ReadAll(block.Body)
	if err != nil {
		return "", nil, nil, err
	}
	// github.com/ProtonMail/go-crypto no longer verifies the optional CRC-24
	// checksum (RFC 9580). Verify it like golang.org/x/crypto/openpgp did, so
	// corrupted armor is still rejected.
	if want, ok := armorChecksum(armorStr); ok && want != crc24(data) {
		return "", nil, nil, armor.ArmorCorrupt
	}
	return block.Type, block.Header, data, nil
}

// armorChecksum returns the CRC-24 from the armor's checksum line: a
// five-character "=XXXX" line directly followed by the "-----END" line. The
// checksum is optional; ok is false when there is none.
func armorChecksum(armorStr string) (crc uint32, ok bool) {
	lines := strings.Split(armorStr, "\n")
	for i := 0; i+1 < len(lines); i++ {
		line := strings.TrimSuffix(lines[i], "\r")
		if len(line) != 5 || line[0] != '=' || !strings.HasPrefix(lines[i+1], "-----END") {
			continue
		}
		var sum [3]byte
		if n, err := base64.StdEncoding.Decode(sum[:], []byte(line[1:])); n != 3 || err != nil {
			return 0, false
		}
		return uint32(sum[0])<<16 | uint32(sum[1])<<8 | uint32(sum[2]), true
	}
	return 0, false
}

// crc24 computes the OpenPGP checksum as specified in RFC 4880, section 6.1.
func crc24(data []byte) uint32 {
	const (
		crc24Init = 0xb704ce
		crc24Poly = 0x1864cfb
		crc24Mask = 0xffffff
	)
	crc := uint32(crc24Init)
	for _, b := range data {
		crc ^= uint32(b) << 16
		for i := 0; i < 8; i++ {
			crc <<= 1
			if crc&0x1000000 != 0 {
				crc ^= crc24Poly
			}
		}
	}
	return crc & crc24Mask
}
