package gearmin

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestNewPacket(t *testing.T) {
	t.Parallel()

	t.Run("Contructs a known packet", func(t *testing.T) {
		t.Parallel()

		pt, err := newPacket(uint32(1))
		assert.NilError(t, err)
		assert.Equal(t, pt.String(), "CAN_DO")
	})

	t.Run("Rejects an unknown packet", func(t *testing.T) {
		t.Parallel()

		pt, err := newPacket(uint32(43))
		assert.Error(t, err, "invalid packet type 43")
		assert.Assert(t, pt == 43)
	})
}
