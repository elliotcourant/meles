package buffers

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestBytesBuffer_Append(t *testing.T) {
	buf := NewAllocatedBytesBuffer(4)
	buf.AppendUint16(2)
	buf.Append('t', 's')
	fmt.Println(hex.Dump(buf.Bytes()))
}

func BenchmarkBuffer_AppendUint16(b *testing.B) {
	b.Run("allocated 2 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(2)
			buf.AppendUint16(2)
		}
	})

	b.Run("allocated 10 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(10)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
		}
	})

	b.Run("allocated 100 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(100)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
		}
	})

	b.Run("allocated 1000 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(1000)
			for x := 0; x < 100; x++ {
				// 10 Bytes
				buf.AppendUint16(2)
				buf.AppendUint16(2)
				buf.AppendUint16(2)
				buf.AppendUint16(2)
				buf.AppendUint16(2)
			}
		}
	})

	b.Run("un-allocated 2 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			buf.AppendUint16(2)
		}
	})

	b.Run("un-allocated 10 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
		}
	})

	b.Run("un-allocated 100 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			// 10 Bytes
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
			buf.AppendUint16(2)
		}
	})

	b.Run("un-allocated 1000 bytes uint16", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			for x := 0; x < 100; x++ {
				// 10 Bytes
				buf.AppendUint16(2)
				buf.AppendUint16(2)
				buf.AppendUint16(2)
				buf.AppendUint16(2)
				buf.AppendUint16(2)
			}
		}
	})
}

func BenchmarkBuffer_Append(b *testing.B) {
	bigChunk := make([]byte, 1000)
	b.Run("allocated 2 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(2)
			buf.Append(2, 2)
		}
	})

	b.Run("allocated 10 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(10)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
		}
	})

	b.Run("allocated 100 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(100)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
		}
	})

	b.Run("allocated 1000 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(1000)
			for x := 0; x < 100; x++ {
				// 10 Bytes
				buf.Append(2, 2)
				buf.Append(2, 2)
				buf.Append(2, 2)
				buf.Append(2, 2)
				buf.Append(2, 2)
			}
		}
	})

	b.Run("allocated 10000 10 byte chunk", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(10000)
			for x := 0; x < 100; x++ {
				buf.Append(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
			}
		}
	})

	b.Run("allocated 1000 1000 byte chunk", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(1000)
			buf.Append(bigChunk...)
		}
	})

	b.Run("allocated 10000 1000 byte chunk", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewAllocatedBytesBuffer(10000)
			for x := 0; x < 10; x++ {
				buf.Append(bigChunk...)
			}
		}
	})

	b.Run("un-allocated 2 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			buf.Append(2, 2)
		}
	})

	b.Run("un-allocated 10 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
		}
	})

	b.Run("un-allocated 100 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			// 10 Bytes
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
			buf.Append(2, 2)
		}
	})

	b.Run("un-allocated 1000 bytes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			for x := 0; x < 100; x++ {
				// 10 Bytes
				buf.Append(2, 2)
				buf.Append(2, 2)
				buf.Append(2, 2)
				buf.Append(2, 2)
				buf.Append(2, 2)
			}
		}
	})

	b.Run("un-allocated 10000 10 byte chunk", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			for x := 0; x < 100; x++ {
				buf.Append(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
			}
		}
	})

	b.Run("un-allocated 1000 1000 byte chunk", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			buf.Append(bigChunk...)
		}
	})

	b.Run("un-allocated 10000 1000 byte chunk", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := NewBytesBuffer()
			for x := 0; x < 10; x++ {
				buf.Append(bigChunk...)
			}
		}
	})
}
