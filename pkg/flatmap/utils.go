package flatmap

// estimateBufferSize estimates appropriate buffer size for a given number of items
func estimateBufferSize(numItems int) int {
	// On average, we expect each item to take about 128 bytes, but use at least 1KB
	// This is a heuristic that can be tuned based on actual usage patterns
	return max(1024, numItems*128)
}
