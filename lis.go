package lightning

func longestIncreasingSubsequence(srclen int, X func(int) int) []int {
	if srclen == 0 {
		return nil
	}
	M := make([]int, srclen+1) // M[j] == index into X such that X[M[j]] is the smallest X[k] where k<=i and an increasing subsequence with length j ends at X[k]
	P := make([]int, srclen)   // P[k] == index in X of predecessor of X[k] in longest increasing subsequence ending at X[k]
	L := 0                     // length of longest increasing subsequence found so far
	for i := range P {
		lo, hi := 1, L
		for lo <= hi {
			mid := (lo + hi + 1) / 2
			if X(M[mid]) < X(i) {
				lo = mid + 1
			} else {
				hi = mid - 1
			}
		}
		newL := lo
		if i > 0 {
			P[i] = M[newL-1]
		}
		M[newL] = i
		if newL > L {
			L = newL
		}
	}
	ret := make([]int, L)
	for k, i := M[L], len(ret)-1; i >= 0; k, i = P[k], i-1 {
		ret[i] = k
	}
	return ret
}
