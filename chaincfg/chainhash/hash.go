package chainhash


// HashSize of array used to store hashes.
const HashSize = 32


// Hash is used in several of the bitcoin messages and common structures.
// It typically represents the double sha256 of data
type Hash [HashSize]byte
