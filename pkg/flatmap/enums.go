package flatmap

type NodeEnum int

const (
	NodeUndecided NodeEnum = iota
	NodeNonLeaf
	NodeLeaf
)
