#include "OLC_BTree.hpp"

// -------------------------------------------------------------------------------------
// BTREE NODES
// -------------------------------------------------------------------------------------
unsigned BTreeLeaf::lowerBound(Key k){}
void BTreeLeaf::insert(Key k,Payload p){}
BTreeLeaf* BTreeLeaf::split(Key& sep){}
// -------------------------------------------------------------------------------------
unsigned BTreeInner::lowerBound(Key k){} 
BTreeInner* BTreeInner::split(Key& sep){}
void BTreeInner::insert(Key k,NodeBase* child){}
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
// BTREE 
// -------------------------------------------------------------------------------------
void OLC_BTree::upsert(Key k, Payload v){}
bool OLC_BTree::lookup(Key k, Payload& result){return false;}
void OLC_BTree::makeRoot(Key k,NodeBase* leftChild,NodeBase* rightChild){}
