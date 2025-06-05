#pragma once

#include "OptLatch.hpp"
// -------------------------------------------------------------------------------------
// BTREE NODES
// -------------------------------------------------------------------------------------
// Methods could be
// - lowerBound
// - split
// - insert
// for both node types, i.e., inner and leaf
// The Nodes are internal to your B-Tree logic, therefore feel free to change them
// the given methods here are just a blueprint and can be changed as you see fit
// -------------------------------------------------------------------------------------

using Key = uint64_t;
using Payload = uint64_t;

enum class NodeType : uint8_t { BTreeInner=1, BTreeLeaf=2 };
static constexpr uint64_t pageSize=4*1024; // DO NOT CHANGE 4KB size nodes 

struct NodeBase : public OptLatch{
   NodeType type;
   uint16_t count;
};

struct BTreeLeafBase : public NodeBase {
   static const NodeType typeMarker=NodeType::BTreeLeaf;
};

struct BTreeInnerBase : public NodeBase {
   static const NodeType typeMarker=NodeType::BTreeInner;
};

struct BTreeLeaf : public BTreeLeafBase {
   // -------------------------------------------------------------------------------------
   struct Entry {
      Key k;
      Payload p;
   };
   // -------------------------------------------------------------------------------------
   static constexpr uint64_t maxEntries=(pageSize-sizeof(NodeBase))/(sizeof(Key)+sizeof(Payload));
   Key keys[maxEntries];
   Payload payloads[maxEntries];
   // -------------------------------------------------------------------------------------
   BTreeLeaf() {
      count=0;
      type=typeMarker;
   }
   // -------------------------------------------------------------------------------------
   bool isFull() { return count==maxEntries; };
   unsigned lowerBound(Key k); // Implement
   void insert(Key k,Payload p); // Implement
   BTreeLeaf* split(Key& sep); // Implement  
};

// -------------------------------------------------------------------------------------
struct BTreeInner : public BTreeInnerBase {
   static const uint64_t maxEntries=(pageSize-sizeof(NodeBase))/(sizeof(Key)+sizeof(NodeBase*));
   NodeBase* children[maxEntries];
   Key keys[maxEntries];
   // -------------------------------------------------------------------------------------
   BTreeInner() {
      count=0;
      type=typeMarker;
   }
   // -------------------------------------------------------------------------------------
   bool isFull() { return count==(maxEntries-1); };
   unsigned lowerBound(Key k); // implement 
   BTreeInner* split(Key& sep); // implement
   void insert(Key k,NodeBase* child); // implement

};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// BTREE 
// -------------------------------------------------------------------------------------
// Implement upsert and lookup, do not change the function signature as we test against this
// interface.
// You do not need to store duplicate keys, we just update them in the upsert method.
// All inputs key, and values, are uint64_t.

class OLC_BTree {
  private:
   std::atomic<NodeBase*> root;
   std::atomic<uint64_t> height;
   // feel free to add variables      
   void makeRoot(Key k,NodeBase* leftChild,NodeBase* rightChild); // implement
   
   public:
   OLC_BTree() {
      root = new BTreeLeaf();
   }
   uint64_t getHeight(){return height;}
   void upsert(Key k, Payload v); // implement upsert, i.e. insert or update if key exists
   bool lookup(Key k, Payload& result); // implement lookup
   
};

