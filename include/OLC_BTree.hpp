#pragma once

#include "OptLatch.hpp"
#include <atomic>
#include <mutex>

// -------------------------------------------------------------------------------------
// BTREE NODES
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

// -------------------------------------------------------------------------------------
struct BTreeLeaf : public BTreeLeafBase {
  
   static constexpr uint64_t ENTRIES_CAPACITY = (pageSize - sizeof(NodeBase)) / (sizeof(Key) + sizeof(Payload));
   
   Key keys[ENTRIES_CAPACITY + 1]; 
   Payload payloads[ENTRIES_CAPACITY + 1]; 

   BTreeLeaf() {
      type = typeMarker;
      count = 0;
   }

   bool isFull() { return count == ENTRIES_CAPACITY; }
   unsigned lowerBound(Key k);
   void insert(Key k, Payload p);
   BTreeLeaf* split(Key& sep);
};
// -------------------------------------------------------------------------------------
struct BTreeInner : public BTreeInnerBase {
     static constexpr uint64_t KEYS_CAPACITY = (pageSize - sizeof(NodeBase) - sizeof(NodeBase*)) / (sizeof(Key) + sizeof(NodeBase*));

   Key keys[KEYS_CAPACITY + 1]; 
   NodeBase* children[KEYS_CAPACITY + 1 + 1]; 

   BTreeInner() {
      type = typeMarker;
      count = 0;
   }

   bool isFull() { return count == KEYS_CAPACITY; }
   unsigned lowerBound(Key k); 
   unsigned getChildIdxForTraversal(Key k) const; 
   BTreeInner* split(Key& sep);
   void insert(Key k, NodeBase* child);
};
// -------------------------------------------------------------------------------------
// BTREE 
// -------------------------------------------------------------------------------------
class OLC_BTree {
public:
   std::atomic<NodeBase*> root;
   std::atomic<uint64_t> height;
   std::mutex treeMutex; 

public:
   OLC_BTree() {
      root = new BTreeLeaf();
      height = 1;
   }

   ~OLC_BTree() {
     
   }

   bool lookup(Key k, Payload& result);
   void upsert(Key k, Payload v);
   bool makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild);
};
