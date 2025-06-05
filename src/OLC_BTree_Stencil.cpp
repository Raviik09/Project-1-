#include "OLC_BTree.hpp"
#include <algorithm> 
#include <cstring>   
#include <vector>
#include <thread>    
#include <stdexcept> 
namespace {
  
  inline unsigned first_ge(const Key* a, unsigned n, Key k) {
     unsigned lo = 0, hi = n;
     while (lo < hi) {
        unsigned mid = lo + (hi - lo) / 2; 
        if (a[mid] < k)
           lo = mid + 1;
        else
           hi = mid;
     }
     return lo;
  }
} 

// -------------------------------------------------------------------------------------
// BTreeLeaf Methods
// -------------------------------------------------------------------------------------
unsigned BTreeLeaf::lowerBound(Key k) {
   return first_ge(keys, count, k);
}

void BTreeLeaf::insert(Key k, Payload p) {
   unsigned pos = lowerBound(k);
 
   if (pos < count && keys[pos] == k) {
      payloads[pos] = p; 
      return;
   }
   std::copy_backward(keys + pos, keys + count, keys + count + 1);
   std::copy_backward(payloads + pos, payloads + count, payloads + count + 1);
   keys[pos] = k;
   payloads[pos] = p;
   ++count;
}

BTreeLeaf* BTreeLeaf::split(Key &sep) {
   auto* newLeaf = new BTreeLeaf();
   unsigned mid = count / 2; 
   unsigned numToMove = count - mid;
   
   std::memcpy(newLeaf->keys, keys + mid, numToMove * sizeof(Key));
   std::memcpy(newLeaf->payloads, payloads + mid, numToMove * sizeof(Payload));
   newLeaf->count = numToMove;
   
   count = mid;
   sep = newLeaf->keys[0]; 
   return newLeaf;
}

// -------------------------------------------------------------------------------------
// BTreeInner Methods
// -------------------------------------------------------------------------------------
unsigned BTreeInner::lowerBound(Key k) { 
   return first_ge(keys, count, k);
}


unsigned BTreeInner::getChildIdxForTraversal(Key k) const {
   unsigned lo = 0, hi = count; 
   while (lo < hi) {
      unsigned mid = lo + (hi - lo) / 2;
      if (k < keys[mid]) {
         hi = mid;
      } else {
         lo = mid + 1;
      }
   }
   return lo; 
}

void BTreeInner::insert(Key k, NodeBase* child) {
   unsigned pos = lowerBound(k); 
   
   std::copy_backward(keys + pos, keys + count, keys + count + 1);
   std::copy_backward(children + pos + 1, children + count + 1, children + count + 2); 
   
   keys[pos] = k;
   children[pos + 1] = child; 
   ++count;
}

BTreeInner* BTreeInner::split(Key &sep) {
   auto* newInner = new BTreeInner();
   unsigned mid = count / 2; 
   sep = keys[mid];  
   
   unsigned numKeysToMoveToRight = count - (mid + 1);
   if (numKeysToMoveToRight > 0) { 
        std::memcpy(newInner->keys, keys + mid + 1, numKeysToMoveToRight * sizeof(Key));
   }
   std::memcpy(newInner->children, children + mid + 1, (numKeysToMoveToRight + 1) * sizeof(NodeBase*));
   newInner->count = numKeysToMoveToRight;
   
   count = mid; 
   return newInner;
}

// -------------------------------------------------------------------------------------
// OLC_BTree Methods
// -------------------------------------------------------------------------------------
bool OLC_BTree::lookup(Key k, Payload &result) {
   
   result = 0;
   
   unsigned restart_counter = 0;
   try {
      while (true) {
         if (restart_counter > 0 && restart_counter % 1000 == 0) { 
             std::this_thread::yield();
         }
         restart_counter++;

         bool needRestart = false;
         NodeBase* curr = root.load(std::memory_order_acquire);
         if (!curr) {
   
            return false;
         }
         
         uint64_t version = curr->readLockOrRestart(needRestart);
         if (needRestart) {
            continue;
         }

         while (curr->type == NodeType::BTreeInner) {
            auto* inner = static_cast<BTreeInner*>(curr);
            unsigned child_idx = inner->getChildIdxForTraversal(k); 
            
            
            if (child_idx > inner->count) {
               needRestart = true;
               break;
            }
            
            NodeBase* child_to_descend = inner->children[child_idx];
            
            
            if (!child_to_descend) {
               needRestart = true;
               break;
            }

            inner->checkOrRestart(version, needRestart); 
            if (needRestart) {
               break; 
            }
            
            curr = child_to_descend;
            version = curr->readLockOrRestart(needRestart); 
            if (needRestart) {
               break; 
            }
         }

         if (needRestart) {
            continue; 
         }

         auto* leaf = static_cast<BTreeLeaf*>(curr);
         unsigned pos = leaf->lowerBound(k); 
         bool found = (pos < leaf->count && leaf->keys[pos] == k);
         Payload temp_payload = 0;
         if (found) {
            temp_payload = leaf->payloads[pos];
         }

         leaf->checkOrRestart(version, needRestart); 
         if (needRestart) {
            continue; 
         }

         if (found) {
            result = temp_payload;
         }
         return found;
      }
   } catch (const std::exception& e) {
      result = 0;
      return false;
   }
}

void OLC_BTree::upsert(Key k, Payload v) {
   unsigned restart_counter = 0;
   try {
      while(true) {
         if (restart_counter > 0 && restart_counter % 1000 == 0) { 
             std::this_thread::yield();
         }
         restart_counter++;

         bool needRestart = false;
         std::vector<BTreeInner*> parents_nodes; 
         std::vector<uint64_t> parents_versions; 

         NodeBase* curr = root.load(std::memory_order_acquire);
         if (!curr) {
            
            std::lock_guard<std::mutex> lock(treeMutex);
            curr = root.load(std::memory_order_acquire);
            if (!curr) {
               root.store(new BTreeLeaf(), std::memory_order_release);
               curr = root.load(std::memory_order_acquire);
            }
         }
         
         uint64_t version = curr->readLockOrRestart(needRestart);
         if(needRestart) {
            continue;
         }

         while (curr->type == NodeType::BTreeInner) {
            auto* inner = static_cast<BTreeInner*>(curr);
            parents_nodes.push_back(inner);
            parents_versions.push_back(version);

            unsigned child_idx = inner->getChildIdxForTraversal(k);
            
            
            if (child_idx > inner->count) {
               needRestart = true;
               break;
            }
            
            NodeBase* child_to_descend = inner->children[child_idx];
            
           
            if (!child_to_descend) {
               needRestart = true;
               break;
            }

            curr->checkOrRestart(version, needRestart); 
            if(needRestart) break;

            curr = child_to_descend;
            version = curr->readLockOrRestart(needRestart); 
            if(needRestart) break;
         }

         if (needRestart) {
            continue;
         }

         auto* leaf = static_cast<BTreeLeaf*>(curr);
         leaf->upgradeToWriteLockOrRestart(version, needRestart);
         if (needRestart) {
            continue; 
         }

         if(!leaf->isFull()){
            leaf->insert(k, v);
            leaf->writeUnlock(); 
            return; 
         }
         
         Key sep_key_from_split;
         BTreeLeaf* new_sibling_leaf = leaf->split(sep_key_from_split);
         
         if (k >= sep_key_from_split) {
            new_sibling_leaf->insert(k, v);
         } else {
            leaf->insert(k, v);
         }
         leaf->writeUnlock(); 

         Key key_to_promote = sep_key_from_split;
         NodeBase* right_child_of_promoted_key = new_sibling_leaf;

         bool bubble_up_restarted = false;
         while (!parents_nodes.empty()) {
            BTreeInner* parent_node = parents_nodes.back();
            uint64_t parent_version_before_lock = parents_versions.back();
            parents_nodes.pop_back();
            parents_versions.pop_back();

            parent_node->upgradeToWriteLockOrRestart(parent_version_before_lock, needRestart);
            if (needRestart) {
               bubble_up_restarted = true;
             
               NodeBase* node_to_delete = right_child_of_promoted_key;
          
               if (node_to_delete && node_to_delete != root.load(std::memory_order_acquire)) {
                  delete node_to_delete;
               }
               break; 
            }

            if (!parent_node->isFull()) {
               parent_node->insert(key_to_promote, right_child_of_promoted_key);
               parent_node->writeUnlock();
               goto finish_upsert_success; 
            }
            
            Key key_promoted_from_parent_split;
            BTreeInner* new_sibling_inner = parent_node->split(key_promoted_from_parent_split);
            
            if (key_to_promote >= key_promoted_from_parent_split) {
               new_sibling_inner->insert(key_to_promote, right_child_of_promoted_key);
            } else {
               parent_node->insert(key_to_promote, right_child_of_promoted_key);
            }
            parent_node->writeUnlock(); 

            key_to_promote = key_promoted_from_parent_split;
            right_child_of_promoted_key = new_sibling_inner;
         }

         if (bubble_up_restarted) {
            continue; 
         }

         { 
            std::lock_guard<std::mutex> lock(treeMutex); 
            NodeBase* old_root = root.load(std::memory_order_acquire);
            
            
            if (!old_root || !right_child_of_promoted_key) {
               if (right_child_of_promoted_key) {
                  delete right_child_of_promoted_key;
               }
               continue;
            }
            
            if (!makeRoot(key_to_promote, old_root, right_child_of_promoted_key)) {
               
               if (right_child_of_promoted_key && right_child_of_promoted_key != old_root) {
                  delete right_child_of_promoted_key;
               }
               continue; 
            }
         }

      finish_upsert_success:
         return; 
      }
   } catch (const std::exception& e) {
      
      return;
   }
}

bool OLC_BTree::makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild) {
   if (!leftChild || !rightChild) {
      return false;
   }
   
   try {
      auto* newRootNode = new BTreeInner();
      newRootNode->keys[0] = k;
      newRootNode->children[0] = leftChild;
      newRootNode->children[1] = rightChild;
      newRootNode->count = 1; 
      
      NodeBase* expected = leftChild;
      if (root.compare_exchange_strong(expected, newRootNode, 
          std::memory_order_release, 
          std::memory_order_acquire)) {
          height.fetch_add(1, std::memory_order_relaxed);
          return true;
      } else {
         delete newRootNode;
         return false;
      }
   } catch (const std::exception& e) {
      return false;
   }
}
