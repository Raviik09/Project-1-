#include <thread>
#include "catch.hpp"  
#include "OLC_BTree.hpp"

#include <iostream>
///// ----------------------- BASIC TEST CASES ----------------------- ///// 

TEST_CASE("TEST OLC BTREE UPSERT AND LOOKUPS", "[ll-upsert-lookups]")
{

   OLC_BTree tree;
   for(uint64_t k = 0; k < 10e6; k++){
      tree.upsert(k,k);
   }

   for(uint64_t k = 0; k < 10e6; k++){
      uint64_t result = 0;
      REQUIRE(tree.lookup(k,result));
      REQUIRE(result == k);
   }
}



TEST_CASE("TEST OLC BTREE UPSERT AND LOOKUPS REVERSE", "[ll-upsert-lookups-reverse]")
{
   OLC_BTree tree;
   for(uint64_t k = 10e6; k > 0; k--){
      tree.upsert(k,k);
   }

   for(uint64_t k = 1; k <= 10e6; k++){
      uint64_t result = 0;
      REQUIRE(tree.lookup(k,result));
      REQUIRE(result == k);
   }
}





TEST_CASE("TEST OLC BTREE UPSERT AND LOOKUPS AND UPDATE", "[ll-upsert-lookups]")
{
   OLC_BTree tree;
   for(uint64_t k = 0; k < 10e6; k++){
      tree.upsert(k,k);
   }
   // update
   for(uint64_t k = 0; k < 10e6; k++){
      tree.upsert(k,k+1);
   }

   for(uint64_t k = 0; k < 10e6; k++){
      uint64_t result = 0;
      REQUIRE(tree.lookup(k,result));
      REQUIRE(result == (k+1));
   }
}
