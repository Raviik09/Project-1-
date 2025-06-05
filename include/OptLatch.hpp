#pragma once

#include <atomic>
struct OptLatch {
   std::atomic<uint64_t> latchVersion{0b100};

   bool isLocked(uint64_t version) {
      return ((version & 0b10) == 0b10);
   }

   uint64_t readLockOrRestart(bool &needRestart) {
      uint64_t version;
      version = latchVersion.load();
      if (isLocked(version) || isObsolete(version)) {
         needRestart = true;
      }
      return version;
   }

   void writeLockOrRestart(bool &needRestart) {
      uint64_t version;
      version = readLockOrRestart(needRestart);
      if (needRestart) return;

      upgradeToWriteLockOrRestart(version, needRestart);
      if (needRestart) return;
   }

   void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
      if (latchVersion.compare_exchange_strong(version, version + 0b10)) {
         version = version + 0b10;
      } else {
         needRestart = true;
      }
   }

   void writeUnlock() {
      latchVersion.fetch_add(0b10);
   }

   bool isObsolete(uint64_t version) {
      return (version & 1) == 1;
   }

   void checkOrRestart(uint64_t startRead, bool &needRestart) const {
      readUnlockOrRestart(startRead, needRestart);
   }

   void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
      needRestart = (startRead != latchVersion.load());
   }
};
