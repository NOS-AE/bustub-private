//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  max_depth_ = max_depth;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & (Size() - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "invalid bucket_idx");
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(bucket_idx < Size(), "invalid bucket_idx");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  throw NotImplementedException("GetSplitImageIndex is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(global_depth_ < max_depth_, "目录页已为最大深度");
  global_depth_++;
  auto msb = Size() >> 1;
  for (uint32_t i = 0; i < msb; i++) {
    local_depths_[i+msb] = local_depths_[i];
    bucket_page_ids_[i+msb] = bucket_page_ids_[i];
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(global_depth_ > 0, "目录页已为最小深度");
  global_depth_--;

}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  auto size = Size();
  for (uint32_t i = 0; i < size; i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return 1 << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "invalid bucket_idx");
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(bucket_idx < Size(), "invalid bucket_idx");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(bucket_idx < Size(), "invalid bucket_idx");
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  BUSTUB_ASSERT(bucket_idx < Size(), "invalid bucket_idx");
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
