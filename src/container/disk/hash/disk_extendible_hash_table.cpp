//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  auto guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto page = guard.template AsMut<ExtendibleHTableHeaderPage>();
  page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  std::shared_lock lock(rwlatch_);

  // 1. 得到key的哈希
  uint32_t hash = Hash(key);
  // 2. 获取header
  auto header_page = bpm_->FetchPageRead(header_page_id_);
  auto header = header_page.template As<ExtendibleHTableHeaderPage>();
  // 3.获取directory
  auto dir_idx = header->HashToDirectoryIndex(hash);
  auto dir_page_id = header->GetDirectoryPageId(dir_idx);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_page.Drop();
  auto dir_page = bpm_->FetchPageRead(dir_page_id);
  auto dir = dir_page.template As<ExtendibleHTableDirectoryPage>();
  // 3. 获取bucket
  auto bucket_idx = dir->HashToBucketIndex(hash);
  auto bucket_page_id = dir->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  dir_page.Drop();
  auto bucket_page = bpm_->FetchPageRead(bucket_page_id);
  const auto *bucket = bucket_page.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  // 4. 查找bucket
  V value;
  if (!bucket->Lookup(key, value, cmp_)) {
    return false;
  }
  result->push_back(std::move(value));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::unique_lock lock(rwlatch_);

  // 1. 得到key的哈希
  uint32_t hash = Hash(key);
  // 2. 获取header
  auto header_page = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_page.template AsMut<ExtendibleHTableHeaderPage>();  // 0x108406500
  // 3.获取directory
  auto dir_idx = header->HashToDirectoryIndex(hash);
  auto dir_page_id = header->GetDirectoryPageId(dir_idx);
  WritePageGuard dir_page;
  bool is_new_dir = false;
  if (dir_page_id == INVALID_PAGE_ID) {
    dir_page = bpm_->NewPageGuarded(&dir_page_id).UpgradeWrite();
    header->SetDirectoryPageId(dir_idx, dir_page_id);
    is_new_dir = true;
  } else {
    dir_page = bpm_->FetchPageWrite(dir_page_id);
  }
  header_page.Drop();
  auto dir = dir_page.template AsMut<ExtendibleHTableDirectoryPage>();  // 0x108405100
  if (is_new_dir) {
    dir->Init(directory_max_depth_);
  }

  // 4. 插入directory
  return InsertToDirectory(dir, hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToDirectory(ExtendibleHTableDirectoryPage *dir, uint32_t hash,
                                                          const K &key, const V &value) -> bool {
  // 1. 获取bucket
  auto bucket_idx = dir->HashToBucketIndex(hash);
  auto bucket_page_id = dir->GetBucketPageId(bucket_idx);
  WritePageGuard bucket_page;
  bool is_new_bucket = false;
  if (bucket_page_id == INVALID_PAGE_ID) {
    BUSTUB_ASSERT(dir->Size() == 1, "directory大小一定为1");
    BUSTUB_ASSERT(bucket_idx == 0, "bucket在directory中下标一定为1");
    bucket_page = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
    dir->SetBucketPageId(bucket_idx, bucket_page_id);
    is_new_bucket = true;
  } else {
    bucket_page = bpm_->FetchPageWrite(bucket_page_id);
  }
  auto bucket = bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (is_new_bucket) {
    bucket->Init(bucket_max_size_);
  }
  // 2. 尝试插入
  if (!bucket->IsFull()) {
    return bucket->Insert(key, value, cmp_);
  }
  // 3. 可能扩展directory
  if (dir->GetLocalDepth(bucket_idx) == dir->GetGlobalDepth()) {
    if (!dir->CanExpand()) {
      return false;
    }
    dir->IncrGlobalDepth();
  }
  // 4. 分裂bucket
  page_id_t new_bucket_page_id;
  auto new_bucket_page = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  auto new_bucket = new_bucket_page.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket->Init(bucket_max_size_);
  auto new_mask = 1 << dir->GetLocalDepth(bucket_idx);
  for (int64_t i = bucket->Size() - 1; i >= 0; i--) {
    std::pair<K, V> e = bucket->EntryAt(i);
    uint32_t h = Hash(e.first);
    if ((h & new_mask) != 0) {
      new_bucket->Insert(e.first, e.second, cmp_);
      bucket->RemoveAt(static_cast<uint32_t>(i));
    }
  }
  // 5. 更新directory
  auto dir_size = dir->Size();
  for (uint32_t idx = hash & dir->GetLocalDepthMask(bucket_idx), step = new_mask; idx < dir_size; idx += step) {
    dir->IncrLocalDepth(idx);
    if ((idx & step) != 0) {  // 1xxx指向new_bucket，0xxx指向原来的bucket
      dir->SetBucketPageId(idx, new_bucket_page_id);
    }
  }
  bucket_page.Drop();
  new_bucket_page.Drop();
  // 6. 再次插入
  return InsertToDirectory(dir, hash, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  std::unique_lock lock(rwlatch_);

  // 1. 得到key的哈希
  uint32_t hash = Hash(key);
  // 2. 获取header
  auto header_page = bpm_->FetchPageRead(header_page_id_);
  auto header = header_page.template As<ExtendibleHTableHeaderPage>();  // 0x108406500
  // 3.获取directory
  auto dir_idx = header->HashToDirectoryIndex(hash);
  auto dir_page_id = header->GetDirectoryPageId(dir_idx);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_page.Drop();
  auto dir_page = bpm_->FetchPageWrite(dir_page_id);
  auto dir = dir_page.template AsMut<ExtendibleHTableDirectoryPage>();
  // 3. 获取bucket
  auto bucket_idx = dir->HashToBucketIndex(hash);
  auto bucket_page_id = dir->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_page = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_page.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // 4. 删除元素
  if (!bucket->Remove(key, cmp_)) {
    return false;
  }
  // 5. 合并bucket
  while (true) {
    auto bucket_depth = dir->GetLocalDepth(bucket_idx);
    if (bucket_depth == 0) {
      break;
    }
    auto merge_mask = 1 << (bucket_depth - 1);
    auto merge_bucket_idx = bucket_idx ^ merge_mask;
    auto merge_bucket_page_id = dir->GetBucketPageId(merge_bucket_idx);
    auto merge_bucket_page = bpm_->FetchPageRead(merge_bucket_page_id);
    const auto *merge_bucket = merge_bucket_page.template As<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket->MergeBucket(*merge_bucket, cmp_)) {
      break;
    }
    merge_bucket_page.Drop();
    BUSTUB_ASSERT(bpm_->DeletePage(merge_bucket_page_id), "删除已合并页面失败");
    // 2. 更新directory
    auto dir_size = dir->Size();
    for (uint32_t idx = hash & dir->GetLocalDepthMask(bucket_idx), step = merge_mask; idx < dir_size; idx += step) {
      dir->DecrLocalDepth(idx);
      dir->SetBucketPageId(idx, bucket_page_id);
    }
  }

  // 6. 收缩direcotry
  while (dir->CanShrink()) {
    dir->DecrGlobalDepth();
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
