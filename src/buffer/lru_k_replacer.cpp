//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include "common/exception.h"

namespace bustub {

void LRUKNode::Access(size_t timestamp) {
  history_.emplace_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
  BUSTUB_ASSERT(history_.size() <= k_, "history_.size() > k");
}

auto LRUKNode::SetEvictable(bool evictable) -> bool {
  bool ok = evictable != is_evictable_;
  is_evictable_ = evictable;
  return ok;
}

auto LRUKNode::operator<(const LRUKNode &node) const -> bool {
  if (history_.size() < k_) {
    if (node.history_.size() < k_) {
      return history_.back() < node.history_.back();
    } else {
      return true;
    }
  } else {
    if (node.history_.size() < k_) {
      return false;
    } else {
      return history_.front() < node.history_.front();
    }
  }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  const LRUKNode *victim = nullptr;
  for (const auto &p : node_store_) {
    if (!p.second.is_evictable_) {
      continue;
    }
    if (victim == nullptr || p.second < *victim) {
      victim = &p.second;
    }
  }
  if (victim == nullptr) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }

  *frame_id = victim->fid_;
  node_store_.erase(victim->fid_);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id,
                                [[maybe_unused]] AccessType access_type) {
  // try_emplace相当于"emplace if exist"
  auto t = node_store_.try_emplace(frame_id, frame_id, k_);
  auto &node = t.first->second;
  node.Access(current_timestamp_);
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 如果frame_id不存在，at函数会抛异常
  auto &node = node_store_.at(frame_id);
  if (node.SetEvictable(set_evictable)) {
    curr_size_ += set_evictable ? 1 : -1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (!node.is_evictable_) {
    throw ExecutionException("try to remove a inevictable frame");
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
