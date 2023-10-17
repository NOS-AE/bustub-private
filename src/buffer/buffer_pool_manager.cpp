//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock lock(latch_);

  // 1. 选择可用的frame
  frame_id_t frame_id;
  bool is_free = false;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    is_free = true;
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  Page &frame = pages_[frame_id];
  if (!is_free) {
    page_table_.erase(frame.page_id_);
  }

  // 2. 写回frame
  if (frame.is_dirty_) {
    char wbuf[BUSTUB_PAGE_SIZE]{};
    memcpy(wbuf, frame.data_, BUSTUB_PAGE_SIZE);
    DiskRequest req = DiskRequest();
    req.is_write_ = true;
    req.data_ = wbuf;
    req.page_id_ = frame.page_id_;
    auto future = req.callback_.get_future();
    disk_scheduler_->Schedule(std::move(req));
    future.wait();
  }

  // 3. 设置metadata
  frame.is_dirty_ = false;
  frame.pin_count_ = 1;
  frame.page_id_ = AllocatePage();

  // 4. 映射page到frame，并pin frame
  page_table_[frame.page_id_] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 返回结果
  *page_id = frame.page_id_;
  return &frame;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock lock(latch_);
  frame_id_t frame_id = INVALID_PAGE_ID;
  // 1. 如果page还在，则继续使用
  if (page_table_.count(page_id) > 0) {
    frame_id = page_table_[page_id];
    Page &page = pages_[frame_id];
    page.pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &page;
  }

  // 2. 选择可用的frame
  bool is_free = false;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    is_free = true;
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  Page &frame = pages_[frame_id];
  if (!is_free) {
    page_table_.erase(frame.page_id_);
  }

  // 3. 写回frame
  std::future<bool> wfuture;
  if (frame.is_dirty_) {
    char wbuf[BUSTUB_PAGE_SIZE]{};
    memcpy(wbuf, frame.data_, BUSTUB_PAGE_SIZE);
    DiskRequest wreq;
    wreq.is_write_ = true;
    wreq.data_ = wbuf;
    wreq.page_id_ = frame.page_id_;
    wfuture = wreq.callback_.get_future();
    disk_scheduler_->Schedule(std::move(wreq));
    wfuture.wait();
  }
  // std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // 4. 读入page
  DiskRequest rreq;
  rreq.data_ = frame.data_;
  rreq.is_write_ = false;
  rreq.page_id_ = page_id;
  auto rfuture = rreq.callback_.get_future();
  disk_scheduler_->Schedule(std::move(rreq));

  // if (frame.is_dirty_) wfuture.wait();
  rfuture.wait();

  // 4. 设置metadata
  frame.is_dirty_ = false;
  frame.pin_count_ = 1;
  frame.page_id_ = page_id;

  // 5. 映射page到frame，并pin frame
  page_table_[frame.page_id_] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &frame;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock lock(latch_);

  // 1. 查找frame
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &frame = pages_[frame_id];

  // 2. 检查pin count
  if (frame.pin_count_ == 0) {
    return false;
  }

  // 3. unpin
  frame.pin_count_--;
  frame.is_dirty_ |= is_dirty;
  if (frame.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock lock(latch_);

  // 1. 查找frame
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &frame = pages_[frame_id];

  // 2. flush REGARDLESS of the dirty flag
  char wbuf[BUSTUB_PAGE_SIZE]{};
  memcpy(wbuf, frame.data_, BUSTUB_PAGE_SIZE);
  DiskRequest wreq;
  wreq.is_write_ = true;
  wreq.page_id_ = frame.page_id_;
  wreq.data_ = wbuf;
  auto future = wreq.callback_.get_future();
  disk_scheduler_->Schedule(std::move(wreq));
  future.get();

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (const auto &p : page_table_) {
    FlushPage(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock lock(latch_);

  // 1. 查找frame
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page &frame = pages_[frame_id];

  // 2. 如果不是pinned则删除（TODO:是否需要写回）
  if (frame.pin_count_ > 0) {
    return false;
  }
  frame.is_dirty_ = false;
  frame.page_id_ = INVALID_PAGE_ID;
  frame.pin_count_ = 0;
  frame.ResetMemory();
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return { this, page };
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  return { this, page };
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  return { this, page };
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return { this, page };
}
}  // namespace bustub
