#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (this == &that) {
    return;
  }
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard ret(bpm_, page_);

  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;

  return ret;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard ret(bpm_, page_);

  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;

  return ret;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
