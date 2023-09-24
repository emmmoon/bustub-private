#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr || bpm_ == nullptr) {
    return;
  }
  if (PageId() != INVALID_PAGE_ID) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  page_ = nullptr;
  bpm_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();

    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    this->is_dirty_ = that.is_dirty_;

    that.page_ = nullptr;
    that.bpm_ = nullptr;
  }

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    if (this->guard_.page_ != nullptr) {
      this->guard_.page_->RUnlatch();
      this->guard_.Drop();
    }

    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
  }
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if (this->guard_.page_ != nullptr) {
      this->guard_.page_->WUnlatch();
      this->guard_.Drop();
    }

    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
  }
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
