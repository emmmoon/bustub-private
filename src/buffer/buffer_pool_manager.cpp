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
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "recovery/log_manager.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  pages_latch_ = new std::mutex[pool_size];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete[] pages_latch_;
}

auto BufferPoolManager::InitNewPage(frame_id_t frame_id, page_id_t page_id) -> void {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  ++pages_[frame_id].pin_count_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();

  if (!free_list_.empty()) {
    *page_id = AllocatePage();
    auto frame_id = free_list_.front();
    free_list_.pop_front();
    replacer_->RecordAccess(frame_id);
    page_table_.insert(std::make_pair(*page_id, frame_id));

    pages_latch_[frame_id].lock();
    latch_.unlock();
    InitNewPage(frame_id, *page_id);
    pages_latch_[frame_id].unlock();
    return pages_ + frame_id;
  }

  frame_id_t frame_id;
  if (replacer_->Evict(&frame_id)) {
    *page_id = AllocatePage();
    page_table_.erase(pages_[frame_id].GetPageId());
    replacer_->RecordAccess(frame_id);
    page_table_.insert(std::make_pair(*page_id, frame_id));

    pages_latch_[frame_id].lock();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    latch_.unlock();
    InitNewPage(frame_id, *page_id);
    pages_latch_[frame_id].unlock();
    return pages_ + frame_id;
  }
  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();

  if (page_table_.count(page_id) == 0) {
    if (!free_list_.empty()) {
      auto frame_id = free_list_.front();
      free_list_.pop_front();
      replacer_->RecordAccess(frame_id);
      page_table_.insert(std::make_pair(page_id, frame_id));

      pages_latch_[frame_id].lock();
      latch_.unlock();
      InitNewPage(frame_id, page_id);
      disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
      pages_latch_[frame_id].unlock();

      return pages_ + frame_id;
    }

    frame_id_t frame_id;
    if (replacer_->Evict(&frame_id)) {
      page_table_.erase(pages_[frame_id].GetPageId());
      replacer_->RecordAccess(frame_id);
      page_table_.insert(std::make_pair(page_id, frame_id));

      pages_latch_[frame_id].lock();
      latch_.unlock();
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
      InitNewPage(frame_id, page_id);
      disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
      pages_latch_[frame_id].unlock();

      return pages_ + frame_id;
    }
    latch_.unlock();
    return nullptr;
  }

  auto frame_id = page_table_[page_id];
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  pages_latch_[frame_id].lock();
  latch_.unlock();
  ++pages_[frame_id].pin_count_;
  pages_latch_[frame_id].unlock();

  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  auto frame_id = page_table_[page_id];

  pages_latch_[frame_id].lock();
  latch_.unlock();

  if (pages_[frame_id].GetPinCount() <= 0) {
    pages_latch_[frame_id].unlock();
    return false;
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_latch_[frame_id].unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  pages_latch_[frame_id].lock();
  latch_.unlock();
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  pages_latch_[frame_id].unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &[pid, fid] : page_table_) {
    FlushPage(pid);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return true;
  }
  auto frame_id = page_table_[page_id];

  pages_latch_[frame_id].lock();
  latch_.unlock();
  if (pages_[frame_id].GetPinCount() > 0) {
    pages_latch_[frame_id].unlock();
    return false;
  }
  pages_latch_[frame_id].unlock();

  latch_.lock();
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  pages_latch_[frame_id].lock();
  latch_.unlock();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  pages_latch_[frame_id].unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);

  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();

  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();

  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);

  return {this, page};
}

}  // namespace bustub
