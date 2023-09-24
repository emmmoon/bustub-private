/**
 * index_iterator.cpp
 */
#include <cassert>
#include <ostream>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : bpm_(buffer_pool_manager), page_id_(page_id), index_(index) {
  if (page_id_ != -1) {
    auto page_guard = bpm_->FetchPageRead(page_id_);
    auto page = page_guard.As<LeafPage>();
    item_ = {page->KeyAt(index_), page->ValueAt(index_)};
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return item_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto page_guard = bpm_->FetchPageRead(page_id_);
  auto page = page_guard.As<LeafPage>();
  if (index_ < page->GetSize() - 1) {
    ++index_;
    item_ = {page->KeyAt(index_), page->ValueAt(index_)};
  } else {
    page_id_ = page->GetNextPageId();
    if (page_id_ == -1) {
      index_ = -1;
      item_ = {};
    } else {
      index_ = 0;
      auto new_guard = bpm_->FetchPageRead(page_id_);
      auto new_page = new_guard.As<LeafPage>();
      item_ = {new_page->KeyAt(index_), new_page->ValueAt(index_)};
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
