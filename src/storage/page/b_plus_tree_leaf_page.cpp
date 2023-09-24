//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "type/value.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  ValueType value = array_[index].second;
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, const KeyComparator &comparator, ValueType &value) const
    -> bool {
  MappingType key_to_find = std::make_pair(key, value);
  int index =
      std::upper_bound(array_, array_ + GetSize(), key_to_find,
                       [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
      array_ - 1;
  if (index >= 0) {
    auto cmp = comparator(key, KeyAt(index));
    if (cmp == 0) {
      value = array_[index].second;
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUpForIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  ValueType value;
  MappingType key_to_find = std::make_pair(key, value);
  int index =
      std::upper_bound(array_, array_ + GetSize(), key_to_find,
                       [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
      array_ - 1;
  return index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const KeyComparator &comparator, const ValueType &value)
    -> void {
  MappingType key_to_find = std::make_pair(key, value);
  int index =
      std::upper_bound(array_, array_ + GetSize(), key_to_find,
                       [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
      array_;
  for (int i = GetSize() - 1; i >= index; --i) {
    array_[i + 1] = array_[i];
  }
  array_[index] = key_to_find;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertToPosition(const KeyType &key, const ValueType &value, const int &pos) -> void {
  MappingType key_to_insert = std::make_pair(key, value);
  for (int i = GetSize() - 1; i >= pos; --i) {
    array_[i + 1] = array_[i];
  }
  array_[pos] = key_to_insert;

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteInPosition(const int &pos) -> void {
  for (int i = pos; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *new_page, const page_id_t &new_page_id) -> void {
  int move_num = GetMinSize();
  int start_index = GetSize() - move_num;
  new_page->CopyFrom(array_ + start_index, move_num);
  new_page->SetNextPageId(GetNextPageId());
  SetNextPageId(new_page_id);
  IncreaseSize(-move_num);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFrom(MappingType *items, int size) -> void {
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> void {
  ValueType value;
  MappingType key_to_delete = std::make_pair(key, value);
  int index =
      std::upper_bound(array_, array_ + GetSize(), key_to_delete,
                       [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
      array_ - 1;
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DistributeOneToRight(BPlusTreeLeafPage *rbnode) -> void {
  KeyType key_to_distribute = array_[GetSize() - 1].first;
  ValueType value_to_distribute = array_[GetSize() - 1].second;
  rbnode->InsertToPosition(key_to_distribute, value_to_distribute, 0);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DistributeOneToLeft(BPlusTreeLeafPage *lbnode) -> void {
  KeyType key_to_distribute = array_[0].first;
  ValueType value_to_distribute = array_[0].second;
  DeleteInPosition(0);
  lbnode->InsertToPosition(key_to_distribute, value_to_distribute, lbnode->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CoalesceWithLeft(BPlusTreeLeafPage *lbnode) -> void {
  int move_num = GetSize();
  lbnode->CopyFrom(array_, move_num);
  IncreaseSize(-move_num);
  lbnode->SetNextPageId(GetNextPageId());
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
