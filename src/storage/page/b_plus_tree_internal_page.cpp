//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, const KeyComparator &comparator) const -> page_id_t {
  MappingType key_to_find = std::make_pair(key, INVALID_PAGE_ID);
  int index =
      std::upper_bound(array_ + 1, array_ + GetSize(), key_to_find,
                       [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
      array_ - 1;
  return ValueAt(index);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUpOfDeletion(const KeyType &key, const KeyComparator &comparator,
                                                      int *key_index) const -> page_id_t {
  MappingType key_to_find = std::make_pair(key, INVALID_PAGE_ID);
  int index =
      std::upper_bound(array_ + 1, array_ + GetSize(), key_to_find,
                       [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
      array_ - 1;

  *key_index = index;
  return ValueAt(index);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const KeyComparator &comparator,
                                            const page_id_t &new_page_id) -> void {
  MappingType key_to_find = std::make_pair(key, new_page_id);
  if (GetSize() == 0) {
    array_[0].second = new_page_id;
  } else {
    int index = std::upper_bound(
                    array_ + 1, array_ + GetSize(), key_to_find,
                    [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
                array_;
    for (int i = GetSize() - 1; i >= index; --i) {
      array_[i + 1] = array_[i];
    }
    array_[index] = key_to_find;
  }
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertToPosition(const KeyType &key, const page_id_t &new_page_id, const int &pos)
    -> void {
  MappingType key_to_find = std::make_pair(key, new_page_id);
  int index = pos >= 0 ? pos : GetSize() + 1 + pos;
  for (int i = GetSize() - 1; i >= index; --i) {
    array_[i + 1] = array_[i];
  }
  array_[index] = key_to_find;

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteInPosition(const int &pos) -> void {
  for (int i = pos; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *new_page, page_id_t new_page_id,
                                                const bool &lite) -> void {
  int move_num = lite ? GetMinSize() - 1 : GetMinSize();
  int start_index = GetSize() - move_num;
  new_page->CopyFrom(array_ + start_index, move_num);
  IncreaseSize(-move_num);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFrom(MappingType *items, int size) -> void {
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsInsertToNewPage(const KeyType &key, const KeyComparator &comparator) const
    -> int {
  MappingType key_to_find = std::make_pair(key, INVALID_PAGE_ID);
  int index =
      std::upper_bound(array_ + 1, array_ + GetSize(), key_to_find,
                       [=](MappingType lhs, MappingType rhs) -> bool { return comparator(lhs.first, rhs.first) < 0; }) -
      array_;
  int move_size = GetMinSize();
  int start_index = GetSize() + 1 - move_size;
  return index - start_index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DistributeOneToRight(BPlusTreeInternalPage *rbnode) -> void {
  KeyType key_to_distribute = array_[GetSize() - 1].first;
  ValueType value_to_distribute = array_[GetSize() - 1].second;
  rbnode->InsertToPosition(key_to_distribute, value_to_distribute, 0);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DistributeOneToLeft(BPlusTreeInternalPage *lbnode) -> void {
  KeyType key_to_distribute = array_[0].first;
  ValueType value_to_distribute = array_[0].second;
  DeleteInPosition(0);
  lbnode->InsertToPosition(key_to_distribute, value_to_distribute, lbnode->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CoalesceWithLeft(BPlusTreeInternalPage *lbnode) -> void {
  int move_num = GetSize();
  lbnode->CopyFrom(array_, move_num);
  IncreaseSize(-move_num);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
