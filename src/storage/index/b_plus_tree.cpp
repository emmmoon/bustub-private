#include <algorithm>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  // std::cout << "name: " << index_name_ << std::endl;
  // std::cout << "leafmaxsize: " << leaf_max_size_ << std::endl;
  // std::cout << "Internalmaxsize: " << internal_max_size_ << std::endl;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return GetRootPageId() == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  // std::cout << "Tree name " << index_name_ << " ";
  // std::cout << "thread " << std::this_thread::get_id() << "getvalue "
  //           << ": " << key << std::endl;
  ReadPageGuard p_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = p_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto node_page_id = header_page->root_page_id_;
  ReadPageGuard node_guard = bpm_->FetchPageRead(node_page_id);
  p_guard = std::move(node_guard);
  auto node = p_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto inode = p_guard.As<InternalPage>();
    node_page_id = inode->LookUp(key, comparator_);
    node_guard = bpm_->FetchPageRead(node_page_id);
    p_guard = std::move(node_guard);
    node = p_guard.As<BPlusTreePage>();
  }

  auto lpage = p_guard.As<LeafPage>();
  ValueType value;
  auto is_in = lpage->LookUp(key, comparator_, value);
  if (is_in) {
    result->push_back(value);
  }
  return is_in;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> page_id_t {
  auto node_page_id = GetRootPageId();
  ReadPageGuard node_guard = bpm_->FetchPageRead(node_page_id);
  auto node = node_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto inode = node_guard.As<InternalPage>();
    node_page_id = inode->LookUp(key, comparator_);
    node_guard = bpm_->FetchPageRead(node_page_id);
    node = node_guard.As<BPlusTreePage>();
  }

  return node_page_id;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  // int opi = OptimisticInsert(key, value);
  // if(opi == 0) {
  //   return false;
  // }
  // if(opi == 1) {
  //   return true;
  // }
  Context cxt;
  cxt.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = cxt.header_page_->As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t pid;
    auto new_guard = bpm_->NewPageGuarded(&pid);
    auto new_page = new_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);
    new_page->Insert(key, comparator_, value);
    cxt.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = pid;
    return true;
  }
  cxt.root_page_id_ = header_page->root_page_id_;
  auto node_page_id = cxt.root_page_id_;
  auto node_guard = bpm_->FetchPageWrite(node_page_id);
  auto node = node_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    if (node->GetSize() < node->GetMaxSize()) {
      cxt.header_page_->Drop();
      while (!cxt.write_set_.empty()) {
        cxt.write_set_.front().Drop();
        cxt.write_set_.pop_front();
      }
    }
    cxt.write_set_.emplace_back(std::move(node_guard));
    auto inode = cxt.write_set_.back().As<InternalPage>();
    node_page_id = inode->LookUp(key, comparator_);
    node_guard = bpm_->FetchPageWrite(node_page_id);
    node = node_guard.As<BPlusTreePage>();
  }

  auto lnode = node_guard.As<LeafPage>();
  bool is_leaf_split = lnode->IsSplitable();
  if (!is_leaf_split) {
    cxt.header_page_->Drop();
    while (!cxt.write_set_.empty()) {
      cxt.write_set_.front().Drop();
      cxt.write_set_.pop_front();
    }
  }
  ValueType uv = value;
  if (lnode->LookUp(key, comparator_, uv)) {
    cxt.header_page_->Drop();
    while (!cxt.write_set_.empty()) {
      cxt.write_set_.front().Drop();
      cxt.write_set_.pop_front();
    }
    return false;
  }
  // std::cout << "i " << key.ToString() << std::endl;
  auto lwnode = node_guard.AsMut<LeafPage>();
  lwnode->Insert(key, comparator_, value);
  if (is_leaf_split) {
    page_id_t child_page_id;
    auto new_page_guard = bpm_->NewPageGuarded(&child_page_id);
    auto new_page = new_page_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);
    lwnode->MoveHalfTo(new_page, child_page_id);
    KeyType key_from_child = new_page->KeyAt(0);

    bool up_to_p = true;
    page_id_t new_page_id;
    while (up_to_p) {
      InternalPage *pnode;
      if (!cxt.write_set_.empty()) {
        pnode = cxt.write_set_.back().AsMut<InternalPage>();
        if (pnode->IsSplitable()) {
          int left_or_right = pnode->IsInsertToNewPage(key_from_child, comparator_);
          new_page_id = SplitOfInternal(pnode, left_or_right);
          if (left_or_right < 0) {
            pnode->InsertToPosition(key_from_child, child_page_id, left_or_right);
            child_page_id = new_page_id;
            auto new_page_guard = bpm_->FetchPageRead(child_page_id);
            key_from_child = new_page_guard.As<InternalPage>()->KeyAt(0);
          } else if (left_or_right > 0) {
            auto new_page_guard = bpm_->FetchPageWrite(new_page_id);
            auto new_split_node = new_page_guard.AsMut<InternalPage>();
            new_split_node->InsertToPosition(key_from_child, child_page_id, left_or_right);
            child_page_id = new_page_id;
            key_from_child = new_split_node->KeyAt(0);
          } else {
            auto new_page_guard = bpm_->FetchPageWrite(new_page_id);
            auto new_split_node = new_page_guard.AsMut<InternalPage>();
            new_split_node->InsertToPosition(key_from_child, child_page_id, left_or_right);
            child_page_id = new_page_id;
            key_from_child = new_split_node->KeyAt(0);
          }
          cxt.write_set_.back().Drop();
          cxt.write_set_.pop_back();
          up_to_p = true;
        } else {
          pnode->Insert(key_from_child, comparator_, child_page_id);
          up_to_p = false;
        }
      } else {
        page_id_t new_root_id;
        auto new_root_guard = bpm_->NewPageGuarded(&new_root_id);
        pnode = new_root_guard.AsMut<InternalPage>();
        pnode->Init(internal_max_size_);
        pnode->Insert(key_from_child, comparator_, cxt.root_page_id_);
        auto header_page_write = cxt.header_page_->AsMut<BPlusTreeHeaderPage>();
        header_page_write->root_page_id_ = new_root_id;
        pnode->Insert(key_from_child, comparator_, child_page_id);
        up_to_p = false;
      }
    }
  }
  // else {
  // std::cout << "thread " << std::this_thread::get_id() << " insert "
  //           << ": " << key.ToString() << " page id: " << node_page_id
  //           << " parents: ";
  // for (auto &page_guard : cxt.write_set_) {
  //   std::cout << page_guard.PageId() << " -> ";
  // }
  // std::cout << "size: "<< cxt.write_set_.back().As<BPlusTreePage>()->GetSize();
  // std::cout << std::endl;
  // }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticInsert(const KeyType &key, const ValueType &value, Transaction *txn) -> int {
  WritePageGuard p_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = p_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t pid;
    auto new_guard = bpm_->NewPageGuarded(&pid);
    auto new_page = new_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);
    new_page->Insert(key, comparator_, value);
    p_guard.Drop();
    auto pw_guard = bpm_->FetchPageWrite(header_page_id_);
    pw_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = pid;
    return 1;
  }
  auto node_page_id = header_page->root_page_id_;
  WritePageGuard node_guard = bpm_->FetchPageWrite(node_page_id);
  p_guard = std::move(node_guard);
  auto node = p_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto inode = p_guard.As<InternalPage>();
    node_page_id = inode->LookUp(key, comparator_);
    node_guard = bpm_->FetchPageWrite(node_page_id);
    p_guard = std::move(node_guard);
    node = p_guard.As<BPlusTreePage>();
  }
  auto lnode = p_guard.As<LeafPage>();
  ValueType uv = value;
  if (lnode->LookUp(key, comparator_, uv)) {
    return 0;
  }
  if (lnode->GetSize() < lnode->GetMaxSize() - 1) {
    auto lwnode = p_guard.AsMut<LeafPage>();
    lwnode->Insert(key, comparator_, value);
    return 1;
  }
  return 2;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitOfInternal(InternalPage *node, int mode) -> page_id_t {
  page_id_t page_id;
  auto new_page_guard = bpm_->NewPageGuarded(&page_id);
  auto new_page = new_page_guard.AsMut<InternalPage>();
  new_page->Init(internal_max_size_);
  bool lite = mode >= 0;
  node->MoveHalfTo(new_page, page_id, lite);

  return page_id;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  // std::cout << "Tree name " << index_name_ << " ";
  // std::cout << "thread " << std::this_thread::get_id() << "delete "
  //           << ": " << key << std::endl;
  Context cxt;
  cxt.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = cxt.header_page_->As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  std::deque<int> entry;
  cxt.root_page_id_ = header_page->root_page_id_;
  auto node_page_id = cxt.root_page_id_;
  auto node_guard = bpm_->FetchPageWrite(node_page_id);
  auto node = node_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto inode = node_guard.As<InternalPage>();
    int key_index;
    node_page_id = inode->LookUpOfDeletion(key, comparator_, &key_index);
    // if(node->GetSize() > node->GetMinSize()) {
    //   cxt.header_page_->Drop();
    //   while(!cxt.write_set_.empty()) {
    //     cxt.write_set_.front().Drop();
    //     cxt.write_set_.pop_front();
    //   }
    // }
    cxt.write_set_.emplace_back(std::move(node_guard));
    entry.emplace_back(key_index);
    node_guard = bpm_->FetchPageWrite(node_page_id);
    node = node_guard.As<BPlusTreePage>();
  }

  auto lnode = node_guard.As<LeafPage>();
  ValueType uv;
  if (!lnode->LookUp(key, comparator_, uv)) {
    return;
  }
  // std::cout << "d " << key.ToString() << std::endl;
  auto lwnode = node_guard.AsMut<LeafPage>();
  // std::cout << "thread " << std::this_thread::get_id() << " delete "
  //           << ": " << key.ToString() << " page id: " << node_page_id
  //           << std::endl;
  if (cxt.root_page_id_ == node_page_id) {
    auto is_empty = DeleteInRootLeaf(lwnode, comparator_, key);
    if (is_empty) {
      cxt.write_set_.clear();
      bpm_->DeletePage(cxt.root_page_id_);
      cxt.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }
  lwnode->Delete(key, comparator_);

  KeyType replace_key = lwnode->KeyAt(0);
  bool all_zero = true;
  if (lwnode->IsUnderFlow()) {
    auto pnode = cxt.write_set_.back().AsMut<InternalPage>();
    int pindex = entry.back();
    if (pindex != 0) {
      all_zero = false;
    }
    entry.pop_back();
    auto re_or_coal = RedistributeOrCoalesce(pnode, pindex);
    if (re_or_coal == RedistributeOrCoalesceType::LEFT_REDISTRIBUTE ||
        re_or_coal == RedistributeOrCoalesceType::RIGHT_REDISTRIBUTE) {
      Redistribute(pnode, pindex, lwnode, re_or_coal);
      pnode->SetKeyAt(pindex, lwnode->KeyAt(0));
      replace_key = lwnode->KeyAt(0);
    } else if (re_or_coal == RedistributeOrCoalesceType::LEFT_COALESCE) {
      Coalesce(pnode, pindex, lwnode, re_or_coal);
      auto leaf_page_id = pnode->ValueAt(pindex);
      pnode->DeleteInPosition(pindex);
      bpm_->DeletePage(leaf_page_id);
    } else if (re_or_coal == RedistributeOrCoalesceType::RIGHT_COALESCE) {
      Coalesce(pnode, pindex, lwnode, re_or_coal);
      auto leaf_page_id = pnode->ValueAt(pindex + 1);
      pnode->DeleteInPosition(pindex + 1);
      pnode->SetKeyAt(pindex, lwnode->KeyAt(0));
      replace_key = lwnode->KeyAt(0);
      bpm_->DeletePage(leaf_page_id);
    }
    auto node = pnode;
    cxt.write_set_.back().Drop();
    cxt.write_set_.pop_back();
    // bool up_to_p = true;
    while (node->IsUnderFlow()) {
      if (!cxt.write_set_.empty()) {
        pnode = cxt.write_set_.back().AsMut<InternalPage>();
        pindex = entry.back();
        if (pindex != 0) {
          all_zero = false;
        }
        entry.pop_back();
        re_or_coal = RedistributeOrCoalesce(pnode, pindex);
        if (re_or_coal == RedistributeOrCoalesceType::LEFT_REDISTRIBUTE ||
            re_or_coal == RedistributeOrCoalesceType::RIGHT_REDISTRIBUTE) {
          Redistribute(pnode, pindex, node, re_or_coal);
          pnode->SetKeyAt(pindex, node->KeyAt(0));
        } else if (re_or_coal == RedistributeOrCoalesceType::LEFT_COALESCE) {
          Coalesce(pnode, pindex, node, re_or_coal);
          auto leaf_page_id = pnode->ValueAt(pindex);
          pnode->DeleteInPosition(pindex);
          bpm_->DeletePage(leaf_page_id);
        } else if (re_or_coal == RedistributeOrCoalesceType::RIGHT_COALESCE) {
          Coalesce(pnode, pindex, node, re_or_coal);
          auto leaf_page_id = pnode->ValueAt(pindex + 1);
          pnode->DeleteInPosition(pindex + 1);
          pnode->SetKeyAt(pindex, node->KeyAt(0));
          bpm_->DeletePage(leaf_page_id);
        }
        node = pnode;
        cxt.write_set_.back().Drop();
        cxt.write_set_.pop_back();
        // up_to_p = true;
      } else {
        if (node->GetSize() <= 1) {
          auto new_root_id = node->ValueAt(0);
          bpm_->DeletePage(cxt.root_page_id_);
          cxt.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
        }
        // up_to_p = false;
        break;
      }
    }
  }
  while (all_zero && !entry.empty()) {
    cxt.write_set_.back().AsMut<InternalPage>()->SetKeyAt(entry.back(), replace_key);
    cxt.write_set_.back().Drop();
    cxt.write_set_.pop_back();
    if (entry.back() != 0) {
      all_zero = false;
    }
    entry.pop_back();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticRemove(const KeyType &key, Transaction *txn) -> bool {
  ReadPageGuard p_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = p_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  auto root_page_id = header_page->root_page_id_;
  auto node_page_id = root_page_id;
  ReadPageGuard node_guard = bpm_->FetchPageRead(node_page_id);
  p_guard = std::move(node_guard);
  auto node = p_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto inode = p_guard.As<InternalPage>();
    node_page_id = inode->LookUp(key, comparator_);
    node_guard = bpm_->FetchPageRead(node_page_id);
    p_guard = std::move(node_guard);
    node = p_guard.As<BPlusTreePage>();
  }
  p_guard.Drop();
  auto lw_guard = bpm_->FetchPageWrite(node_page_id);
  auto lnode = lw_guard.As<LeafPage>();
  ValueType uv;
  if (!lnode->LookUp(key, comparator_, uv)) {
    return true;
  }
  if (((node_page_id == root_page_id) && (lnode->GetSize() > 1)) ||
      ((node_page_id != root_page_id) && (lnode->GetSize() > lnode->GetMinSize()))) {
    auto lwnode = lw_guard.AsMut<LeafPage>();
    lwnode->Delete(key, comparator_);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteInRootLeaf(LeafPage *node, const KeyComparator &comparator, const KeyType &key) -> bool {
  node->Delete(key, comparator);
  return static_cast<bool>(node->GetSize() == 0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RedistributeOrCoalesce(InternalPage *pnode, const int &key_index) -> RedistributeOrCoalesceType {
  page_id_t left_bro_id;
  page_id_t right_bro_id;
  if (key_index != 0) {
    left_bro_id = pnode->ValueAt(key_index - 1);
    ReadPageGuard lbnode_guard = bpm_->FetchPageRead(left_bro_id);
    auto lbnode = lbnode_guard.As<LeafPage>();
    if (lbnode->CanRedistribute()) {
      return RedistributeOrCoalesceType::LEFT_REDISTRIBUTE;
    }
  } else {
    right_bro_id = pnode->ValueAt(key_index + 1);
    ReadPageGuard rbnode_guard = bpm_->FetchPageRead(right_bro_id);
    auto rbnode = rbnode_guard.As<LeafPage>();
    if (rbnode->CanRedistribute()) {
      return RedistributeOrCoalesceType::RIGHT_REDISTRIBUTE;
    }
  }
  if (key_index != 0) {
    return RedistributeOrCoalesceType::LEFT_COALESCE;
  }
  return RedistributeOrCoalesceType::RIGHT_COALESCE;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::Redistribute(InternalPage *pnode, const int &key_index, T *node, RedistributeOrCoalesceType ftype)
    -> void {
  if (ftype == RedistributeOrCoalesceType::LEFT_REDISTRIBUTE) {
    auto left_bro_id = pnode->ValueAt(key_index - 1);
    WritePageGuard lbnode_guard = bpm_->FetchPageWrite(left_bro_id);
    auto lbnode = lbnode_guard.AsMut<T>();
    lbnode->DistributeOneToRight(node);

    return;
  }
  auto right_bro_id = pnode->ValueAt(key_index + 1);
  WritePageGuard rbnode_guard = bpm_->FetchPageWrite(right_bro_id);
  auto rbnode = rbnode_guard.AsMut<T>();
  rbnode->DistributeOneToLeft(node);
  pnode->SetKeyAt(key_index + 1, rbnode->KeyAt(0));
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::Coalesce(InternalPage *pnode, const int &key_index, T *node, RedistributeOrCoalesceType ftype)
    -> void {
  if (ftype == RedistributeOrCoalesceType::LEFT_COALESCE) {
    auto left_bro_id = pnode->ValueAt(key_index - 1);
    WritePageGuard lbnode_guard = bpm_->FetchPageWrite(left_bro_id);
    auto lbnode = lbnode_guard.AsMut<T>();
    node->CoalesceWithLeft(lbnode);

    return;
  }
  auto right_bro_id = pnode->ValueAt(key_index + 1);
  WritePageGuard rbnode_guard = bpm_->FetchPageWrite(right_bro_id);
  auto rbnode = rbnode_guard.AsMut<T>();
  rbnode->CoalesceWithLeft(node);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard p_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = p_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  // return INDEXITERATOR_TYPE(bpm_, 1, 0);
  auto node_page_id = header_page->root_page_id_;
  ReadPageGuard node_guard = bpm_->FetchPageRead(node_page_id);
  p_guard = std::move(node_guard);
  auto node = p_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto inode = p_guard.As<InternalPage>();
    node_page_id = inode->ValueAt(0);
    node_guard = bpm_->FetchPageRead(node_page_id);
    p_guard = std::move(node_guard);
    node = p_guard.As<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE{bpm_, node_page_id, 0};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard p_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = p_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  auto node_page_id = header_page->root_page_id_;
  ReadPageGuard node_guard = bpm_->FetchPageRead(node_page_id);
  p_guard = std::move(node_guard);
  auto node = p_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto inode = p_guard.As<InternalPage>();
    node_page_id = inode->LookUp(key, comparator_);
    node_guard = bpm_->FetchPageRead(node_page_id);
    p_guard = std::move(node_guard);
    node = p_guard.As<BPlusTreePage>();
  }

  auto lpage = p_guard.As<LeafPage>();
  ValueType value;
  auto is_in = lpage->LookUp(key, comparator_, value);
  if (is_in) {
    int index = lpage->LookUpForIndex(key, comparator_);
    return INDEXITERATOR_TYPE(bpm_, node_page_id, index);
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, -1, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpdateRootPageId(page_id_t page_id) -> void {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
