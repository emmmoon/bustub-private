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
#include <algorithm>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.empty()) {
    return false;
  }
  if (node_list_.empty()) {
    return false;
  }
  auto fid = node_list_.front();
  *frame_id = fid;
  --curr_size_;
  node_store_.erase(*frame_id);
  node_list_.pop_front();

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  ++current_timestamp_;

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw bustub::NotImplementedException("The frame_id is over the size");
  }

  if (node_store_.count(frame_id) == 0) {
    auto new_node = std::make_shared<LRUKNode>(k_);
    new_node->AddAccess(current_timestamp_);
    node_store_.insert(std::make_pair(frame_id, new_node));
  } else {
    node_store_[frame_id]->AddAccess(current_timestamp_);
    if (node_store_[frame_id]->IsEvictable()) {
      auto it1 = std::find(node_list_.begin(), node_list_.end(), frame_id);
      node_list_.erase(it1);
      node_list_.emplace_back(frame_id);
      // auto node1 = node_store_[frame_id];
      // bool aready_insert = false;
      // for (auto it2 = node_list_.begin(); it2 != node_list_.end(); ++it2) {
      //   auto node2 = node_store_[*it2];
      //   if (*node1 < *node2) {
      //     node_list_.erase(it1);
      //     node_list_.insert(it2, frame_id);
      //     aready_insert = true;
      //     break;
      //   }
      // }

      // if (!aready_insert) {
      //   node_list_.erase(it1);
      //   node_list_.emplace_back(frame_id);
      // }
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw bustub::NotImplementedException("The frame_id is over the size");
  }
  if (node_store_[frame_id]->IsEvictable() != set_evictable) {
    if (node_store_[frame_id]->IsEvictable()) {
      --curr_size_;
      node_store_[frame_id]->ModifyEvictable();
      auto it = std::find(node_list_.begin(), node_list_.end(), frame_id);
      node_list_.erase(it);

    } else {
      ++curr_size_;
      node_store_[frame_id]->ModifyEvictable();

      if (node_list_.empty()) {
        node_list_.emplace_front(frame_id);
      } else {
        auto new_node = node_store_[frame_id];
        bool aready_insert = false;
        for (auto it2 = node_list_.begin(); it2 != node_list_.end(); ++it2) {
          auto node2 = node_store_[*it2];
          if (*new_node < *node2) {
            node_list_.insert(it2, frame_id);
            aready_insert = true;
            break;
          }
        }
        if (!aready_insert) {
          node_list_.emplace_back(frame_id);
        }
      }
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (!node_store_[frame_id]->IsEvictable()) {
    throw bustub::NotImplementedException("The frame is not evictable.");
  }
  --curr_size_;
  node_store_.erase(frame_id);
  auto it = std::find(node_list_.begin(), node_list_.end(), frame_id);
  node_list_.erase(it);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);

  return curr_size_;
}

LRUKNode::LRUKNode(size_t k) : k_(k) {}
}  // namespace bustub
