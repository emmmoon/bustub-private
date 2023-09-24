#include "primer/trie.h"
#include <algorithm>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto curoot = this->root_;
  for (auto ch : key) {
    if (curoot == nullptr) {
      return nullptr;
    }
    auto ucuroot = curoot->Clone();
    if (ucuroot->children_.count(ch) == 0) {
      return nullptr;
    }
    curoot = ucuroot->children_[ch];
  }

  auto new_cur = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(curoot);
  if (new_cur != nullptr) {
    const TrieNodeWithValue<T> *ptr = new_cur.get();
    return ptr->value_.get();
  }

  return nullptr;
}

template <class T>
auto Trie::PutDfs(const std::shared_ptr<const TrieNode> &root, std::string_view key, T value, unsigned int index) const
    -> std::shared_ptr<TrieNode> {
  if (index == key.size()) {
    if (root == nullptr) {
      auto new_node_f = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));

      return new_node_f;
    }
    auto old_node_uf = root->Clone();
    auto old_node_f = std::shared_ptr<TrieNode>(std::move(old_node_uf));
    auto new_node_f =
        std::make_shared<TrieNodeWithValue<T>>(old_node_f->children_, std::make_shared<T>(std::move(value)));

    return new_node_f;
  }

  if (root == nullptr) {
    auto new_node = std::make_shared<TrieNode>();
    new_node->children_[key[index]] = PutDfs(nullptr, key, std::move(value), index + 1);

    return new_node;
  }

  auto old_node_uf = root->Clone();
  auto new_node = std::shared_ptr<TrieNode>(std::move(old_node_uf));
  if (new_node->children_.count(key[index]) != 0) {
    new_node->children_[key[index]] = PutDfs(new_node->children_[key[index]], key, std::move(value), index + 1);

    return new_node;
  }
  new_node->children_[key[index]] = PutDfs(nullptr, key, std::move(value), index + 1);

  return new_node;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  auto new_root = PutDfs(this->root_, key, std::move(value), 0);
  return Trie(new_root);
}

auto Trie::RemoveDfs(const std::shared_ptr<const TrieNode> &root, std::string_view key, unsigned int index) const
    -> std::shared_ptr<TrieNode> {
  if (index == key.size()) {
    if (root->children_.empty()) {
      return nullptr;
    }
    auto new_node = std::make_shared<TrieNode>(root->children_);
    return new_node;
  }
  std::shared_ptr<const TrieNode> new_node;
  auto t = std::shared_ptr<const TrieNode>(root);

  if (t->children_.count(key[index]) != 0) {
    new_node = RemoveDfs(t->Clone()->children_[key[index]], key, index + 1);
    auto node = root->Clone();
    if (new_node) {
      node->children_[key[index]] = new_node;
    } else {
      node->children_.erase(key[index]);
      if (!node->is_value_node_ && node->children_.empty()) {
        return nullptr;
      }
    }
    return node;
  }
  return root->Clone();
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  auto root = RemoveDfs(this->root_, key, 0);

  return Trie(root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
