#include "primer/trie.h"

#include <string_view>

#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key.
  // If the node doesn't exist, return nullptr. After you find the node, you
  // should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is
  // mismatched, and you should return nullptr. Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }

  std::shared_ptr<const TrieNode> node = root_;

  for (char k : key) {
    if (node->children_.count(k) == 0) {
      return nullptr;
    }
    node = node->children_.at(k);
  }

  auto terminal = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (terminal == nullptr) {
    return nullptr;
  }

  return terminal->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when
  // creating `shared_ptr` on that value. throw
  // NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the
  // node corresponding to the key already exists, you should create a new
  // `TrieNodeWithValue`.

  std::shared_ptr<TrieNode> node;
  auto value_p = std::make_shared<T>(std::move(value));

  // new root, new trie
  if (key.empty()) {
    if (root_ == nullptr) {
      node = std::make_shared<TrieNodeWithValue<T>>(value_p);
    } else {
      node = std::make_shared<TrieNodeWithValue<T>>(root_->children_, value_p);
    }
  } else {
    if (root_ == nullptr) {
      node = std::make_shared<TrieNode>();
    } else {
      node = root_->Clone();
    }
  }
  Trie trie(node);

  if (key.empty()) {
    return trie;
  }

  // put
  size_t i = 0;
  std::shared_ptr<TrieNode> new_node;
  for (char k : key) {
    if (node->children_.count(k) == 0) {
      if (i == key.length() - 1) {
        new_node = std::make_shared<TrieNodeWithValue<T>>(value_p);
      } else {
        new_node = std::make_shared<TrieNode>();
      }
    } else {
      auto child = node->children_.at(k);
      if (i == key.length() - 1) {
        new_node =
            std::make_shared<TrieNodeWithValue<T>>(child->children_, value_p);
      } else {
        new_node = child->Clone();
      }
    }

    node->children_[k] = new_node;
    node = new_node;
    i++;
  }

  return trie;
}

// see cur as root, key[key_index:] as key, try to do "remove"
// return whether "remove" is done, and return a new root if "remove" takes place.
// (no remove if key not exist or try to remove a TrieNode which has no value)
auto Trie::remove(std::shared_ptr<const TrieNode> cur, std::string_view key,
                  size_t key_index)
    -> std::tuple<bool, std::shared_ptr<const TrieNode>> {
  char k = key[key_index];
  if (cur->children_.count(k) == 0) {
    return {false, nullptr};
  }

  bool do_remove;
  std::shared_ptr<const TrieNode> new_child;

  auto child = cur->children_.at(k);

  if (key.length() - 1 == key_index) {
    do_remove = child->is_value_node_;
    if (do_remove && !child->children_.empty()) {
      new_child = std::make_shared<TrieNode>(child->children_);
    } else {
      new_child = nullptr;
    }
  } else {
    std::tie(do_remove, new_child) = remove(child, key, key_index + 1);
  }

  if (do_remove) {
    if (new_child == nullptr) {
      if (cur->children_.size() > 1 || cur->is_value_node_) {
        auto new_node = std::shared_ptr<TrieNode>(cur->Clone());
        new_node->children_.erase(k);
        return {true, new_node};
      } else {
        return {true, nullptr};  // after deleting child, cur has no child, so also delete it
      }
    } else {
      auto new_node = std::shared_ptr<TrieNode>(cur->Clone());
      new_node->children_[k] = new_child;
      return {true, new_node};
    }
  } else {
    return {false, nullptr};
  }
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node
  // doesn't contain a value any more, you should convert it to `TrieNode`. If a
  // node doesn't have children any more, you should remove it.
  if (root_ == nullptr) {
    return *this;
  }

  if (key.empty()) {
    if (root_->is_value_node_) {
      return Trie(std::make_shared<TrieNode>(root_->children_));
    } else {
      return *this;
    }
  }

  auto [do_delete, new_node] = remove(root_, key, 0);

  if (do_delete) {
    return Trie(new_node);
  } else {
    return *this;
  }
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and
// functions in the header file. However, we separate the implementation into a
// .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate
// them here, so that they can be picked up by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below
// lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
