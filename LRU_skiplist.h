#include <iostream>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <mutex>
#include <fstream>
#include <unordered_map>
#include <list>
#include <ctime>

#define STORE_FILE "store/dumpFile"

std::mutex mtx; // 用于临界区的互斥锁
std::string delimiter = ":"; // 用于解析键值对的分隔符

// 节点类模板，包含键值对和指向下一级节点的指针
template<typename K, typename V>
class Node {

public:

    Node() {}

    // 构造函数，初始化键值对和节点层数，以及过期时间
    Node(K k, V v, int level, time_t expire_time = 0);

    ~Node();

    K get_key() const;

    V get_value() const;

    time_t get_expire_time() const; // 获取过期时间

    void set_value(V);

    void set_expire_time(time_t); // 设置过期时间

    Node<K, V> **forward; // 指向下一级节点的指针数组

    int node_level; // 节点所在的层级

private:
    K key;
    V value;
    time_t expire_time; // 过期时间戳
};

// Node类的构造函数实现
template<typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level, time_t expire_time) {
    this->key = k;
    this->value = v;
    this->node_level = level;
    this->expire_time = expire_time;

    // 创建指向下一级节点的指针数组
    this->forward = new Node<K, V>*[level + 1];

    // 初始化指针数组为空（NULL）
    memset(this->forward, 0, sizeof(Node<K, V>*) * (level + 1));
}

// Node类的析构函数，实现对指针数组的释放
template<typename K, typename V>
Node<K, V>::~Node() {
    delete[] forward;
}

template<typename K, typename V>
K Node<K, V>::get_key() const {
    return key;
}

template<typename K, typename V>
V Node<K, V>::get_value() const {
    return value;
}

template<typename K, typename V>
time_t Node<K, V>::get_expire_time() const {
    return expire_time;
}

template<typename K, typename V>
void Node<K, V>::set_value(V value) {
    this->value = value;
}

template<typename K, typename V>
void Node<K, V>::set_expire_time(time_t expire_time) {
    this->expire_time = expire_time;
}

// LRU缓存类模板，使用哈希表和双向链表实现
template<typename K, typename V>
class LRUCache {
public:
    LRUCache(size_t capacity); // 构造函数，初始化缓存容量

    ~LRUCache();

    bool get(const K& key, V& value); // 获取键对应的值

    void put(const K& key, const V& value, time_t expire_time); // 插入键值对并设置过期时间

    void remove(const K& key); // 移除指定键的元素

    void evict_expired_items(); // 清理过期的元素

private:
    size_t capacity; // 缓存容量
    std::list<std::pair<K, Node<K, V>*>> item_list; // 双向链表用于记录访问顺序
    std::unordered_map<K, typename std::list<std::pair<K, Node<K, V>*>>::iterator> item_map; // 哈希表用于快速查找
};

template<typename K, typename V>
LRUCache<K, V>::LRUCache(size_t capacity) : capacity(capacity) {}

template<typename K, typename V>
LRUCache<K, V>::~LRUCache() {
    // 析构函数，清理缓存中的节点
    for (auto& item : item_list) {
        delete item.second;
    }
}

// 获取键对应的值，并将该键移动到链表头部（表示最近使用）
template<typename K, typename V>
bool LRUCache<K, V>::get(const K& key, V& value) {
    auto it = item_map.find(key);
    if (it == item_map.end()) {
        return false; // 未找到该键
    }
    value = it->second->second->get_value();
    item_list.splice(item_list.begin(), item_list, it->second); // 将节点移动到链表头部
    return true;
}

// 插入键值对到缓存，如果缓存已满则移除最久未使用的节点
template<typename K, typename V>
void LRUCache<K, V>::put(const K& key, const V& value, time_t expire_time) {
    auto it = item_map.find(key);
    if (it != item_map.end()) {
        // 如果键已存在，更新值和过期时间，并将节点移动到链表头部
        item_list.splice(item_list.begin(), item_list, it->second);
        it->second->second->set_value(value);
        it->second->second->set_expire_time(expire_time);
    } else {
        // 如果缓存已满，移除链表尾部（最久未使用）的节点
        if (item_list.size() >= capacity) {
            auto last = item_list.end();
            last--;
            item_map.erase(last->first);
            delete last->second;
            item_list.pop_back();
        }
        // 插入新节点到链表头部
        Node<K, V>* node = new Node<K, V>(key, value, 0, expire_time);
        item_list.push_front(std::make_pair(key, node));
        item_map[key] = item_list.begin();
    }
}

// 移除指定键的节点
template<typename K, typename V>
void LRUCache<K, V>::remove(const K& key) {
    auto it = item_map.find(key);
    if (it != item_map.end()) {
        item_list.erase(it->second); // 从链表中移除节点
        delete it->second->second; // 删除节点
        item_map.erase(it); // 从哈希表中移除键
    }
}

// 清理已过期的键值对
template<typename K, typename V>
void LRUCache<K, V>::evict_expired_items() {
    time_t now = time(nullptr);
    for (auto it = item_list.begin(); it != item_list.end();) {
        if (it->second->get_expire_time() <= now) { // 检查是否过期
            auto erase_it = it++;
            item_map.erase(erase_it->first);
            delete erase_it->second;
            item_list.erase(erase_it);
        } else {
            ++it;
        }
    }
}

// 跳表类模板
template<typename K, typename V>
class SkipList {

public:
    SkipList(int, size_t lru_capacity); // 构造函数，初始化最大层数和LRU缓存容量

    ~SkipList();

    int insert_element(K, V, time_t expire_time = 0); // 插入元素

    void delete_element(K); // 删除元素

    bool search_element(K); // 查找元素

    void display_list(); // 显示跳表内容

    void dump_file(); // 保存跳表数据到文件

    void load_file(); // 从文件加载跳表数据

    void evict_expired_items(); // 清理过期元素

    int size(); // 获取跳表大小

private:
    int get_random_level(); // 获取随机层级

    Node<K, V>* create_node(K, V, int, time_t expire_time = 0); // 创建新节点

    void get_key_value_from_string(const std::string& str, std::string* key, std::string* value, std::string* expire_time_str); // 从字符串解析键值对和过期时间

    bool is_valid_string(const std::string& str); // 检查字符串是否有效

    void clear(Node<K, V>*); // 递归删除节点

private:
    int _max_level; // 最大层级
    int _skip_list_level; // 当前层级
    int _element_count; // 跳表中的元素数量
    Node<K, V>* _header; // 跳表头节点
    LRUCache<K, V>* _lru_cache; // LRU缓存指针
    std::ofstream _file_writer; // 文件写操作对象
    std::ifstream _file_reader; // 文件读操作对象
};

// 创建新节点
template<typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(const K k, const V v, int level, time_t expire_time) {
    Node<K, V>* n = new Node<K, V>(k, v, level, expire_time);
    return n;
}

// 获取随机层级，用于插入新节点时确定它在跳表中的层级
template<typename K, typename V>
int SkipList<K, V>::get_random_level() {
    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
}

// 跳表构造函数，初始化最大层级和LRU缓存容量
template<typename K, typename V>
SkipList<K, V>::SkipList(int max_level, size_t lru_capacity) {
    this->_max_level = max_level;
    this->_skip_list_level = 0;
    this->_element_count = 0;

    // 创建头节点并初始化键值为空
    K k;
    V v;
    this->_header = new Node<K, V>(k, v, _max_level);

    // 初始化LRU缓存
    _lru_cache = new LRUCache<K, V>(lru_capacity);
}

// 跳表析构函数，清理资源
template<typename K, typename V>
SkipList<K, V>::~SkipList() {
    if (_file_writer.is_open()) {
        _file_writer.close();
    }
    if (_file_reader.is_open()) {
        _file_reader.close();
    }

    if (_header->forward[0] != nullptr) {
        clear(_header->forward[0]);
    }
    delete _header;
    delete _lru_cache;
}

// 递归清理节点
template<typename K, typename V>
void SkipList<K, V>::clear(Node<K, V>* cur) {
    if (cur->forward[0] != nullptr) {
        clear(cur->forward[0]);
    }
    delete cur;
}

// 插入元素到跳表，并将元素插入LRU缓存
template<typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value, time_t expire_time) {
    mtx.lock(); // 加锁
    Node<K, V>* current = this->_header;

    Node<K, V>* update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    // 从最高层开始向下查找插入位置
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    if (current != NULL && current->get_key() == key) {
        std::cout << "key: " << key << ", exists" << std::endl;
        mtx.unlock();
        return 1; // 如果键已存在，返回1
    }

    if (current == NULL || current->get_key() != key) {
        int random_level = get_random_level();

        // 如果随机层级高于当前层级，更新跳表层级
        if (random_level > _skip_list_level) {
            for (int i = _skip_list_level + 1; i < random_level + 1; i++) {
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }

        // 创建新节点并插入跳表
        Node<K, V>* inserted_node = create_node(key, value, random_level, expire_time);
        for (int i = 0; i <= random_level; i++) {
            inserted_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = inserted_node;
        }

        // 插入LRU缓存
        _lru_cache->put(key, value, expire_time);

        std::cout << "Successfully inserted key:" << key << ", value:" << value << std::endl;
        _element_count++;
    }
    mtx.unlock(); // 解锁
    return 0;
}

// 删除元素，并从LRU缓存中移除
template<typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
    mtx.lock(); // 加锁
    Node<K, V>* current = this->_header;
    Node<K, V>* update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];
    if (current != NULL && current->get_key() == key) {
        for (int i = 0; i <= _skip_list_level; i++) {
            if (update[i]->forward[i] != current)
                break;
            update[i]->forward[i] = current->forward[i];
        }

        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0) {
            _skip_list_level--;
        }

        _lru_cache->remove(key); // 从LRU缓存中移除

        std::cout << "Successfully deleted key " << key << std::endl;
        delete current;
        _element_count--;
    }
    mtx.unlock(); // 解锁
}

// 查找元素
template<typename K, typename V>
bool SkipList<K, V>::search_element(K key) {
    std::cout << "search_element-----------------" << std::endl;

    // 先在LRU缓存中查找
    V value;
    if (_lru_cache->get(key, value)) {
        std::cout << "Found key in LRU Cache: " << key << ", value: " << value << std::endl;
        return true;
    }

    // 如果在LRU缓存中找不到，再在跳表中查找
    Node<K, V>* current = _header;

    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if (current && current->get_key() == key) {
        // 在跳表中找到，先输出并插入LRU缓存
        value = current->get_value();
        _lru_cache->put(key, value, current->get_expire_time());
        std::cout << "Found key in Skip List: " << key << ", value: " << value << std::endl;
        return true;
    }

    std::cout << "Not Found Key:" << key << std::endl;
    return false;
}


// 显示跳表内容
template<typename K, typename V>
void SkipList<K, V>::display_list() {
    std::cout << "\n*****Skip List*****" << "\n";
    for (int i = 0; i <= _skip_list_level; i++) {
        Node<K, V>* node = this->_header->forward[i];
        std::cout << "Level " << i << ": ";
        while (node != NULL) {
            std::cout << node->get_key() << ":" << node->get_value() << ";";
            node = node->forward[i];
        }
        std::cout << std::endl;
    }
}

// 保存跳表内容到文件
template<typename K, typename V>
void SkipList<K, V>::dump_file() {
    std::cout << "dump_file-----------------" << std::endl;
    _file_writer.open(STORE_FILE); // 打开文件
    Node<K, V>* node = this->_header->forward[0]; // 从第0层开始遍历

    while (node != NULL) {
        // 保存键、值和过期时间，格式为 key:value:expire_time
        _file_writer << node->get_key() << ":" << node->get_value() << ":" << node->get_expire_time() << "\n";
        std::cout << node->get_key() << ":" << node->get_value() << ":" << node->get_expire_time() << ";\n";
        node = node->forward[0]; // 移动到下一个节点
    }

    _file_writer.flush(); // 刷新缓冲区
    _file_writer.close(); // 关闭文件
}

// 从文件加载跳表内容
template<typename K, typename V>
void SkipList<K, V>::load_file() {
    _file_reader.open(STORE_FILE); // 打开文件
    std::cout << "load_file-----------------" << std::endl;
    std::string line;
    std::string* key = new std::string();
    std::string* value = new std::string();
    std::string* expire_time_str = new std::string();
    time_t now = time(nullptr);

    while (getline(_file_reader, line)) { // 逐行读取
        get_key_value_from_string(line, key, value, expire_time_str); // 解析键值对和过期时间
        if (key->empty() || value->empty() || expire_time_str->empty()) {
            continue;
        }
        time_t expire_time = std::stol(*expire_time_str);
        if (expire_time > now) { // 仅在未过期的情况下插入跳表
            insert_element(stoi(*key), *value, expire_time);
            std::cout << "key:" << *key << " value:" << *value << " expire_time:" << *expire_time_str << std::endl;
        } else {
            std::cout << "Expired key:" << *key << " skipped." << std::endl;
        }
    }

    delete key;
    delete value;
    delete expire_time_str;
    _file_reader.close(); // 关闭文件
}

// 从字符串解析键值对和过期时间
template<typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string& str, std::string* key, std::string* value, std::string* expire_time_str) {
    if (!is_valid_string(str)) {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter) + 1, str.rfind(delimiter) - str.find(delimiter) - 1);
    *expire_time_str = str.substr(str.rfind(delimiter) + 1);
}

// 检查字符串是否有效
template<typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string& str) {
    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}

// 清理过期元素
template<typename K, typename V>
void SkipList<K, V>::evict_expired_items() {
    _lru_cache->evict_expired_items();
}
