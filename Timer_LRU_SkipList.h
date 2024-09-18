#include <iostream>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <mutex>
#include <fstream>
#include <unordered_map>
#include <list>
#include <ctime>
#include <thread>
#include <chrono>
#include <functional>
#include <atomic>

#define STORE_FILE "store/dumpFile" // 文件路径，用于存储跳表数据

std::mutex mtx; // 全局互斥锁，保证线程安全
std::string delimiter = ":"; // 定义用于解析键值对的分隔符

// 节点类模板，表示跳表中的每个节点
template<typename K, typename V>
class Node {
public:
    Node() {}

    // 构造函数，初始化键、值、层数、过期时间
    Node(K k, V v, int level, time_t expire_time = 0);

    // 析构函数，释放指针数组
    ~Node();

    // 获取节点的键
    K get_key() const;

    // 获取节点的值
    V get_value() const;

    // 获取节点的过期时间
    time_t get_expire_time() const;

    // 设置节点的值
    void set_value(V value);

    // 设置节点的过期时间
    void set_expire_time(time_t expire_time);

    // 存储指向下一个节点的指针数组，每层一个指针
    Node<K, V> **forward;

    // 当前节点所处的层级
    int node_level;

private:
    K key; // 键
    V value; // 值
    time_t expire_time; // 过期时间
};

// Node类的构造函数实现
template<typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level, time_t expire_time) {
    this->key = k;
    this->value = v;
    this->node_level = level;
    this->expire_time = expire_time;

    // 创建指向下一个节点的指针数组
    this->forward = new Node<K, V>*[level + 1];

    // 将指针数组初始化为空指针
    memset(this->forward, 0, sizeof(Node<K, V>*) * (level + 1));
}

// Node类的析构函数，释放指针数组
template<typename K, typename V>
Node<K, V>::~Node() {
    delete[] forward;
}

// 获取节点的键
template<typename K, typename V>
K Node<K, V>::get_key() const {
    return key;
}

// 获取节点的值
template<typename K, typename V>
V Node<K, V>::get_value() const {
    return value;
}

// 获取节点的过期时间
template<typename K, typename V>
time_t Node<K, V>::get_expire_time() const {
    return expire_time;
}

// 设置节点的值
template<typename K, typename V>
void Node<K, V>::set_value(V value) {
    this->value = value;
}

// 设置节点的过期时间
template<typename K, typename V>
void Node<K, V>::set_expire_time(time_t expire_time) {
    this->expire_time = expire_time;
}

// LRU缓存类模板，用于存储最近使用的键值对
template<typename K, typename V>
class LRUCache {
public:
    LRUCache(size_t capacity); // 构造函数，初始化缓存容量
    ~LRUCache(); // 析构函数，释放缓存

    bool get(const K& key, V& value); // 获取指定键的值
    void put(const K& key, const V& value, time_t expire_time = 0); // 插入键值对
    void remove(const K& key); // 删除指定键
    void evict_expired_items(); // 清理过期的缓存项

private:
    size_t capacity; // 缓存容量
    std::list<std::pair<K, Node<K, V>*>> item_list; // 双向链表，存储缓存项
    std::unordered_map<K, typename std::list<std::pair<K, Node<K, V>*>>::iterator> item_map; // 哈希表，用于快速查找
};

// 构造函数，初始化LRU缓存的容量
template<typename K, typename V>
LRUCache<K, V>::LRUCache(size_t capacity) : capacity(capacity) {}

// 析构函数，清理所有缓存项
template<typename K, typename V>
LRUCache<K, V>::~LRUCache() {
    for (auto& item : item_list) {
        delete item.second; // 释放节点内存
    }
}

// 获取指定键的值，如果找到则返回true并更新链表顺序
template<typename K, typename V>
bool LRUCache<K, V>::get(const K& key, V& value) {
    auto it = item_map.find(key);
    if (it == item_map.end()) {
        return false; // 未找到指定键
    }

    Node<K, V>* node = it->second->second; // 获取节点
    time_t now = time(nullptr); // 获取当前时间

    // 如果节点已过期，则删除它
    if (node->get_expire_time() != 0 && node->get_expire_time() <= now) {
        item_list.erase(it->second);
        delete node;
        item_map.erase(it);
        return false;
    }

    value = node->get_value(); // 获取节点的值
    item_list.splice(item_list.begin(), item_list, it->second); // 将节点移到链表头部
    return true;
}

// 插入键值对到缓存，如果缓存已满则删除最久未使用的项
template<typename K, typename V>
void LRUCache<K, V>::put(const K& key, const V& value, time_t expire_time) {
    auto it = item_map.find(key);
    if (it != item_map.end()) {
        // 如果键已存在，更新值并移动到链表头部
        item_list.splice(item_list.begin(), item_list, it->second);
        it->second->second->set_value(value);
        it->second->second->set_expire_time(expire_time);
    } else {
        // 如果缓存已满，删除链表尾部的节点
        if (item_list.size() >= capacity) {
            auto last = item_list.end();
            last--;
            item_map.erase(last->first);
            delete last->second;
            item_list.pop_back();
        }
        // 创建新节点并插入到链表头部
        Node<K, V>* node = new Node<K, V>(key, value, 0, expire_time);
        item_list.push_front(std::make_pair(key, node));
        item_map[key] = item_list.begin();
    }
}

// 删除指定键的缓存项
template<typename K, typename V>
void LRUCache<K, V>::remove(const K& key) {
    auto it = item_map.find(key);
    if (it != item_map.end()) {
        item_list.erase(it->second); // 从链表中删除
        delete it->second->second; // 释放节点
        item_map.erase(it); // 从哈希表中删除
    }
}

// 清理已过期的缓存项
template<typename K, typename V>
void LRUCache<K, V>::evict_expired_items() {
    time_t now = time(nullptr);
    for (auto it = item_list.begin(); it != item_list.end();) {
        if (it->second->get_expire_time() != 0 && it->second->get_expire_time() <= now) {
            auto erase_it = it++;
            item_map.erase(erase_it->first);
            delete erase_it->second;
            item_list.erase(erase_it);
        } else {
            ++it;
        }
    }
}

// 定时器类，用于执行定期任务
class Timer {
public:
    Timer() : _execute(false) {} // 构造函数，初始化定时器

    // 启动定时器，interval为定时器间隔时间，func为定时调用的函数
    void start(int interval, std::function<void()> func) {
        _execute = true;
        _thread = std::thread([this, interval, func]() {
            while (_execute) {
                std::this_thread::sleep_for(std::chrono::milliseconds(interval));
                func(); // 定时执行任务
            }
        });
    }

    // 停止定时器
    void stop() {
        _execute = false;
        if (_thread.joinable()) {
            _thread.join();
        }
    }

    ~Timer() {
        stop(); // 析构函数，确保定时器停止
    }

private:
    std::atomic<bool> _execute; // 控制定时器是否运行
    std::thread _thread; // 定时器线程
};

// 跳表类模板
template<typename K, typename V>
class SkipList {
public:
    // 构造函数，初始化最大层级、LRU缓存容量、定时器间隔时间
    SkipList(int max_level, size_t lru_capacity, int interval = 60000);

    // 析构函数，清理资源
    ~SkipList();

    int insert_element(K, V, time_t expire_time = 0); // 插入元素
    void delete_element(K); // 删除元素
    bool search_element(K); // 查找元素
    void display_list(); // 显示跳表内容
    void dump_file(); // 将跳表内容保存到文件
    void load_file(); // 从文件加载跳表内容
    void evict_expired_items(); // 清理过期元素
    int size(); // 获取跳表大小

private:
    int get_random_level(); // 随机获取层数
    Node<K, V>* create_node(K, V, int, time_t expire_time = 0); // 创建新节点
    void periodic_task(); // 定时执行的任务
    void clear(Node<K, V>*); // 递归删除节点

private:
    int _max_level; // 跳表的最大层级
    int _skip_list_level; // 当前跳表的层级
    int _element_count; // 元素数量
    Node<K, V>* _header; // 跳表的头节点
    LRUCache<K, V>* _lru_cache; // LRU缓存
    Timer _timer; // 定时器
    std::ofstream _file_writer; // 文件写对象
    std::ifstream _file_reader; // 文件读对象
};

// 创建新节点
template<typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(const K k, const V v, int level, time_t expire_time) {
    Node<K, V>* n = new Node<K, V>(k, v, level, expire_time);
    return n;
}

// 获取随机层数，用于确定新插入节点的层级
template<typename K, typename V>
int SkipList<K, V>::get_random_level() {
    int k = 1;
    while (rand() % 2) {
        k++;
    }
    return (k < _max_level) ? k : _max_level;
}

// 构造函数，初始化跳表和LRU缓存，并启动定时器
template<typename K, typename V>
SkipList<K, V>::SkipList(int max_level, size_t lru_capacity, int interval)
    : _max_level(max_level), _skip_list_level(0), _element_count(0) {
    _header = new Node<K, V>(K(), V(), _max_level); // 创建头节点
    _lru_cache = new LRUCache<K, V>(lru_capacity); // 初始化LRU缓存
    _timer.start(interval, std::bind(&SkipList::periodic_task, this)); // 启动定时器
}

// 析构函数，停止定时器并清理资源
template<typename K, typename V>
SkipList<K, V>::~SkipList() {
    _timer.stop(); // 停止定时器
    if (_file_writer.is_open()) {
        _file_writer.close(); // 关闭文件写入流
    }
    if (_file_reader.is_open()) {
        _file_reader.close(); // 关闭文件读取流
    }
    if (_header->forward[0] != nullptr) {
        clear(_header->forward[0]); // 递归删除所有节点
    }
    delete _header; // 删除头节点
    delete _lru_cache; // 删除LRU缓存
}

// 递归删除节点
template<typename K, typename V>
void SkipList<K, V>::clear(Node<K, V>* cur) {
    if (cur->forward[0] != nullptr) {
        clear(cur->forward[0]);
    }
    delete cur;
}

// 插入元素到跳表
template<typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value, time_t expire_time) {
    mtx.lock(); // 加锁，保证线程安全
    Node<K, V>* current = _header;
    Node<K, V>* update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    // 从最高层向下查找插入位置
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    // 如果键已存在，打印信息并返回
    if (current != nullptr && current->get_key() == key) {
        std::cout << "Key: " << key << " already exists\n";
        mtx.unlock();
        return 1;
    }

    // 如果键不存在，生成随机层级并插入新节点
    if (current == nullptr || current->get_key() != key) {
        int random_level = get_random_level();
        if (random_level > _skip_list_level) {
            for (int i = _skip_list_level + 1; i < random_level + 1; i++) {
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }

        // 插入新节点
        Node<K, V>* inserted_node = create_node(key, value, random_level, expire_time);
        for (int i = 0; i <= random_level; i++) {
            inserted_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = inserted_node;
        }

        // 将新节点插入LRU缓存
        _lru_cache->put(key, value, expire_time);
        std::cout << "Successfully inserted key: " << key << ", value: " << value << std::endl;
        _element_count++;
    }
    mtx.unlock(); // 解锁
    return 0;
}

// 删除元素
template<typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
    mtx.lock(); // 加锁
    Node<K, V>* current = _header;
    Node<K, V>* update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    // 从最高层向下查找删除位置
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    // 如果找到要删除的键，则删除节点
    if (current != nullptr && current->get_key() == key) {
        for (int i = 0; i <= _skip_list_level; i++) {
            if (update[i]->forward[i] != current)
                break;
            update[i]->forward[i] = current->forward[i];
        }

        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == nullptr) {
            _skip_list_level--;
        }

        // 从LRU缓存中移除该节点
        _lru_cache->remove(key);
        std::cout << "Successfully deleted key: " << key << std::endl;
        delete current;
        _element_count--;
    }
    mtx.unlock(); // 解锁
}

// 查找元素
template<typename K, typename V>
bool SkipList<K, V>::search_element(K key) {
    std::cout << "search_element-----------------\n";

    // 先从LRU缓存中查找
    V value;
    if (_lru_cache->get(key, value)) {
        std::cout << "Found key in LRU Cache: " << key << ", value: " << value << std::endl;
        return true;
    }

    // 如果缓存中没有，则在跳表中查找
    Node<K, V>* current = _header;

    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    // 如果在跳表中找到，输出结果并将其插入LRU缓存
    if (current && current->get_key() == key) {
        value = current->get_value();
        _lru_cache->put(key, value, current->get_expire_time());
        std::cout << "Found key in Skip List: " << key << ", value: " << value << std::endl;
        return true;
    }

    std::cout << "Not Found Key: " << key << std::endl;
    return false;
}

// 显示跳表内容
template<typename K, typename V>
void SkipList<K, V>::display_list() {
    std::cout << "\n*****Skip List*****\n";
    for (int i = 0; i <= _skip_list_level; i++) {
        Node<K, V>* node = _header->forward[i];
        std::cout << "Level " << i << ": ";
        while (node != nullptr) {
            std::cout << node->get_key() << ":" << node->get_value() << "; ";
            node = node->forward[i];
        }
        std::cout << std::endl;
    }
}

// 将跳表内容保存到文件
template<typename K, typename V>
void SkipList<K, V>::dump_file() {
    std::cout << "dump_file-----------------\n";
    _file_writer.open(STORE_FILE);
    Node<K, V>* node = _header->forward[0];

    // 遍历跳表，将键值对写入文件
    while (node != nullptr) {
        _file_writer << node->get_key() << ":" << node->get_value() << "\n";
        std::cout << node->get_key() << ":" << node->get_value() << ";\n";
        node = node->forward[0];
    }

    _file_writer.flush(); // 刷新缓冲区
    _file_writer.close(); // 关闭文件
}

// 清理过期元素
template<typename K, typename V>
void SkipList<K, V>::evict_expired_items() {
    _lru_cache->evict_expired_items(); // 调用LRU缓存的清理方法
}

// 定时执行的任务：清理过期元素并存盘
template<typename K, typename V>
void SkipList<K, V>::periodic_task() {
    std::lock_guard<std::mutex> lock(mtx); // 加锁，避免与其他操作冲突
    std::cout << "Performing periodic cleanup and dump...\n";
    _lru_cache->evict_expired_items(); // 清理过期键值对
    dump_file(); // 存盘
}

