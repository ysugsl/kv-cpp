#include <iostream>
#include <ctime>
#include "LRU_skiplist.h"
#define FILE_PATH "./store/dumpFile"

int main() {
    // 创建一个跳表对象，最大层数为5，LRU缓存的容量为3
    SkipList<int, std::string> skipList(5, 3);

    // 插入一些数据，设置不同的过期时间
    std::cout << "Inserting data..." << std::endl;
    skipList.insert_element(1, "data1", time(nullptr) + 10);  // 10秒后过期
    skipList.insert_element(2, "data2", 0);  // 永不过期
    skipList.insert_element(3, "data3", time(nullptr) + 5);   // 5秒后过期
    skipList.insert_element(4, "data4", time(nullptr) + 15);  // 15秒后过期

    // 显示跳表内容
    std::cout << "\nDisplaying SkipList after insertions:" << std::endl;
    skipList.display_list();

    // 测试查找功能
    std::cout << "\nSearching for data with key 2 (should be found):" << std::endl;
    if (skipList.search_element(2)) {
        std::cout << "Key 2 found in SkipList." << std::endl;
    } else {
        std::cout << "Key 2 not found in SkipList." << std::endl;
    }

    // 等待6秒后再查找已过期的数据
    std::cout << "\nWaiting 6 seconds for some data to expire..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(6));

    std::cout << "\nSearching for data with key 3 (should be expired):" << std::endl;
    if (skipList.search_element(3)) {
        std::cout << "Key 3 found in SkipList." << std::endl;
    } else {
        std::cout << "Key 3 not found (expired or not present)." << std::endl;
    }

    // 显示跳表内容
    std::cout << "\nDisplaying SkipList after expiration:" << std::endl;
    skipList.display_list();

    // 删除数据
    std::cout << "\nDeleting data with key 2..." << std::endl;
    skipList.delete_element(2);

    // 显示跳表内容
    std::cout << "\nDisplaying SkipList after deletion:" << std::endl;
    skipList.display_list();

    return 0;
}
