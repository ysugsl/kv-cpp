#include "Timer_LRU_SkipList.h"

int main() {
    // 创建一个跳表，最大层数为10，LRU缓存容量为100，定时任务每60秒执行一次
    SkipList<int, std::string> skip_list(10, 100, 60000);

    // 插入100个元素，每个元素的过期时间不同
    for (int i = 1; i <= 100; ++i) {
        // 将每个元素的过期时间设置为当前时间 + i 秒，方便观察逐渐过期
        skip_list.insert_element(i, "value" + std::to_string(i), time(nullptr) + i);
    }

    // 显示初始跳表内容
    std::cout << "Initial Skip List with 100 elements inserted: " << std::endl;
    skip_list.display_list();

    // 查找一些元素，测试查找功能
    for (int i = 10; i <= 50; i += 10) {
        if (skip_list.search_element(i)) {
            std::cout << "Found key " << i << " in skip list." << std::endl;
        } else {
            std::cout << "Key " << i << " not found." << std::endl;
        }
    }

    // 等待一段时间，模拟部分元素过期
    std::cout << "Sleeping for 30 seconds to allow some items to expire..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));

    // 清理过期项
    skip_list.evict_expired_items();

    // 显示跳表内容，过期元素应被清理
    std::cout << "Skip List after cleaning expired elements: " << std::endl;
    skip_list.display_list();

    // 删除一些元素
    skip_list.delete_element(5);
    skip_list.delete_element(20);
    skip_list.delete_element(50);

    std::cout << "Skip List after deleting keys 5, 20, and 50: " << std::endl;
    skip_list.display_list();

    // 再次插入新的元素，覆盖之前的一些键
    for (int i = 90; i <= 100; ++i) {
        skip_list.insert_element(i, "new_value" + std::to_string(i), time(nullptr) + 60); // 60秒后过期
    }

    std::cout << "Skip List after reinserting some keys with updated values: " << std::endl;
    skip_list.display_list();

    // 等待一段时间，确保定期任务（清理过期项和存盘）运行
    std::cout << "Sleeping for 1 minute to observe the periodic task..." << std::endl;
    std::this_thread::sleep_for(std::chrono::minutes(1));

    // 再次显示跳表内容，检查自动清理效果
    std::cout << "Final Skip List: " << std::endl;
    skip_list.display_list();

    return 0;
}

