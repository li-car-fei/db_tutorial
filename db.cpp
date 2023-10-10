#include <cerrno>      // 错误码相关
#include <fcntl.h>     // 文件控制
#include <cstdbool>    // 布尔类型
#include <cstdio>      // 标准输入输出
#include <cstdlib>     // 通用工具函数
#include <cstring>     // 字符串操作
#include <unistd.h>    // POSIX API
#include <cstdint>     // 固定宽度整数类型
#include <sys/stat.h>  // 文件状态
#include <iostream>
#include <vector>

// 处理普通命令的结果状态
typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
  EXECUTE_TABLE_FULL
} ExecuteResult;

// 输入流的定义
typedef struct
{
    std::string content;
    std::size_t length;
} InputRecord;

// 新建一个InputRecord，所有的输入共用它
InputRecord* new_input_record(){
    return new InputRecord;
}

// 程序结束时要 delete 掉一开始new的对象
void Destroy_input_record(InputRecord* input_order){ delete input_order; }

// 处理输入命令的函数，输入一个 InputOrder*
void read_input(InputRecord* input_order){
    std::cout << "Enter a string for database order: ";
    std::getline(std::cin, input_order->content);
    input_order->length = input_order->content.length();
}

// 特殊命令识别并执行的状态
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAN
} MetaCommandResult;

// 特殊命令的识别与执行
MetaCommandResult do_meta_command(InputRecord* input_record){
    if(input_record->content.find('.exit') != std::string::npos){
        // 退出数据库
        std::cout << "close the database" << std::endl;
        return META_COMMAND_SUCCESS;
    }else if (input_record->content.find('.btree') != std::string::npos)
    {
        // 查看 btree
        std::cout << "show the btree" << std::endl;
        return META_COMMAND_SUCCESS;
    }else if (input_record->content.find('.constants') != std::string::npos)
    {
        // 查看 constants 数据库相关的常量
        std::cout << "show the constants" << std::endl;
        print_constants();
        return META_COMMAND_SUCCESS;
    }else{
        return META_COMMAND_UNRECOGNIZED_COMMAN;
    }
    
    
}

// 识别普通命令的结果
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

// 普通命令的 statement 类型，只支持 insert 和 select
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

// 暂时定义一个数据行，只有 username 和 email
const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t COLUMN_EMAIL_SIZE = 255;
typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

// 打印一行的数据
void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// 计算结构中某个属性所需占的内存大小
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

// 每个数据项所占内存
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;     // 一行数据所占

const uint32_t PAGE_SIZE = 4096;                                    // 一页所占内存
const uint32_t TABLE_MAX_PAGES = 100;                               // 页的数量

// Peger 定义
typedef struct{
    int file_descriptor;    // 数据库标记
    uint32_t file_length;   // 数据库大小
    uint32_t num_pages;     // page页的数量
    void* pages[TABLE_MAX_PAGES];   // 指向每个页的指针数组
} Pager;

// Table 定义
typedef struct {
    Pager* pager;           // Pager 指针，用于页相关操作
    uint32_t root_page_num;
} Table;

// Cursor 定义
typedef struct {
    Table* table;           // 当前cursor查询的table
    uint32_t page_num;      // 当前指向的页号
    uint32_t cell_num;      // 页内的偏移，这里是指定第几项元素（行）
    bool end_of_table;      // 是否指向表的末尾
} Cursor;

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

/*
 * Common Node Header Layout
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Internal Node Header Layout
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
/* Keep this small for testing */
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;


/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

/*
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;


NodeType get_node_type(void* node) {
  uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
  uint8_t value = type;
  *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root(void* node) {
  uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
  return (bool)value;
}

void set_node_root(void* node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* node_parent(void* node) { return static_cast<uint32_t*>(node + PARENT_POINTER_OFFSET); }

uint32_t* internal_node_num_keys(void* node) {
  return static_cast<uint32_t*>(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* internal_node_right_child(void* node) {
  return static_cast<uint32_t*>(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
  return static_cast<uint32_t*>(node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    return internal_node_right_child(node);
  } else {
    return internal_node_cell(node, child_num);
  }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
  return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t* leaf_node_num_cells(void* node) {
  return static_cast<uint32_t*>(node + LEAF_NODE_NUM_CELLS_OFFSET);
}

uint32_t* leaf_node_next_leaf(void* node) {
  return static_cast<uint32_t*>(node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return static_cast<uint32_t*>(node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE);
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return static_cast<uint32_t*>(leaf_node_cell(node, cell_num));
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t get_node_max_key(void* node) {
  switch (get_node_type(node)) {
    case NODE_INTERNAL:
      return *internal_node_key(node, *internal_node_num_keys(node) - 1);
    case NODE_LEAF:
      return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
}

// 打印一些常量
void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

// 获取指定页的指针
void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss. Allocate memory and load from file.
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // We might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}


void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

// 打印 b+ 树
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
  void* node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      num_keys = *leaf_node_num_cells(node);
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level + 1);
        printf("- %d\n", *leaf_node_key(node, i));
      }
      break;
    case (NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        child = *internal_node_child(node, i);
        print_tree(pager, child, indentation_level + 1);

        indent(indentation_level + 1);
        printf("- key %d\n", *internal_node_key(node, i));
      }
      child = *internal_node_right_child(node);
      print_tree(pager, child, indentation_level + 1);
      break;
  }
}


void serialize_row(Row* source, void* destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void initialize_leaf_node(void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

void initialize_internal_node(void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = new Cursor;
  cursor->table = table;
  cursor->page_num = page_num;

  // Binary search
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key == key_at_index) {
      cursor->cell_num = index;
      return cursor;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor->cell_num = min_index;
  return cursor;
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
  /*
  Return the index of the child which should contain
  the given key.
  */

  uint32_t num_keys = *internal_node_num_keys(node);

  /* Binary search */
  uint32_t min_index = 0;
  uint32_t max_index = num_keys; /* there is one more child than key */

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);

  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void* child = get_page(table->pager, child_num);
  switch (get_node_type(child)) {
    case NODE_LEAF:
      return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL:
      return internal_node_find(table, child_num, key);
  }
}

/*
Return the position of the given key.
If the key is not present, return the position
where it should be inserted
*/
Cursor* table_find(Table* table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  void* root_node = get_page(table->pager, root_page_num);

  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    return internal_node_find(table, root_page_num, key);
  }
}

Cursor* table_start(Table* table) {
  Cursor* cursor = table_find(table, 0);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

void* cursor_value(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    /* Advance to next leaf node */
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      /* This was rightmost leaf */
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}

Pager* pager_open(const char* filename) {
  int fd = open(filename,
                O_RDWR |      // Read/Write mode
                    O_CREAT,  // Create file if it does not exist
                S_IWUSR |     // User write permission
                    S_IRUSR   // User read permission
                );

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = new Pager;
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  if (file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);

  Table* table = new Table;
  table->pager = pager;

  if (pager->num_pages == 0) {
    // New database file. Initialize page 0 as leaf node.
    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }

  return table;
}



// 普通命令识别出的 Statement，如果是 insert就会有要插入的 row 数据
typedef struct {
  StatementType type;
  Row row_to_insert;  // only used by insert statement
} Statement;

// 为insert操作做准备，从输入中取出 id 等数据
PrepareResult prepare_insert(InputRecord* input_order, Statement* statement){
    statement->type = STATEMENT_INSERT;

    std::vector<std::string> tokens;
    std::size_t start = 0;
    std::size_t end = input_order->content.find(' ');
    while (end != std::string::npos)
    {
        tokens.emplace_back(input_order->content.substr(start, end-start));
        start = end + 1;
        end = input_order->content.find(' ', start);
    }

    std::string id_string = tokens[1];
    std::string username = tokens[2];
    std::string email = tokens[3];

    int id = std::stoi(id_string);
    if(id < 0) return PREPARE_NEGATIVE_ID;

    if(username.length() > COLUMN_USERNAME_SIZE || email.length() > COLUMN_EMAIL_SIZE) return PREPARE_STRING_TOO_LONG;

    statement->row_to_insert.id = id;
    std::strcpy(statement->row_to_insert.username, username.c_str());
    std::strcpy(statement->row_to_insert.email, email.c_str());

    return PREPARE_SUCCESS;
}

// 识别普通命令的函数
PrepareResult prepare_statement(InputRecord* input_order, Statement* statement){
    if (input_order->content.compare(0, 6, "insert") == 0)
    {
        // 后面要执行insert操作
        // 需要继续从 input_order->content 中取出插入的位置、插入的数据
        return prepare_insert(input_order, statement);
    }
    if (input_order->content.compare(0, 6, "select") == 0)
    {
        // 后面要执行select操作
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}




int main(int argc, char* argv[]){
    // if (argc < 2)
    // {   
    //     std::cout << "Must supply a database filename.\n" << std::endl;
    //     exit(EXIT_FAILURE);
    // }
    
    // char *file_name = argv[1];
    char file_name[] = "test_db.txt";

    InputRecord* input_order = new_input_record();
    while(true){
        read_input(input_order);

        // 处理特殊命令 . 开头的命令
        if(input_order->content[0] == '.'){
            switch (do_meta_command(input_order))
            {
            case (META_COMMAND_SUCCESS):
                continue;               // 执行正常，继续程序循环
            
            case (META_COMMAND_UNRECOGNIZED_COMMAN):
                std::cout << "Unrecognized command: " << input_order->content << std::endl;
                continue;               // 命令识别错误，但是先继续监听命令，不退出
            }
        }

        // 处理普通的 CRUD 的命令
        Statement statement;
        switch (prepare_statement(input_order, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword.\n");
                continue;
        }
        
    }

    // delete 掉一开始 new 的 InputRecord* input_order
    Destroy_input_record(input_order);
}