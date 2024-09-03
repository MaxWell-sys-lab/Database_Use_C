#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#define COLUMN_USERNAME_SIZE 32     // 用户名的大小
#define COLUMN_EMAIL_SIZE 255       // 邮箱的大小

/**
 * 存储行的结构体
 * 行是数据库中的基本单元，每一行都有一个id、一个用户名和一个邮箱
 */
typedef struct
{
    int id;
    char username[COLUMN_USERNAME_SIZE + 1];    // 加1是为了存储字符串结束符
    char email[COLUMN_EMAIL_SIZE + 1];          // 加1是为了存储字符串结束符
} Row;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)   // 获取结构体中某个属性的大小
const uint32_t ID_SIZE = size_of_attribute(Row, id);                // id的大小
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);    // username的大小
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);          // email的大小
const uint32_t ID_OFFSET = 0;                                       // id的偏移量
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;               // username的偏移量
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;      // email的偏移量
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;     // 行的大小

const uint32_t PAGE_SIZE = 4096;                                        // 页的大小
#define TABLE_MAX_PAGES 100                                             // 最大页数
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;                    // 每页的行数
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;        // 最大行数

/**
 * 分页器
 * 分页器是一个抽象层，用于管理文件的读写，以及缓存页
 */
typedef struct
{
    int file_descriptor;    // 文件描述符
    uint32_t file_length;   // 文件长度
    void *pages[TABLE_MAX_PAGES];   // 页，用于缓存文件中的数据
} Pager;

/**
 * 表
 * 表是一个抽象层，用于管理行
 */
typedef struct
{
    Pager *pager;                   // 分页器
    uint32_t num_rows;              // 行数
} Table;

/**
 * 游标
 * 游标是一个抽象层，用于遍历表中的行
 */
typedef struct
{
    Table *table;                   // 表
    uint32_t row_num;               // 行号
    bool end_of_table;              // 是否到表尾
} Cursor;

/**
 * 语句
 * 语句用于表示用户输入的语句
 */
typedef struct
{
    StatementType type;             // 语句类型
    Row row_to_insert;              // 插入的行，只有在语句类型为STATEMENT_INSERT时有效
} Statement;

/**
 * 输入缓冲区
 * 输入缓冲区用于存储用户输入
 */
typedef struct
{
    char *buffer;           // 输入缓冲区
    size_t buffer_length;   // 缓冲区长度
    ssize_t input_length;   // 输入长度
} InputBuffer;

/**
 * 元命令执行结果
 * 元命令是一种特殊的命令，用于执行一些特殊的操作，比如退出程序
 */
typedef enum
{
    META_COMMAND_SUCCESS,            // 元命令执行成功
    META_COMMAND_UNRECOGNIZED_COMMAND // 未识别的元命令
} MetaCommandResult;

/**
 * 语句识别结果
 * 语句识别结果用于表示语句的识别结果
 */
typedef enum
{
    PREPARE_SUCCESS,                // 准备成功
    PREPARE_NEGATIVE_ID,            // id为负数
    PREPARE_SYNTAX_TOO_LONG,        // 语法过长
    PREPARE_SYNTAX_ERROR,           // 语法错误
    PREPARE_UNRECOGNIZED_STATEMENT  // 未识别的语句
} PrepareResult;

/**
 * 语句类型
 * 语句类型用于表示语句的类型
 */
typedef enum
{
    STATEMENT_INSERT,               // 插入语句
    STATEMENT_SELECT                // 查询语句
} StatementType;

/**
 * 执行结果
 * 执行结果用于表示执行语句的结果
 */
typedef enum
{
    EXECUTE_SUCCESS,                // 执行成功
    EXECUTE_TABLE_FULL              // 表已满
} ExecuteResult;

void serialize_row(Row *source, void *destination);    // 序列化行
void deserialize_row(void *source, Row *destination);  // 反序列化行
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);    // 语句处理
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);  // 准备插入语句
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);    // 准备语句
void print_row(Row *row);    // 打印行
ExecuteResult execute_insert(Statement *statement, Table *table);    // 执行插入语句
ExecuteResult execute_select(Statement *statement, Table *table);    // 执行查询语句
ExecuteResult execute_statement(Statement *statement, Table *table);    // 执行语句
Table* db_open(const char *filename);    // 打开数据库
Pager* pager_open(const char *filename);    // 打开分页器
void pager_flush(Pager *pager, uint32_t page_num, uint32_t size);    // 刷新分页器
void* get_page(Pager *pager, uint32_t page_num);    // 获取页
void db_close(Table *table);    // 关闭数据库
InputBuffer *new_input_buffer();    // 创建输入缓冲区
void print_prompt();    // 打印提示符
void read_input(InputBuffer *input_buffer);    // 读取输入
void close_input_buffer(InputBuffer *input_buffer);    // 关闭输入缓冲区
Cursor *table_start(Table *table);    // 获取表的起始游标
Cursor *table_end(Table *table);    // 获取表的结束游标
void *cursor_value(Cursor *cursor);    // 获取游标指向的行地址
void cursor_advance(Cursor *cursor);    // 游标前进

/**
 * 序列化行
 * @param source 源行
 * @param destination 目标地址
 */
void serialize_row(Row *source, void *destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

/**
 * 反序列化行
 * @param source 源地址
 * @param destination 目标行
 */
void deserialize_row(void *source, Row *destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/**
 * 获取游标指向的行地址
 * @param cursor 游标
 * @return 行地址
 */
void *cursor_value(Cursor *cursor)
{
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

/**
 * 游标前进
 * @param cursor 游标
 */
void cursor_advance(Cursor *cursor)
{
    cursor->row_num += 1;
    if(cursor->row_num >= cursor->table->num_rows)
    {
        cursor->end_of_table = true;
    }
}

/**
 * 语句处理
 * @param input_buffer 输入缓冲区
 * @param table 表
 * @return 元命令执行结果
 */
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
{
    if(strcmp(input_buffer->buffer, ".exit") == 0)
    {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

/**
 * 准备插入语句
 * @param input_buffer 输入缓冲区
 * @param statement 语句
 * @return 语句识别结果
 */
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{
    statement->type = STATEMENT_INSERT;
    
    char *keyword = strtok(input_buffer->buffer, " ");  // 解析关键字
    char *id_string = strtok(NULL, " ");                // 解析id
    char *username = strtok(NULL, " ");                 // 解析用户名
    char *email = strtok(NULL, " ");                    // 解析邮箱

    if(id_string == NULL || username == NULL || email == NULL)
    {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if(id < 0)
    {
        return PREPARE_NEGATIVE_ID;
    }
    if(strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE)
    {
        return PREPARE_SYNTAX_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

/**
 * 准备语句
 * @param input_buffer 输入缓冲区
 * @param statement 语句
 * @return 语句识别结果
 */
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    if(strncmp(input_buffer->buffer, "insert", 6) == 0) // 因为insert语句会包含其他字符，所以只需要比较前6个字符
    {
        return prepare_insert(input_buffer, statement);
    }
    if(strcmp(input_buffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * 打印行
 * @param row 行
 */
void print_row(Row *row)
{
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

/**
 * 执行插入语句
 * @param statement 语句
 * @param table 表
 * @return 执行结果
 */
ExecuteResult execute_insert(Statement *statement, Table *table)
{
    if(table->num_rows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement->row_to_insert);
    Cursor *cursor = table_end(table);
    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows += 1;

    free(cursor);

    return EXECUTE_SUCCESS;
}

/**
 * 执行查询语句
 * @param statement 语句
 * @param table 表
 * @return 执行结果
 */
ExecuteResult execute_select(Statement *statement, Table *table)
{
    Cursor *cursor = table_start(table);

    Row row;
    while(cursor->end_of_table != true)
    {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    return EXECUTE_SUCCESS;
}

/**
 * 执行语句
 * @param statement 语句
 * @param table 表
 * @return 执行结果
 */
ExecuteResult execute_statement(Statement *statement, Table *table)
{
    switch(statement->type)
    {
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
    }
}

/**
 * 打开数据库
 * @param filename 文件名
 * @return 表
 */
Table* db_open(const char *filename)
{
    Pager *pager = pager_open(filename);    // 打开分页器
    uint32_t num_rows = pager->file_length / ROW_SIZE;    // 计算行数

    Table* table = (Table *)malloc(sizeof(Table));    // 分配表内存空间
    table->pager = pager;    // 设置分页器
    table->num_rows = num_rows;    // 设置行数

    return table;
}

/**
 * 打开分页器
 * @param filename 文件名
 * @return 分页器
 */
Pager* pager_open(const char *filename)
{
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);    // 打开文件
    if(fd == -1)
    {
        printf("Unable to open file\n");    // 打印错误信息
        exit(EXIT_FAILURE);    // 退出程序
    }

    off_t file_length = lseek(fd, 0, SEEK_END);    // 获取文件长度

    Pager *pager = (Pager *)malloc(sizeof(Pager));    // 分配分页器内存空间
    pager->file_descriptor = fd;    // 设置文件描述符
    pager->file_length = file_length;    // 设置文件长度

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }

    return pager;
}

/**
 * 刷新分页器
 * @param pager 分页器
 * @param page_num 页号
 */
void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{
    if(pager->pages[page_num] == NULL)
    {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if(offset == -1)
    {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if(bytes_written == -1)
    {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

/**
 * 获取页
 * @param pager 分页器
 * @param page_num 页号
 * @return 页
 */
void* get_page(Pager *pager, uint32_t page_num)
{
    if(page_num > TABLE_MAX_PAGES)
    {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);    // 打印错误信息
        exit(EXIT_FAILURE);    // 退出程序
    }

    if(pager->pages[page_num] == NULL)
    {
        // 缓存未命中，分配内存空间
        void *page = malloc(PAGE_SIZE);    // 分配页内存空间
        uint32_t num_pages = pager->file_length / PAGE_SIZE;    // 计算页数

        // 为了保证文件长度是PAGE_SIZE的整数倍
        if(pager->file_length % PAGE_SIZE)
        {
            num_pages += 1;
        }

        if(page_num <= num_pages)
        {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);    // 设置文件偏移量
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);    // 读取文件
            if(bytes_read == -1)
            {
                printf("Error reading file: %d\n", errno);    // 打印错误信息
                exit(EXIT_FAILURE);    // 退出程序
            }
        }

        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

/**
 * 关闭数据库
 * @param table 表
 */
void db_close(Table *table)
{
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    // 刷新并释放所有已使用的页
    for(uint32_t i = 0; i < num_full_pages; i++)
    {
        if(pager->pages[i] == NULL)
        {
            continue;
        }

        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // 刷新并释放额外的页
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if(num_additional_rows > 0)
    {
        uint32_t page_num = num_full_pages;
        if(pager->pages[page_num] != NULL)
        {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    // 关闭文件描述符
    int result = close(pager->file_descriptor);
    if(result == -1)
    {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    // 释放所有页的内存空间
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void *page = pager->pages[i];
        if(page)
        {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    // 释放分页器和表的内存空间
    free(pager);
    free(table);
}

/**
 * 创建输入缓冲区
 * @return 输入缓冲区指针
 */
InputBuffer *new_input_buffer()
{
    InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));   // 分配输入缓冲区的内存空间
    input_buffer->buffer = NULL;        // 初始化缓冲区指针为空
    input_buffer->buffer_length = 0;    // 初始化缓冲区长度为0
    input_buffer->input_length = 0;     // 初始化输入长度为0

    return input_buffer;                // 返回输入缓冲区指针
}

/**
 * 打印提示符
 */
void print_prompt()
{
    printf("db > ");
}

/**
 * 读取输入
 * @param input_buffer 输入缓冲区
 */
void read_input(InputBuffer *input_buffer)
{
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);    // 从标准输入读取一行输入，并将结果存储在输入缓冲区中

    if(bytes_read <= 0)
    {
        printf("Error reading input\n");    // 如果读取失败，则打印错误信息
        exit(EXIT_FAILURE);    // 退出程序
    }

    input_buffer->input_length = bytes_read - 1;    // 计算输入长度
    input_buffer->buffer[bytes_read - 1] = 0;    // 在输入缓冲区末尾添加字符串结束符
}

/**
 * 关闭输入缓冲区
 * @param input_buffer 输入缓冲区
 */
void close_input_buffer(InputBuffer *input_buffer)
{
    free(input_buffer->buffer);    // 释放输入缓冲区的内存空间
    free(input_buffer);    // 释放输入缓冲区结构体的内存空间
}

/**
 * 获取表的起始游标
 * @param table 表
 * @return 游标
 */
Cursor *table_start(Table *table)
{
    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));    // 分配游标内存空间
    cursor->table = table;    // 设置表
    cursor->row_num = 0;    // 设置行号
    cursor->end_of_table = (table->num_rows == 0);    // 设置是否到表尾

    return cursor;    // 返回游标
}

/**
 * 获取表的结束游标
 * @param table 表
 * @return 游标
 */
Cursor *table_end(Table *table)
{
    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));    // 分配游标内存空间
    cursor->table = table;    // 设置表
    cursor->row_num = table->num_rows;    // 设置行号
    cursor->end_of_table = true;    // 设置是否到表尾

    return cursor;    // 返回游标
}

/**
 * 主函数
 * @param argc 参数个数
 * @param argv 参数列表
 */
int main(int argc, char *argv[])
{
    if(argc < 2)
    {
        printf("Must supply a database filename.\n");    // 打印错误信息
        exit(EXIT_FAILURE);    // 退出程序
    }

    char *filename = argv[1];    // 获取数据库文件名
    Table *table = db_open(filename);    // 打开数据库

    InputBuffer *input_buffer = new_input_buffer();    // 创建输入缓冲区

    while(true)
    {
        print_prompt();                 // 打印提示符
        read_input(input_buffer);       // 读取输入

        if(input_buffer->buffer[0] == '.')  // 判断是否为元命令
        {
            switch(do_meta_command(input_buffer, table))
            {
                case (META_COMMAND_SUCCESS):
                    continue;   // 元命令执行成功，继续下一次循环，回到打印提示符
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'.\n", input_buffer->buffer);    // 打印错误信息
                    continue;   // 元命令未识别，继续下一次循环，回到打印提示符
            }
        }

        Statement statement;
        switch(prepare_statement(input_buffer, &statement))   // 判断语句类型
        {
            case (PREPARE_SUCCESS):
                break;  // 准备成功，继续下一步，退出switch
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");    // 打印错误信息
                continue;
            case (PREPARE_SYNTAX_TOO_LONG):
                printf("String is too long.\n");    // 打印错误信息
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");    // 打印错误信息
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);    // 打印错误信息
                continue;
        }

        switch(execute_statement(&statement, table))    // 执行语句
        {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");    // 打印执行成功信息
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");    // 打印错误信息
                break;
        }
    }
}