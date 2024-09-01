#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define COLUMN_USERNAME_SIZE 32     // 用户名的大小
#define COLUMN_EMAIL_SIZE 255       // 邮箱的大小

/**
 * 存储行的结构体
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
 * 表
 */
typedef struct
{
    uint32_t num_rows;              // 行数
    void *pages[TABLE_MAX_PAGES];   // 页
} Table;

/**
 * 输入缓冲区
 */
typedef struct
{
    char *buffer;           // 输入缓冲区
    size_t buffer_length;   // 缓冲区长度
    ssize_t input_length;   // 输入长度
} InputBuffer;

/**
 * 元命令执行结果
 */
typedef enum
{
    META_COMMAND_SUCCESS,            // 元命令执行成功
    META_COMMAND_UNRECOGNIZED_COMMAND // 未识别的元命令
} MetaCommandResult;

/**
 * 语句识别结果
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
 */
typedef enum
{
    STATEMENT_INSERT,               // 插入语句
    STATEMENT_SELECT                // 查询语句
} StatementType;

/**
 * 语句
 */
typedef struct
{
    StatementType type;             // 语句类型
    Row row_to_insert;              // 插入的行，只有在语句类型为STATEMENT_INSERT时有效
} Statement;

/**
 * 执行结果
 */
typedef enum
{
    EXECUTE_SUCCESS,                // 执行成功
    EXECUTE_TABLE_FULL              // 表已满
} ExecuteResult;

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
 * 获取某一行地址
 * @param table 表
 * @param row_num 行号
 * @return 该行地址
 */
void *row_slot(Table *table, uint32_t row_num)  
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;    // 页号
    void *page = table->pages[page_num];            // 页面起始地址
    if(page == NULL)
    {
        page = table->pages[page_num] = malloc(PAGE_SIZE);  // 分配页的内存空间
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;                  // 行偏移量
    uint32_t byte_offset = row_offset * ROW_SIZE;                   // 字节偏移量
    return page + byte_offset;
}

/**
 * 语句处理
 * @param input_buffer 输入缓冲区
 * @return 元命令执行结果
 */
MetaCommandResult do_meta_command(InputBuffer *input_buffer)
{
    if(strcmp(input_buffer->buffer, ".exit") == 0)
    {
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
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;

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
    Row row;
    for(uint32_t i = 0; i < table->num_rows; i++)
    {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
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
 * 创建表
 * @return 表指针
 */
Table *new_table()
{
    Table *table = (Table *)malloc(sizeof(Table));
    table->num_rows = 0;
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        table->pages[i] = NULL;
    }
    return table;
}

/**
 * 释放表
 * @param table 表
 */
void free_table(Table *table)
{
    for(int i = 0; table->pages[i]; i++)
    {
        free(table->pages[i]);
    }
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
 * 主函数
 * @param argc 参数个数
 * @param argv 参数列表
 */
int main(int argc, char *argv[])
{
    Table *table = new_table();    // 创建表
    InputBuffer *input_buffer = new_input_buffer();    // 创建输入缓冲区

    while(true)
    {
        print_prompt();                 // 打印提示符
        read_input(input_buffer);       // 读取输入

        if(input_buffer->buffer[0] == '.')
        {
            switch(do_meta_command(input_buffer))
            {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'.\n", input_buffer->buffer);    // 打印错误信息
                    continue;
            }
        }

        Statement statement;
        switch(prepare_statement(input_buffer, &statement))
        {
            case (PREPARE_SUCCESS):
                break;
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

        switch(execute_statement(&statement, table))
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