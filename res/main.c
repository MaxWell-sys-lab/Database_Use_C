#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

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
} Statement;

/**
 * 语句处理
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
 * 准备语句
 * @param input_buffer 输入缓冲区
 * @param statement 语句
 * @return 语句识别结果
 */
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    if(strncmp(input_buffer->buffer, "insert", 6) == 0) // 因为insert语句会包含其他字符，所以只需要比较前6个字符
    {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if(strcmp(input_buffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * 执行语句
 * @param statement 语句
 */
void execute_statement(Statement *statement)
{
    switch(statement->type)
    {
        case STATEMENT_INSERT:
            printf("This is where we would do an insert.\n");
            break;
        case STATEMENT_SELECT:
            printf("This is where we would do a select.\n");
            break;
    }
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
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);    // 打印错误信息
                continue;
        }

        execute_statement(&statement);
        printf("Executed.\n");
    }
}