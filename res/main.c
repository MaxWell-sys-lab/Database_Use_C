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

        if(strcmp(input_buffer->buffer, ".exit") == 0)    // 如果输入为".exit"，则退出程序
        {
            close_input_buffer(input_buffer);    // 关闭输入缓冲区
            exit(EXIT_SUCCESS);         // 退出程序
        }
        else
        {
            printf("Unrecognized command '%s'.\n", input_buffer->buffer);    // 打印未识别的命令
        }
    }
}