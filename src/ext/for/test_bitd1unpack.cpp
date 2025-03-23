#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <vp4.h>
#include <vint.h>
#include "conf.h"

// 定义PAD8宏
#ifndef PAD8
#define PAD8(_x_) (((_x_) + 7) / 8)
#endif

const unsigned TEST_SIZE = 512;

#ifdef __AVX2__
void generate_test_data(unsigned* raw_values, unsigned n, unsigned char* encoded_data,
                        unsigned* out_size) {
    // 使用p4nd1enc256v32编码原始数据
    size_t end_ptr = p4nd1enc256v32(raw_values, n, encoded_data);

    // 计算编码后数据大小
    *out_size = end_ptr;
}
#endif
#define _1vbxget32(_ip_, _x_, _act_) do { _x_ = (unsigned)(*_ip_++);\
       if(!(_x_ & 0x80u)) {                                                                      _act_;}\
  else if(!(_x_ & 0x40u)) { _x_ = bswap16(ctou16(_ip_ - 1) & 0xff3fu); _ip_++;                       _act_;}\
  else if(!(_x_ & 0x20u)) { _x_ = (_x_ & 0x1f)<<16 | ctou16(_ip_);                    _ip_ += 2; _act_;}\
  else if(!(_x_ & 0x10u)) { _x_ = bswap32(ctou32(_ip_-1) & 0xffffff0fu);                  _ip_ += 3; _act_;}\
  else                    { _x_ = (unsigned long long)((_x_) & 0x07)<<32 | ctou32(_ip_); _ip_ += 4; _act_;}\
} while(0)
#define xvbxget32(_ip_, _x_) _1vbxget32(_ip_, _x_, ;)

// 用于快速得到 10^k 的一个表，避免多次调用 pow
// 注意 10^10=10000000000 需要 64 位才能存
static const uint64_t g_pow10[] = {
    1ULL,         // 10^0
    10ULL,        // 10^1
    100ULL,       // 10^2
    1000ULL,      // 10^3
    10000ULL,     // 10^4
    100000ULL,    // 10^5
    1000000ULL,   // 10^6
    10000000ULL,  // 10^7
    100000000ULL, // 10^8
    1000000000ULL,// 10^9
    10000000000ULL// 10^10
};

// 计算 10^(floor(b/3))，若超出 g_pow10 范围可再加判断
static inline uint64_t get_pow10_for_b(unsigned b) {
    // floor(b/3)
    unsigned idx = b / 3;
    if (idx >= sizeof(g_pow10)/sizeof(g_pow10[0])) {
        // 超过预置表最大 10^10，就固定返回 10^10 或自行处理
        return g_pow10[10];
    }
    return g_pow10[idx];
}

// 计算 2^b 的函数
static inline unsigned power2(unsigned b) {
    // (1U << b) 当 b=32 时也可能溢出，你可自行判断
    return (1U << b);
}


/**
 * @param values        输出数组
 * @param n             要生成的数据个数
 * @param b             当前位宽
 * @param with_exception 0=无异常，1=有异常
 */
void generate_raw_data_for_bitwidth(unsigned* values, unsigned n,
                                    unsigned b, int with_exception)
{
    if (n == 0) return;

    if (!with_exception) {
        // =====================================
        //         无异常模式：递增序列
        // =====================================
        // 1) 先给一个随机初始值 base (你也可随意决定)
        unsigned base = rand() % 1000;
        values[0] = base;

        // 2) 根据 b 分段决定“增量最大范围”
        unsigned inc_range;
        if (b < 4) {
            // b=0 => 2^0=1, b=1 =>2, b=2 =>4, b=3=>8
            inc_range = power2(b);
        } else {
            // b>=4 => 用10^(floor(b/3)) => 10,100,1000,...
            uint64_t r = get_pow10_for_b(b);
            // 这里最好判断 r 是否超出 unsigned 范围
            // 若测试场景不会特别大，可以直接转为 unsigned
            if (r > 0xFFFFFFFFULL) {
                r = 0xFFFFFFFFULL; // 避免溢出
            }
            inc_range = (unsigned)r;
        }

        // 3) 生成递增序列
        for (unsigned i = 1; i < n; i++) {
            // +1 是为了避免 0 增量的情况
            unsigned inc = 1 + rand() % inc_range;
            base += inc;
            values[i] = base;
        }

    } else {
        // =====================================
        //         有异常模式：直接随机
        // =====================================

        // 观察示例得知：
        //  - b=0 => rand()%2
        //  - b=2 => rand()%4
        //  - b=3 => rand()%10
        //  - b=7 => rand()%100
        //  - b=10 => rand()%1000
        //  - b=13 => rand()%10000
        //  => 规律：当 b >= 3 用 10^(floor(b/3))；当 b < 3 用特殊处理

        uint64_t val_range = 0; // 用 64 位临时存，最后再转回 unsigned

        if (b == 0) {
            val_range = 2; // 0..1
        } 
        else if (b == 1) {
            // 你没给 b=1 的具体例子，这里假设跟 b=0 一样 => range=2
            val_range = 2; // 0..1
        } 
        else if (b == 2) {
            val_range = 4; // 0..3
        }
        else {
            // b>=3 => 用 10^(floor(b/3))
            val_range = get_pow10_for_b(b);
            // 同样检查一下是否超过 unsigned
            if (val_range > 0xFFFFFFFFULL) {
                val_range = 0xFFFFFFFFULL;
            }
        }

        // 直接随机
        for (unsigned i = 0; i < n; i++) {
            unsigned x = (unsigned)(rand() % (unsigned)val_range);
            values[i] = x;
        }
    }
}

/**
 * 生成 n 个有符号数:
 *   - b<3: 范围很小(±(1<<b) 之类)
 *   - b>=3: 直接从 ±(10^(floor(b/3))) 随机, 并包含一定的负值
 *
 * with_exception=0 => 生成一个“有序/有限范围”
 * with_exception=1 => 生成一个“更大随机范围” (你可自定义)
 */
static void generate_raw_signed_data_for_zigzag(unsigned *values,
                                                unsigned n,
                                                unsigned b,
                                                int with_exception)
{
    if (n == 0) return;

    // srand(...) 在外部一次初始化
    uint64_t val_range = 1;
    if (b < 3) {
        // 例如 b=0 =>±1, b=1=>±2, b=2=>±4
        val_range = (1ULL << b);
    } else {
        // b>=3 => use get_pow10_for_b(b) => 10^(floor(b/3))
        val_range = get_pow10_for_b(b); // 参考你贴的 delta pfor
        if(val_range > 0x7fffffffULL) {
            val_range = 0x7fffffffULL; // 避免溢出 32-bit
        }
    }

    for(unsigned i=0; i<n; i++){
        // 先产生 0..val_range-1
        int32_t x = (int32_t)(rand() % (unsigned)val_range);
        // 随机决定正负
        if(with_exception) {
            // 例如 50% 概率取反
            if((rand() & 1) == 1) x = -x;
        } else {
            // 不带异常 => 大部分正, 也可以小概率负
            if((rand()%10)==0) x = -x; 
        }
        values[i] = x;
    }
}
/**
 * 对 b=0..32, with_exception=0 or 1:
 *   1) 生成 n=256(或512) 个 int32_t
 *   2) zigzag encode => store => bitpack => (similar to "encoded_data")
 *   3) zigzag decode => compare => check mismatch
 */
void run_testZigzag(unsigned b,
                    int with_exception,
                    unsigned TEST_SIZE,
                    unsigned *raw_values,
                    unsigned char *encoded_data,
                    unsigned *decoded1,
                    unsigned *decoded2)
{
    printf("Zigzag 测试: 位宽 b=%u, with_exception=%d\n", b, with_exception);

    // 1) 生成带正负 raw data
    generate_raw_signed_data_for_zigzag(raw_values, TEST_SIZE, b, with_exception);
    unsigned encoded_size = p4nzenc256v32(raw_values, TEST_SIZE, encoded_data);

    // 获取编码头部信息（例如起始值等）
    unsigned start;
    unsigned char* copy = encoded_data;
    xvbxget32(copy, start);
    unsigned char encoded_b = copy[0];  // 编码后的第一个字节为位宽
    if((encoded_b & 0x40)) {
        encoded_b &= 0x3f;
    } else {
        if(encoded_b & 0x80) {
            encoded_b &= 0x7f;
        }
    }
    printf("  编码参数: 位宽 b=%u, 起始值 start=%u, 编码大小=%u字节\n", encoded_b, start, encoded_size);

    // 3) decode => two versions for cross-check 
    //   (here we define "decoded1" from "bitzunpack256v32...??" and "decoded2" from "bitzunpack256scalarv32Zigzag"??)
    memset(decoded1,0,TEST_SIZE*sizeof(unsigned));
    memset(decoded2,0,TEST_SIZE*sizeof(unsigned));

    // "decoded1" => maybe  vector version if you have it? e.g. "bitzunpack256v32(in,b, out,??)" 
    // "decoded2" => scalar version ?

    // for demonstration, we do the same decode to compare:
    p4nzdec256v32(encoded_data, TEST_SIZE, decoded1);
    p4nzdec256scalarv32(encoded_data, TEST_SIZE, decoded2);

    // 4) compare mismatch
    int mismatch=0;
    for(unsigned i=0;i<TEST_SIZE;i++){
        if(decoded1[i] != decoded2[i]){
            if(mismatch<10)
                printf(" mismatch at i=%u: dec1=%d, dec2=%d\n", i, decoded1[i], decoded2[i]);
            mismatch++;
        }
    }
    if(mismatch==0){
        printf(" decode1 & decode2 match!\n");
        // verify with original
        int error=0;
        for(unsigned i=0;i<TEST_SIZE;i++){
            if(decoded1[i] != raw_values[i]){
                if(error<10)
                  printf(" raw mismatch at i=%u: raw=%d, dec=%d\n", i,raw_values[i], decoded1[i]);
                error++;
            }
        }
        if(error==0) printf(" and match raw data!\n");
        else printf(" total %d raw mismatch\n", error);
    } else {
        printf(" total mismatch=%d\n", mismatch);
    }
    printf("\n");
}

#ifdef __AVX2__
void run_test(unsigned b, int with_exception, unsigned TEST_SIZE,
              unsigned* raw_values, unsigned char* encoded_data,
              unsigned* decoded1, unsigned* decoded2) {
    printf("测试: 位宽 b=%u, 异常%s\n", b, (with_exception ? "有" : "无"));

    // 生成符合当前 b 与异常模式的原始数据
    generate_raw_data_for_bitwidth(raw_values, TEST_SIZE, b, with_exception);

    unsigned encoded_size;
    generate_test_data(raw_values, TEST_SIZE, encoded_data, &encoded_size);

    // 获取编码头部信息（例如起始值等）
    unsigned start;
    unsigned char* copy = encoded_data;
    xvbxget32(copy, start);
    unsigned char encoded_b = copy[0];  // 编码后的第一个字节为位宽
    if((encoded_b & 0x40)) {
        encoded_b &= 0x3f;
    } else {
        if(encoded_b & 0x80) {
            encoded_b &= 0x7f;
        }
    }
    printf("  编码参数: 位宽 b=%u, 起始值 start=%u, 编码大小=%u字节\n", encoded_b, start, encoded_size);

    // 清空解码缓冲区
    memset(decoded1, 0, TEST_SIZE * sizeof(unsigned));
    memset(decoded2, 0, TEST_SIZE * sizeof(unsigned));

    // 调用两种解码方式
    p4nd1dec256v32(encoded_data, TEST_SIZE, decoded1);
    p4nd1dec256scalarv32(encoded_data, TEST_SIZE, decoded2);

    // 比较两个解码结果是否匹配
    int mismatch = 0;
    for (unsigned i = 0; i < TEST_SIZE; i++) {
        if (decoded1[i] != decoded2[i]) {
            if (mismatch < 10)
                printf("  不匹配: 索引 %u, 原始值=%u, 原始解码=%u, 标量解码=%u\n",
                       i, raw_values[i], decoded1[i], decoded2[i]);
            mismatch++;
        }
    }
    if (mismatch == 0) {
        printf("  通过: 所有解码值匹配!\n");
        // 验证解码值与原始数据是否一致
        int error = 0;
        for (unsigned i = 0; i < TEST_SIZE && error < 10; i++) {
            if (decoded1[i] != raw_values[i]) {
                printf("  编码/解码错误: 索引 %u, 原始值=%u, 解码值=%u\n",
                       i, raw_values[i], decoded1[i]);
                error++;
            }
        }
        if (error == 0)
            printf("  验证通过: 解码结果与原始数据一致\n");
    } else {
        printf("  失败: 有 %d 个值不匹配\n", mismatch);
        printf("  原始数据 (前16个): ");
        for (unsigned i = 0; i < 16 && i < TEST_SIZE; i++)
            printf("%u ", raw_values[i]);
        printf("...\n");
        printf("  原始解码 (前16个): ");
        for (unsigned i = 0; i < 16 && i < TEST_SIZE; i++)
            printf("%u ", decoded1[i]);
        printf("...\n");
        printf("  标量解码 (前16个): ");
        for (unsigned i = 0; i < 16 && i < TEST_SIZE; i++)
            printf("%u ", decoded2[i]);
        printf("...\n");
    }
    printf("\n");
}
#endif

void testZigZag()
{
    const unsigned TEST_SIZE=512; //or512
    unsigned *raw_values= (unsigned*) malloc(TEST_SIZE*sizeof(unsigned));
    unsigned *decoded1=  (unsigned*) malloc(TEST_SIZE*sizeof(unsigned));
    unsigned *decoded2=  (unsigned*) malloc(TEST_SIZE*sizeof(unsigned));
    unsigned char* encoded_data= (unsigned char*) malloc(TEST_SIZE*4+ 10); //maybe

    srand((unsigned)time(NULL));
    printf("开始测试 p4nzdec256v32...\n");

    for(unsigned b=0; b<=32; b++){
        run_testZigzag(b,0, TEST_SIZE, raw_values, encoded_data, decoded1, decoded2);
        run_testZigzag(b,1, TEST_SIZE, raw_values, encoded_data, decoded1, decoded2);
    }

    free(raw_values);
    free(decoded1);
    free(decoded2);
    free(encoded_data);
}

#ifdef __AVX2__
void test_p4nd1dec256v32() {
    const unsigned TEST_SIZE = 512;

    // 分配缓冲区
    unsigned* raw_values   = (unsigned*)malloc(TEST_SIZE * sizeof(unsigned));
    unsigned char* encoded_data = (unsigned char*)malloc(TEST_SIZE * sizeof(unsigned) * 2);
    unsigned* decoded1     = (unsigned*)malloc(TEST_SIZE * sizeof(unsigned));
    unsigned* decoded2     = (unsigned*)malloc(TEST_SIZE * sizeof(unsigned));

    srand((unsigned)time(NULL));
    printf("开始测试 p4nd1dec256v32...\n");

    // 对 b = 0 到 31 测试两种模式：无异常和有异常
    for (unsigned b = 0; b < 32; b++) {
        run_test(b, 0, TEST_SIZE, raw_values, encoded_data, decoded1, decoded2);
        run_test(b, 1, TEST_SIZE, raw_values, encoded_data, decoded1, decoded2);
    }
    // 对 b == 32 只测试无异常情况
    run_test(32, 0, TEST_SIZE, raw_values, encoded_data, decoded1, decoded2);

    free(raw_values);
    free(encoded_data);
    free(decoded1);
    free(decoded2);

    printf("测试完成!\n");
}
#endif
int main() {
#ifdef __AVX2__
    test_p4nd1dec256v32();
#endif
    testZigZag();
    //test_until_b1_achieved_improved();
    return 0;
}
