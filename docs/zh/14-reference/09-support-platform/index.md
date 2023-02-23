---
title: 支持平台列表
description: "TDengine 服务端、客户端和连接器支持的平台列表"
---

## TDengine 服务端支持的平台列表

|              | **Windows server 2016/2019** | **Windows 10/11** | **CentOS 7.9/8** | **Ubuntu 18/20** | **统信 UOS** | **银河/中标麒麟** | **凝思 V60/V80** | **macOS** |
| ------------ | ---------------------------- | ----------------- | ---------------- | ---------------- | ------------ | ----------------- | ---------------- | --------- |
| X64          | ●                            | ●                 | ●                | ●                | ●            | ●                 | ●                | ●         |
| 树莓派 ARM64 |                              |                   | ●                |                  |              |                   |                  |           |
| 华为云 ARM64 |                              |                   |                  | ●                |              |                   |                  |           |
| M1           |                              |                   |                  |                  |              |                   |                  | ●         |

注： ● 表示经过官方测试验证， ○ 表示非官方测试验证。

## TDengine 客户端和连接器支持的平台列表

目前 TDengine 的连接器可支持的平台广泛，目前包括：X64/X86/ARM64/ARM32/MIPS/LoongArch64 等硬件平台，以及 Linux/Win64/Win32/macOS 等开发环境。

对照矩阵如下：

| **CPU**     | **X64 64bit** | **X64 64bit** | **ARM64** | **X64 64bit** | **ARM64** |
| ----------- | ------------- | ------------- | --------- | ------------- | --------- |
| **OS**      | **Linux**     | **Win64**     | **Linux** | **macOS**     | **macOS** |
| **C/C++**   | ●             | ●             | ●         | ●             | ●         |
| **JDBC**    | ●             | ●             | ●         | ○             | ○         |
| **Python**  | ●             | ●             | ●         | ●             | ●         |
| **Go**      | ●             | ●             | ●         | ●             | ●         |
| **NodeJs**  | ●             | ●             | ●         | ○             | ○         |
| **C#**      | ●             | ●             | ○         | ○             | ○         |
| **RESTful** | ●             | ●             | ●         | ●             | ●         |

注：● 表示官方测试验证通过，○ 表示非官方测试验证通过，-- 表示未经验证。