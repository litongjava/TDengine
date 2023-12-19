/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, arb/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_ARBITRATOR_H_
#define _TD_ARBITRATOR_H_

#include "tmsgcb.h"
#include "tqueue.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define arbFatal(...) do { if (arbDebugFlag & DEBUG_FATAL) { taosPrintLog("ARB FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define arbError(...) do { if (arbDebugFlag & DEBUG_ERROR) { taosPrintLog("ARB ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define arbWarn(...)  do { if (arbDebugFlag & DEBUG_WARN)  { taosPrintLog("ARB WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define arbInfo(...)  do { if (arbDebugFlag & DEBUG_INFO)  { taosPrintLog("ARB ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define arbDebug(...) do { if (arbDebugFlag & DEBUG_DEBUG) { taosPrintLog("ARB ", DEBUG_DEBUG, arbDebugFlag, __VA_ARGS__); }}    while(0)
#define arbTrace(...) do { if (arbDebugFlag & DEBUG_TRACE) { taosPrintLog("ARB ", DEBUG_TRACE, arbDebugFlag, __VA_ARGS__); }}    while(0)
// clang-format on

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SArbitrator SArbitrator;

/* ------------------------ SArbitrator ------------------------ */
SArbitrator *arbitratorOpen(const char *path, SMsgCb msgCb);
void         arbitratorClose(SArbitrator *pArbitrator);

int32_t arbitratorCreate(const char *path, int32_t arbitratorId);
void    arbitratorDestroy(const char *path);

void arbitratorProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_ARBITRATOR_H_*/
