/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
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

#ifndef _TD_ARBITRATOR_INT_H_
#define _TD_ARBITRATOR_INT_H_

#include "os.h"

#include "tlog.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "trpc.h"
#include "tjson.h"

#include "arbitrator.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ARB_INFO_FNAME     "arbitrator.json"
#define ARB_INFO_FNAME_TMP "arbitrator_tmp.json"

typedef struct {
  int32_t  hbSeq;
  int32_t  lastHbSeq;
} SArbHbSeqNum;

typedef struct {
  int32_t arbId;
  SArray *vgroups;  // SArbitratorVgroupInfo
} SArbitratorInfo;

struct SArbitrator {
  SArbitratorInfo arbInfo;
  SHashObj       *hbSeqMap; // key: ((int64_t)dnodeId << 32) + vgId, value: SArbHbSeqNum
  char            arbToken[TD_ARB_TOKEN_SIZE];
  SMsgCb          msgCb;
  char            path[];
};

int32_t arbitratorUpdateInfo(const char *dir, SArbitratorInfo *pInfo);
int64_t arbitratorGenerateHbSeqKey(int32_t dnodeId, int32_t vgId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_ARBITRATOR_INT_H_*/
