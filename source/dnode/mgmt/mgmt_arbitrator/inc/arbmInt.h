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

#ifndef _TD_DND_ARBITRATOR_INT_H_
#define _TD_DND_ARBITRATOR_INT_H_

#include "dmUtil.h"

#include "arbitrator.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ARB_MGMT_INFO_FNAME     "arbitrators.json"
#define ARB_MGMT_INFO_FNAME_TMP "arbitrators_tmp.json"

typedef struct SArbitratorMgmt {
  SDnodeData      *pData;
  SMsgCb           msgCb;
  const char      *path;
  const char      *name;
  SQWorkerPool     queryPool;
  SWWorkerPool     fetchPool;
  SSingleWorker    mgmtWorker;
  SHashObj        *hash;
  TdThreadRwlock   lock;
  SArbitratorsStat state;
  TdThread         thread;
  bool             stop;
} SArbitratorMgmt;

typedef struct {
  int32_t arbitratorId;
  int8_t  dropped;
  char    path[PATH_MAX + 20];
} SArbWrapperCfg;

typedef struct {
  int32_t       arbitratorId;
  int32_t       refCount;
  int8_t        dropped;
  int8_t        failed;
  int8_t        disable;
  char         *path;
  SArbitrator  *pImpl;
  SMultiWorker  pWriteW;
  SMultiWorker  pSyncW;
  SMultiWorker  pSyncRdW;
  SMultiWorker  pApplyW;
  STaosQueue   *pQueryQ;
  STaosQueue   *pStreamQ;
  STaosQueue   *pFetchQ;
} SArbitratorObj;

typedef struct {
  int32_t          opened;
  int32_t          failed;
  int32_t          threadIndex;
  TdThread         thread;
  SArbitratorMgmt *pMgmt;
  SArbWrapperCfg  *pCfg;
  SArbitratorObj  *pArbitrator;
} SArbitratorThread;

// arbmHandle.c
SArray *arbmGetMsgHandles();
int32_t arbmProcessCreateReq(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg);
int32_t arbmProcessDropReq(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg);

// arbmFile.c
int32_t          arbmGetArbitratorListFromFile(SArbitratorMgmt *pMgmt, SArbWrapperCfg **ppCfgs, int32_t *numOfArbitrators);
int32_t          arbmWriteArbitratorListToFile(SArbitratorMgmt *pMgmt);
SArbitratorObj **arbmGetArbitratorListFromHash(SArbitratorMgmt *pMgmt, int32_t *numOfArbitrators);

// arbmWorker.c
int32_t arbmPutRpcMsgToQueue(SArbitratorMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc);
int32_t arbmGetQueueSize(SArbitratorMgmt *pMgmt, int32_t vgId, EQueueType qtype);

int32_t arbmStartWorker(SArbitratorMgmt *pMgmt);
void    arbmStopWorker(SArbitratorMgmt *pMgmt);
int32_t arbmPutNodeMsgToQueue(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_ARBITRATOR_INT_H_*/
