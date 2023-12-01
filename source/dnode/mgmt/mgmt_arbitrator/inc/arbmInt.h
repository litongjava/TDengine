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

typedef struct SArbitratorMgmt {
  SDnodeData  *pData;
  SArbitrator *pArbitrator;
  SMsgCb       msgCb;
  const char  *path;
  const char  *name;
  // SSingleWorker queryWorker;
  // SSingleWorker fetchWorker;
} SArbitratorMgmt;

// amHandle.c
SArray *arbmGetMsgHandles();
int32_t arbmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);
int32_t arbmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg);

// amWorker.c
int32_t arbmPutRpcMsgToQueue(SArbitratorMgmt *pMgmt, EQueueType qtype, SRpcMsg *pMsg);
int32_t arbmGetQueueSize(SArbitratorMgmt *pMgmt, int32_t vgId, EQueueType qtype);

int32_t arbmStartWorker(SArbitratorMgmt *pMgmt);
void    arbmStopWorker(SArbitratorMgmt *pMgmt);
int32_t arbmPutNodeMsgToQueryQueue(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg);
int32_t arbmPutNodeMsgToFetchQueue(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg);

int32_t arbPreprocessQueryMsg(SArbitrator *pArbitrator, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_ARBITRATOR_INT_H_*/
