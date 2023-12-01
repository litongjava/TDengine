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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "arbmInt.h"

static inline void arbmSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void arbmProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SArbitratorMgmt *pMgmt = pInfo->ahandle;
  dTrace("msg:%p, get from arbitrator queue", pMsg);

  int32_t code = arbProcessQueryMsg(pMgmt->pArbitrator, pInfo->timestamp, pMsg);
  if (IsReq(pMsg) && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (code != 0 && terrno != 0) code = terrno;
    arbmSendRsp(pMsg, code);
  }

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static int32_t arbmPutNodeMsgToWorker(SSingleWorker *pWorker, SRpcMsg *pMsg) {
  dTrace("msg:%p, put into worker %s, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->msgType));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t arbmPutNodeMsgToQueryQueue(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  arbPreprocessQueryMsg(pMgmt->pArbitrator, pMsg);

  return 0;
  // return amPutNodeMsgToWorker(&pMgmt->queryWorker, pMsg);
}

int32_t arbmPutNodeMsgToFetchQueue(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  return 0;
  // return amPutNodeMsgToWorker(&pMgmt->fetchWorker, pMsg);
}

int32_t arbmPutRpcMsgToQueue(SArbitratorMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  return 0;
  // SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen);
  // if (pMsg == NULL) return -1;
  // memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  // pRpc->pCont = NULL;

  // switch (qtype) {
  //   case QUERY_QUEUE:
  //     dTrace("msg:%p, is created and will put into arbitrator-query queue, len:%d", pMsg, pRpc->contLen);
  //     taosWriteQitem(pMgmt->queryWorker.queue, pMsg);
  //     return 0;
  //   case READ_QUEUE:
  //   case FETCH_QUEUE:
  //     dTrace("msg:%p, is created and will put into arbitrator-fetch queue, len:%d", pMsg, pRpc->contLen);
  //     taosWriteQitem(pMgmt->fetchWorker.queue, pMsg);
  //     return 0;
  //   default:
  //     terrno = TSDB_CODE_INVALID_PARA;
  //     rpcFreeCont(pMsg->pCont);
  //     taosFreeQitem(pMsg);
  //     return -1;
  // }
}

int32_t arbmGetQueueSize(SArbitratorMgmt *pMgmt, int32_t vgId, EQueueType qtype) {
  return 0;
  // int32_t size = -1;

  // switch (qtype) {
  //   case QUERY_QUEUE:
  //     size = taosQueueItemSize(pMgmt->queryWorker.queue);
  //     break;
  //   case FETCH_QUEUE:
  //     size = taosQueueItemSize(pMgmt->fetchWorker.queue);
  //     break;
  //   default:
  //     break;
  // }

  // return size;
}

int32_t arbmStartWorker(SArbitratorMgmt *pMgmt) {
  return 0;
  // SSingleWorkerCfg queryCfg = {
  //     .min = tsNumOfVnodeQueryThreads,
  //     .max = tsNumOfVnodeQueryThreads,
  //     .name = "arbitrator-query",
  //     .fp = (FItem)amProcessQueue,
  //     .param = pMgmt,
  // };

  // if (tSingleWorkerInit(&pMgmt->queryWorker, &queryCfg) != 0) {
  //   dError("failed to start arbitrator-query worker since %s", terrstr());
  //   return -1;
  // }

  // SSingleWorkerCfg fetchCfg = {
  //     // .min = tsNumOfArbitratorFetchThreads,
  //     // .max = tsNumOfArbitratorFetchThreads,
  //     .min = 1,
  //     .max = 1,
  //     .name = "arbitrator-fetch",
  //     .fp = (FItem)amProcessQueue,
  //     .param = pMgmt,
  // };

  // if (tSingleWorkerInit(&pMgmt->fetchWorker, &fetchCfg) != 0) {
  //   dError("failed to start arbitrator-fetch worker since %s", terrstr());
  //   return -1;
  // }

  // dDebug("arbitrator workers are initialized");
  // return 0;
}

void arbmStopWorker(SArbitratorMgmt *pMgmt) {
  // tSingleWorkerCleanup(&pMgmt->queryWorker);
  // tSingleWorkerCleanup(&pMgmt->fetchWorker);
  // dDebug("arbitrator workers are closed");
}
