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

int32_t arbmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SMCreateArbitratorReq createReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to create arbitrator since %s", terrstr());
    tFreeSMCreateQnodeReq(&createReq);
    return -1;
  }

  bool deployed = true;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write arbitrator file since %s", terrstr());
    tFreeSMCreateQnodeReq(&createReq);
    return -1;
  }

  tFreeSMCreateQnodeReq(&createReq);
  return 0;
}

int32_t arbmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  SMDropArbitratorReq dropReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop arbitrator since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  bool deployed = false;
  if (dmWriteFile(pInput->path, pInput->name, deployed) != 0) {
    dError("failed to write arbitrator file since %s", terrstr());
    tFreeSMCreateQnodeReq(&dropReq);
    return -1;
  }

  tFreeSMCreateQnodeReq(&dropReq);
  return 0;
}

SArray *arbmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(16, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  // Requests handled by VNODE
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY, arbmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_QUERY, arbmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_CONTINUE, arbmPutNodeMsgToQueryQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH, arbmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_MERGE_FETCH, arbmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_FETCH_RSP, arbmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_QUERY_HEARTBEAT, arbmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_SCH_CANCEL_TASK, arbmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_DROP_TASK, arbmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_SCH_TASK_NOTIFY, arbmPutNodeMsgToFetchQueue, 1) == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
