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

int32_t arbmProcessCreateReq(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SDCreateArbitratorReq createReq = {0};
  if (tDeserializeSDCreateArbitratorReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t arbId = createReq.arbitratorId;

  char path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN, "%s%sarbitrator%d", pMgmt->path, TD_DIRSEP, arbId);

  if (arbitratorCreate(path, arbId) != 0) {
    dError("arbitratorId:%d, failed to create arbitrator since %s", arbId, terrstr());
    return -1;
  }

  return 0;
}

int32_t arbmProcessDropReq(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SDDropArbitratorReq dropReq = {0};
  if (tDeserializeSDCreateArbitratorReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  return 0;
}

SArray *arbmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(16, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  // Requests handled by ARBITRATOR
  // if (dmSetMgmtHandle(pArray, TDMT_ARB_REGISTER_VGROUP, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_ARBITRATOR, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_ARBITRATOR, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
