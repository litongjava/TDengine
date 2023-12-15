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
  SArbWrapperCfg        wrapperCfg = {0};
  int32_t               code = -1;
  if (tDeserializeSDCreateArbitratorReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t arbId = createReq.arbitratorId;

  char path[TSDB_FILENAME_LEN] = {0};
  snprintf(path, TSDB_FILENAME_LEN, "%s%sarbitrator%d", pMgmt->path, TD_DIRSEP, arbId);

  if (arbitratorCreate(path, arbId) != 0) {
    dError("arbitratorId:%d, failed to create arbitrator since %s", arbId, terrstr());
    code = terrno;
    return code;
  }

  SArbitrator *pImpl = arbitratorOpen(path, pMgmt->msgCb);
  if (pImpl == NULL) {
    dError("arbitratorId:%d, failed to open arbitrator since %s", arbId, terrstr());
    code = terrno;
    goto _OVER;
  }

  wrapperCfg.arbitratorId = arbId;
  wrapperCfg.dropped = 0;
  snprintf(wrapperCfg.path, sizeof(wrapperCfg.path), "%s%sarbtrator%d", pMgmt->path, TD_DIRSEP, arbId);
  code = arbmOpenArbitrator(pMgmt, &wrapperCfg, pImpl);
  if (code != 0) {
    dError("arbitratorId:%d, failed to open vnode since %s", arbId, terrstr());
    code = terrno;
    goto _OVER;
  }

  // code = arbitratorStart(pImpl);
  // if (code != 0) {
  //   dError("arbitratorId:%d, failed to start sync since %s", arbId, terrstr());
  //   goto _OVER;
  // }

  code = arbmWriteArbitratorListToFile(pMgmt);
  if (code != 0) {
    code = terrno;
    goto _OVER;
  }

_OVER:
  if (code != 0) {
    arbitratorClose(pImpl);
    arbitratorDestroy(path);
  } else {
    dInfo("arbitratorId:%d, arbitrator management handle msgType:%s, end to create arbitrator, arbitrator is created",
          arbId, TMSG_INFO(pMsg->msgType));
  }

  terrno = code;
  return code;
}

int32_t arbmProcessDropReq(SArbitratorMgmt *pMgmt, SRpcMsg *pMsg) {
  SDDropArbitratorReq dropReq = {0};
  if (tDeserializeSDCreateArbitratorReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t arbitratorId = dropReq.arbitratorId;
  dInfo("arbitratorId:%d, start to drop arbitrator", arbitratorId);

  if (dropReq.dnodeId != pMgmt->pData->dnodeId) {
    terrno = TSDB_CODE_INVALID_MSG;
    dError("arbitratorId:%d, dnodeId:%d not matched with local dnode", dropReq.arbitratorId, dropReq.dnodeId);
    return -1;
  }

  SArbitratorObj *pArbitrator = arbmAcquireArbitratorImpl(pMgmt, arbitratorId, false);
  if (pArbitrator == NULL) {
    dInfo("arbitratorId:%d, failed to drop since %s", arbitratorId, terrstr());
    terrno = TSDB_CODE_ARB_NOT_EXIST;
    return -1;
  }

  pArbitrator->dropped = 1;
  if (arbmWriteArbitratorListToFile(pMgmt) != 0) {
    pArbitrator->dropped = 0;
    arbmReleaseArbitrator(pMgmt, pArbitrator);
    return -1;
  }

  arbmCloseArbitrator(pMgmt, pArbitrator);
  arbmWriteArbitratorListToFile(pMgmt);

  dInfo("arbitratorId:%d, is dropped", arbitratorId);
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
