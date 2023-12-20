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

  SArbitratorObj *pArbObj = arbmAcquireArbitratorImpl(pMgmt, arbitratorId, false);
  if (pArbObj == NULL) {
    dInfo("arbitratorId:%d, failed to drop since %s", arbitratorId, terrstr());
    terrno = TSDB_CODE_ARB_NOT_EXIST;
    return -1;
  }

  pArbObj->dropped = 1;
  if (arbmWriteArbitratorListToFile(pMgmt) != 0) {
    pArbObj->dropped = 0;
    arbmReleaseArbitrator(pMgmt, pArbObj);
    return -1;
  }

  arbmCloseArbitrator(pMgmt, pArbObj);
  arbmWriteArbitratorListToFile(pMgmt);

  dInfo("arbitratorId:%d, is dropped", arbitratorId);
  return 0;
}

static void arbmProcessGetAribtratorVgIdsRsp(SArbitratorMgmt *pMgmt, SRpcMsg *pRsp) {
  SMGetArbitratorsRsp getRsp = {0};
  if (tDeserializeSMGetArbitratorsRsp(pRsp->pCont, pRsp->contLen, &getRsp)) {
    dError("failed to deserialize get-arbitrators rsp since %s", terrstr());
    goto _OVER;
  }

  if (getRsp.dnodeId != pMgmt->pData->dnodeId) {

  }


 _OVER:
  tFreeSMGetArbitratorsRsp(&getRsp);
  rpcFreeCont(pRsp->pCont);
}

void arbmSendGetArbitratorsReq(SArbitratorMgmt *pMgmt) {
  SMGetArbitratorsReq req = {0};

  taosThreadRwlockRdlock(&pMgmt->pData->lock);
  req.dnodeId = pMgmt->pData->dnodeId;
  taosThreadRwlockUnlock(&pMgmt->pData->lock);

  int32_t contLen = tSerializeSMGetArbitratorsReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSMGetArbitratorsReq(pHead, contLen, &req);

  SRpcMsg rpcMsg = {.pCont = pHead,
                    .contLen = contLen,
                    .msgType = TDMT_MND_GET_ARBITRATORS,
                    .info.ahandle = (void *)0x9527,
                    .info.refId = 0,
                    .info.noResp = 0};
  SRpcMsg rpcRsp = {0};

  dTrace("send get-arbitrators req to mnode, dnodeId:%d", req.dnodeId);

  SEpSet epSet = {0};
  dmGetMnodeEpSet(pMgmt->pData, &epSet);
  rpcSendRecvWithTimeout(pMgmt->msgCb.clientRpc, &epSet, &rpcMsg, &rpcRsp, 5000);
  if (rpcRsp.code != 0) {
    dmRotateMnodeEpSet(pMgmt->pData);
    char tbuf[256];
    dmEpSetToStr(tbuf, sizeof(tbuf), &epSet);
    dError("failed to send get-arbitrators req since %s, epSet:%s, inUse:%d", tstrerror(rpcRsp.code), tbuf,
           epSet.inUse);
  }
  arbmProcessGetAribtratorVgIdsRsp(pMgmt, &rpcRsp);
}

SArray *arbmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(16, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  // Requests handled by ARBITRATOR
  if (dmSetMgmtHandle(pArray, TDMT_DND_CREATE_ARBITRATOR, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_DND_DROP_ARBITRATOR, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_MND_GET_ARBITRATORS_RSP, arbmPutNodeMsgToQueue, 0) == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
