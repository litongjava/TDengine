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

#include "arbInt.h"

static int32_t arbitratorProcessSetVgroupsReq(SArbitrator *pArb, SRpcMsg *pMsg) {
  SArbSetVgroupsReq setReq = {0};
  if (tDeserializeSArbSetVgroupsReq(pMsg->pCont, pMsg->contLen, &setReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (setReq.arbId != pArb->arbInfo.arbId) {
    terrno = TSDB_CODE_INVALID_MSG;
    arbError("arbId not matched local:%d, msg:%d", pArb->arbInfo.arbId, setReq.arbId);
    return -1;
  }

  SArray *tmp = pArb->arbInfo.vgroups;
  pArb->arbInfo.vgroups = setReq.vgroups;
  setReq.vgroups = tmp;

  arbInfo("arbId:%d, save config while process set vgroups", pArb->arbInfo.arbId);
  if (arbitratorUpdateInfo(pArb->path, &pArb->arbInfo) < 0) {
    return -1;
  }

  return 0;
}

static int32_t arbitratorProcessArbHeartBeatTimer(SArbitrator *pArb, SRpcMsg *pMsg) {
  // TODO(LSG): send msg to all vnodes;
  return 0;
  //   SVArbHeartBeatReq req = {.dnodeId = pMgmt->pData->dnodeId};
  //   int32_t           contLen = tSerializeSVArbHeartBeatReq(NULL, 0, &req);
  //   void             *pHead = rpcMallocCont(contLen);
  //   tSerializeSVArbHeartBeatReq(pHead, contLen, &req);

  //   SRpcMsg rpcMsg = {.pCont = pHead,
  //                     .contLen = contLen,
  //                     .msgType = TDMT_VND_ARB_HEARTBEAT,
  //                     .info.ahandle = (void *)0x9527,
  //                     .info.refId = 0,
  //                     .info.noResp = 0};
  //   SEpSet  epset = {0};

  //   dmGetMnodeEpSet(pMgmt->pData, &epset);

  //   return rpcSendRequest(pMgmt->msgCb.clientRpc, &epset, &rpcMsg, NULL);
}

void arbitratorProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SArbitrator *pArb = pInfo->ahandle;
  int32_t      code = -1;

  arbTrace("msg:%p, get from arb-mgmt queue", pMsg);
  switch (pMsg->msgType) {
    case TDMT_ARB_SET_VGROUPS:
      code = arbitratorProcessSetVgroupsReq(pArb, pMsg);
      break;
    case TDMT_ARB_HEARTBEAT_TIMER:
      code = arbitratorProcessArbHeartBeatTimer(pArb, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      arbError("msg:%p, not processed in arb-mgmt queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0) {
      if (terrno != 0) code = terrno;
      arbError("msg:%p, failed to process since %s, type:%s", pMsg, tstrerror(code), TMSG_INFO(pMsg->msgType));
    }
    // arbmSendRsp(pMsg, code);
  }

  arbTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}
