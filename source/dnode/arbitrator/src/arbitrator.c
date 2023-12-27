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
#include "tmisce.h"

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

typedef struct {
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;
  SArray  *array;
} SArbHbDnodeInfo;

static int32_t arbitratorProcessArbHeartBeatTimer(SArbitrator *pArb, SRpcMsg *pMsg) {
  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  // collect all vgId/hbSeq of Dnodes
  SArray *pVgInfoArray = pArb->arbInfo.vgroups;
  int32_t arraySize = taosArrayGetSize(pVgInfoArray);
  for (int32_t i = 0; i < arraySize; i++) {
    SArbitratorVgroupInfo *pVgInfo = taosArrayGet(pVgInfoArray, i);
    for (int32_t j = 0; j < pVgInfo->replica; j++) {
      SReplica *pReplica = &pVgInfo->replicas[j];
      SArbHbDnodeInfo *pDnodeInfo = taosHashGet(pHash, &pReplica->id, sizeof(int32_t));
      if (!pDnodeInfo) {
        SArbHbDnodeInfo dnodeInfo = {0};
        memcpy(dnodeInfo.fqdn, pReplica->fqdn, TSDB_FQDN_LEN);
        dnodeInfo.port = pReplica->port;
        dnodeInfo.array = taosArrayInit(16, sizeof(SVArbHeartBeatSeq));
        taosHashPut(pHash, &pReplica->id, sizeof(int32_t), &dnodeInfo, sizeof(SArbHbDnodeInfo));
        pDnodeInfo = taosHashGet(pHash, &pReplica->id, sizeof(int32_t));
      }
      SVArbHeartBeatSeq seq = {.vgId = pVgInfo->vgId};
      int64_t           key = arbitratorGenerateHbSeqKey(pReplica->id, pVgInfo->vgId);
      SArbHbSeqNum     *pHbSeqNum = taosHashGet(pArb->hbSeqMap, &key, sizeof(int64_t));
      if (!pHbSeqNum) {
        SArbHbSeqNum seqNum = {.hbSeq = 0, .lastHbSeq = -1};
        taosHashPut(pArb->hbSeqMap, &key, sizeof(int64_t), &seqNum, sizeof(SArbHbSeqNum));
        pHbSeqNum = taosHashGet(pArb->hbSeqMap, &key, sizeof(int64_t));
      }

      seq.seqNo = pHbSeqNum->hbSeq++; // update seq of dnode-vg
      taosArrayPush(pDnodeInfo->array, &seq);
    }
  }

  size_t keyLen = 0;
  void   *pIter = taosHashIterate(pHash, NULL);
  while (pIter) {
    int32_t dnodeId = *(int32_t *)taosHashGetKey(pIter, &keyLen);
    SArbHbDnodeInfo *pDnodeInfo = pIter;

    SVArbHeartBeatReq req = {0};
    req.arbId = pArb->arbInfo.arbId;
    memcpy(req.arbToken, pArb->arbToken, TD_ARB_TOKEN_SIZE);
    req.dnodeId = dnodeId;
    req.arbSeqArray = pDnodeInfo->array;
    int32_t contLen = tSerializeSVArbHeartBeatReq(NULL, 0, &req);
    void   *pHead = rpcMallocCont(contLen);
    tSerializeSVArbHeartBeatReq(pHead, contLen, &req);

    SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_VND_ARB_HEARTBEAT};

    SEpSet epset = {.inUse = 0, .numOfEps = 1};
    addEpIntoEpSet(&epset, pDnodeInfo->fqdn, pDnodeInfo->port);
    pArb->msgCb.sendReqFp(&epset, &rpcMsg);

    pIter = taosHashIterate(pHash, pIter);
  }

  taosHashCleanup(pHash);
  return 0;
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
