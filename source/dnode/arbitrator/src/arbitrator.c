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

static int32_t arbitratorProcessSetGroupsReq(SArbitrator *pArb, SRpcMsg *pMsg) {
  SArbSetGroupsReq setReq = {0};
  if (tDeserializeSArbSetGroupsReq(pMsg->pCont, pMsg->contLen, &setReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (setReq.arbId != pArb->arbId) {
    terrno = TSDB_CODE_INVALID_MSG;
    arbError("arbId not matched local:%d, msg:%d", pArb->arbId, setReq.arbId);
    return -1;
  }

  size_t sz = taosArrayGetSize(setReq.groups);
  for (size_t i; i < sz; i++) {
    SArbitratorGroupInfo *pInfo = taosArrayGet(setReq.groups, i);
    int32_t   groupId = pInfo->groupId;

    SArbGroup* pGroup = taosHashGet(pArb->arbGroupMap, &groupId, sizeof(int32_t));
    if (pGroup) {
      // TODO(LSG): handle group update
      continue;
    }

    SArbGroup group = {0};
    for (int8_t j; j < pInfo->replica; j++) {
      SReplica *pReplica = &pInfo->replicas[j];
      int32_t   dnodeId = pReplica->id;
      group.members[j].info.dnodeId = dnodeId;
      group.members[j].state.nextHbSeq = 0;
      group.members[j].state.responsedHbSeq = -1;

      SArbDnode *pArbDnode = taosHashGet(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t));
      if (!pArbDnode) {
        taosArrayPush(pArbDnode->groupIds, &groupId);
        continue;
      }
      SArbDnode arbDnode = {0};
      arbDnode.port = pReplica->port;
      memcpy(arbDnode.fqdn, pReplica->fqdn, TSDB_FQDN_LEN);
      arbDnode.groupIds = taosArrayInit(16, sizeof(int32_t));
      taosArrayPush(arbDnode.groupIds, &groupId);
      taosHashPut(pArb->arbDnodeMap, &dnodeId, sizeof(int32_t), &arbDnode, sizeof(SArbDnode));
    }
    taosHashPut(pArb->arbGroupMap, &groupId, sizeof(int32_t), &group, sizeof(SArbGroup));
  }

  arbInfo("arbId:%d, save config while process set groups", pArb->arbId);
  // TODO(lsg): update disk data
  // if (arbitratorUpdateDiskData(pArb->path, &pArb->arbInfo) < 0) {
  //   return -1;
  // }

  return 0;
}

static int32_t arbitratorProcessArbHeartBeatTimer(SArbitrator *pArb, SRpcMsg *pMsg) {
  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);

  size_t keyLen = 0;
  void *pIter = taosHashIterate(pArb->arbDnodeMap, NULL);
  while (pIter) {
    int32_t dnodeId = *(int32_t *)taosHashGetKey(pIter, &keyLen);
    SArbDnode *pArbDnode = pIter;

    SVArbHeartBeatReq req = {0};
    req.arbId = pArb->arbId;
    memcpy(req.arbToken, pArb->arbToken, TD_ARB_TOKEN_SIZE);
    req.dnodeId = dnodeId;

    size_t sz = taosArrayGetSize(pArbDnode->groupIds);
    req.arbSeqArray = taosArrayInit(sz, sizeof(SVArbHeartBeatSeq));
    for (size_t i=0;i<sz;i++) {
      // TODO(LSG): finish this
      // SArbitratorGroup arb
      // int32_t vgId = taosArrayGet(pArbDnode->groupIds, i);
      // taosArrayPush(req.arbSeqArray, );
    }

    int32_t contLen = tSerializeSVArbHeartBeatReq(NULL, 0, &req);
    void   *pHead = rpcMallocCont(contLen);
    tSerializeSVArbHeartBeatReq(pHead, contLen, &req);

    SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_VND_ARB_HEARTBEAT};

    SEpSet epset = {.inUse = 0, .numOfEps = 1};
    addEpIntoEpSet(&epset, pArbDnode->fqdn, pArbDnode->port);
    pArb->msgCb.sendReqFp(&epset, &rpcMsg);

    tFreeSVArbHeartBeatReq(&req);
    pIter = taosHashIterate(pHash, pIter);
  }

  taosHashCleanup(pHash);
  return 0;
}

static void arbitratorUpdateVnodeToken(SArbitrator *pArb, int32_t dnodeId, SArray *arbSeqTokenArray) {
  size_t sz = taosArrayGetSize(arbSeqTokenArray);
  for (size_t i = 0; i < sz; i++) {
    SVArbHeartBeatSeqToken *pTk = taosArrayGet(arbSeqTokenArray, i);
    int64_t                 key = arbitratorGenerateHbSeqKey(dnodeId, pTk->vgId);
    SArbHbSeqNum           *pSeqNum = taosHashAcquire(pArb->hbSeqMap, &key, sizeof(int64_t));
    if (!pSeqNum) {
      arbInfo("arbId:%d, update dnode:%d vnode:%d token failed, no local seqNum found", pArb->arbId, dnodeId,
              pTk->vgId);
      continue;
    }

    if (pSeqNum->lastHbSeq >= pTk->seqNo) {
      arbInfo("arbId:%d, update dnode:%d vnode:%d token failed, seqNo expired, msg:%d local:%d", pArb->arbId,
              dnodeId, pTk->vgId, pTk->seqNo, pSeqNum->lastHbSeq);
      taosHashRelease(pArb->hbSeqMap, pSeqNum);
      continue;
    }

    // update local
    pSeqNum->lastHbSeq = pTk->seqNo;

    taosHashRelease(pArb->hbSeqMap, pSeqNum);
  }
}

static int32_t arbitratorProcessArbHeartBeatRsp(SArbitrator *pArb, SRpcMsg *pMsg) {
  SVArbHeartBeatRsp arbHbRsp = {0};
  if (tDeserializeSVArbHeartBeatRsp(pMsg->pCont, pMsg->contLen, &arbHbRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  if (arbHbRsp.arbId != pArb->arbId) {
    terrno = TSDB_CODE_INVALID_MSG;
    arbError("arbId not matched local:%d, msg:%d", pArb->arbId, arbHbRsp.arbId);
    goto _OVER;
  }

  if (strcmp(arbHbRsp.arbToken, pArb->arbToken) != 0) {
    terrno = TSDB_CODE_ARB_TOKEN_MISMATCH;
    arbInfo("arbId:%d, arbToken not matched local:%s, msg:%s", pArb->arbId, pArb->arbToken, arbHbRsp.arbToken);
    goto _OVER;
  }

  arbitratorUpdateVnodeToken(pArb, arbHbRsp.dnodeId, arbHbRsp.arbSeqTokenArray);

_OVER:
  tFreeSVArbHeartBeatRsp(&arbHbRsp);
  return terrno == TSDB_CODE_SUCCESS ? 0 : -1;
}

void arbitratorProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SArbitrator *pArb = pInfo->ahandle;
  int32_t      code = -1;

  arbTrace("msg:%p, get from arb-mgmt queue", pMsg);
  switch (pMsg->msgType) {
    case TDMT_ARB_SET_VGROUPS:
      code = arbitratorProcessSetGroupsReq(pArb, pMsg);
      break;
    case TDMT_ARB_HEARTBEAT_TIMER:
      code = arbitratorProcessArbHeartBeatTimer(pArb, pMsg);
      break;
    case TDMT_VND_ARB_HEARTBEAT_RSP:
      code = arbitratorProcessArbHeartBeatRsp(pArb, pMsg);
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
