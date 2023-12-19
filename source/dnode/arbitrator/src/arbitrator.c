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

int32_t arbitratorProcessGetAribtratorVgIdsRsp(SArbitrator *pArb, SRpcMsg *pMsg) { return -1; }

void arbitratorProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SArbitrator *pArb = pInfo->ahandle;
  int32_t          code = -1;

  arbTrace("msg:%p, get from arbitrator-mgmt queue", pMsg);
  switch (pMsg->msgType) {
    case TDMT_MND_GET_ARBITRATORS_RSP:
      code = arbitratorProcessGetAribtratorVgIdsRsp(pArb, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      arbError("msg:%p, not processed in arbitrator-mgmt queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0) {
      if (terrno != 0) code = terrno;
      arbError("msg:%p, failed to process since %s, type:%s", pMsg, tstrerror(code), TMSG_INFO(pMsg->msgType));
    }
    //arbmSendRsp(pMsg, code);
  }

  arbTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}
