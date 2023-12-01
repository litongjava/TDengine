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

#include "executor.h"
#include "arbInt.h"
#include "query.h"
#include "qworker.h"

SArbitrator *arbOpen(const SArbitratorOpt *pOption) {
  SArbitrator *pArbitrator = taosMemoryCalloc(1, sizeof(SArbitrator));
  if (NULL == pArbitrator) {
    qError("calloc SArbitrator failed");
    return NULL;
  }

  // if (qWorkerInit(NODE_TYPE_ARBITRATOR, pArbitrator->arbId, (void **)&pArbitrator->pQuery, &pOption->msgCb)) {
  //   taosMemoryFreeClear(pArbitrator);
  //   return NULL;
  // }

  // pArbitrator->msgCb = pOption->msgCb;
  return pArbitrator;
}

void arbClose(SArbitrator *pArbitrator) {
  // qWorkerDestroy((void **)&pArbitrator->pQuery);
  taosMemoryFree(pArbitrator);
}

int32_t arbPreprocessQueryMsg(SArbitrator *pArbitrator, SRpcMsg *pMsg) {
  return 0;
  // if (TDMT_SCH_QUERY != pMsg->msgType && TDMT_SCH_MERGE_QUERY != pMsg->msgType) {
  //   return 0;
  // }

  // return qWorkerPreprocessQueryMsg(pArbitrator->pQuery, pMsg, false);
}

int32_t arbProcessQueryMsg(SArbitrator *pArbitrator, int64_t ts, SRpcMsg *pMsg) {
  return 0;
  // int32_t     code = -1;
  // SReadHandle handle = {.pMsgCb = &pArbitrator->msgCb};
  // qTrace("message in arbitrator queue is processing");

  // switch (pMsg->msgType) {
  //   case TDMT_SCH_QUERY:
  //   case TDMT_SCH_MERGE_QUERY:
  //     code = qWorkerProcessQueryMsg(&handle, pArbitrator->pQuery, pMsg, ts);
  //     break;
  //   case TDMT_SCH_QUERY_CONTINUE:
  //     code = qWorkerProcessCQueryMsg(&handle, pArbitrator->pQuery, pMsg, ts);
  //     break;
  //   case TDMT_SCH_FETCH:
  //   case TDMT_SCH_MERGE_FETCH:
  //     code = qWorkerProcessFetchMsg(pArbitrator, pArbitrator->pQuery, pMsg, ts);
  //     break;
  //   case TDMT_SCH_CANCEL_TASK:
  //     // code = qWorkerProcessCancelMsg(pArbitrator, pArbitrator->pQuery, pMsg, ts);
  //     break;
  //   case TDMT_SCH_DROP_TASK:
  //     code = qWorkerProcessDropMsg(pArbitrator, pArbitrator->pQuery, pMsg, ts);
  //     break;
  //   case TDMT_VND_TMQ_CONSUME:
  //     // code =  tqProcessConsumeReq(pArbitrator->pTq, pMsg);
  //     // break;
  //   case TDMT_SCH_QUERY_HEARTBEAT:
  //     code = qWorkerProcessHbMsg(pArbitrator, pArbitrator->pQuery, pMsg, ts);
  //     break;
  //   case TDMT_SCH_TASK_NOTIFY:
  //     code = qWorkerProcessNotifyMsg(pArbitrator, pArbitrator->pQuery, pMsg, ts);
  //     break;
  //   default:
  //     qError("unknown msg type:%d in arbitrator queue", pMsg->msgType);
  //     terrno = TSDB_CODE_APP_ERROR;
  // }

  // if (code == 0) return TSDB_CODE_ACTION_IN_PROGRESS;
  // return code;
}
