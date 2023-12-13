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

int32_t arbProcessMsg(SArbitrator *pArbitrator, int64_t ts, SRpcMsg *pMsg) {
  int32_t     code = -1;
  SReadHandle handle = {.pMsgCb = NULL};
  qTrace("message in arbitrator queue is processing");

  switch (pMsg->msgType) {
    case TDMT_SCH_QUERY_CONTINUE:
      code = qWorkerProcessCQueryMsg(&handle, NULL, pMsg, ts);
      break;
    default:
      qError("unknown msg type:%d in arbitrator queue", pMsg->msgType);
      terrno = TSDB_CODE_APP_ERROR;
  }

  if (code == 0) return TSDB_CODE_ACTION_IN_PROGRESS;
  return code;
}
