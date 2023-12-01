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
#include "libs/function/tudf.h"

static int32_t arbmRequire(const SMgmtInputOpt *pInput, bool *required) {
  return dmReadFile(pInput->path, pInput->name, required);
}

static void arbmInitOption(SArbitratorMgmt *pMgmt, SArbitratorOpt *pOption) { pOption->msgCb = pMgmt->msgCb; }

static void arbmClose(SArbitratorMgmt *pMgmt) {
  if (pMgmt->pArbitrator != NULL) {
    arbmStopWorker(pMgmt);
    arbClose(pMgmt->pArbitrator);
    pMgmt->pArbitrator = NULL;
  }

  taosMemoryFree(pMgmt);
}

static int32_t arbmOpen(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  SArbitratorMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SArbitratorMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)arbmPutRpcMsgToQueue;
  pMgmt->msgCb.qsizeFp = (GetQueueSizeFp)arbmGetQueueSize;
  pMgmt->msgCb.mgmt = pMgmt;

  SArbitratorOpt option = {0};
  arbmInitOption(pMgmt, &option);
  pMgmt->pArbitrator = arbOpen(&option);
  if (pMgmt->pArbitrator == NULL) {
    dError("failed to open arbitrator since %s", terrstr());
    arbmClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("arbitrator-impl", "initialized");

  if (udfcOpen() != 0) {
    dError("arbitrator can not open udfc");
    arbmClose(pMgmt);
    return -1;
  }

  if (arbmStartWorker(pMgmt) != 0) {
    dError("failed to start arbitrator worker since %s", terrstr());
    arbmClose(pMgmt);
    return -1;
  }
  tmsgReportStartup("arbitrator-worker", "initialized");

  pOutput->pMgmt = pMgmt;
  return 0;
}

SMgmtFunc arbmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = arbmOpen;
  mgmtFunc.closeFp = (NodeCloseFp)arbmClose;
  mgmtFunc.createFp = (NodeCreateFp)arbmProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)arbmProcessDropReq;
  mgmtFunc.requiredFp = arbmRequire;
  mgmtFunc.getHandlesFp = arbmGetMsgHandles;

  return mgmtFunc;
}
