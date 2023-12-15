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
  *required = true;
  return 0;
  // return dmReadFile(pInput->path, pInput->name, required);
}

SArbitratorObj *arbmAcquireArbitratorImpl(SArbitratorMgmt *pMgmt, int32_t arbitratorId, bool strict) {
  SArbitratorObj *pArbitrator = NULL;

  taosThreadRwlockRdlock(&pMgmt->lock);
  taosHashGetDup(pMgmt->hash, &arbitratorId, sizeof(int32_t), (void *)&pArbitrator);
  if (pArbitrator == NULL || strict && (pArbitrator->dropped || pArbitrator->failed)) {
    terrno = TSDB_CODE_ARB_INVALID_ARB_ID;
    pArbitrator = NULL;
  } else {
    int32_t refCount = atomic_add_fetch_32(&pArbitrator->refCount, 1);
    // dTrace("arbitratorId:%d, acquire arbitrator, ref:%d", pArbitrator->arbitratorId, refCount);
  }
  taosThreadRwlockUnlock(&pMgmt->lock);

  return pArbitrator;
}

SArbitratorObj *arbmAcquireArbitrator(SArbitratorMgmt *pMgmt, int32_t arbitratorId) {
  return arbmAcquireArbitratorImpl(pMgmt, arbitratorId, true);
}

void arbmReleaseArbitrator(SArbitratorMgmt *pMgmt, SArbitratorObj *pArbitrator) {
  if (pArbitrator == NULL) return;

  taosThreadRwlockRdlock(&pMgmt->lock);
  int32_t refCount = atomic_sub_fetch_32(&pArbitrator->refCount, 1);
  // dTrace("arbitratorId:%d, release arbitrator, ref:%d", pArbitrator->arbitratorId, refCount);
  taosThreadRwlockUnlock(&pMgmt->lock);
}

static void arbmCleanup(SArbitratorMgmt *pMgmt) {
  arbmStopWorker(pMgmt);
  taosMemoryFree(pMgmt);
}

static void arbmFreeArbitratorObj(SArbitratorObj **ppArbitrator) {
  if (!ppArbitrator || !(*ppArbitrator)) return;

  SArbitratorObj *pArbitrator = *ppArbitrator;
  taosMemoryFree(pArbitrator->path);
  taosMemoryFree(pArbitrator);
  ppArbitrator[0] = NULL;
}

int32_t arbmOpenArbitrator(SArbitratorMgmt *pMgmt, SArbWrapperCfg *pCfg, SArbitrator *pImpl) {
  SArbitratorObj *pArbitrator = taosMemoryCalloc(1, sizeof(SArbitratorObj));
  if (pArbitrator == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pArbitrator->arbitratorId = pCfg->arbitratorId;
  pArbitrator->refCount = 0;
  pArbitrator->dropped = 0;
  pArbitrator->failed = 0;
  pArbitrator->path = taosStrdup(pCfg->path);
  pArbitrator->pImpl = pImpl;

  if (pArbitrator->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pArbitrator);
    return -1;
  }

  if (pImpl) {
    if (arbmAllocQueue(pMgmt, pArbitrator) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(pArbitrator->path);
      taosMemoryFree(pArbitrator);
      return -1;
    }
  } else {
    pArbitrator->failed = 1;
  }

  taosThreadRwlockWrlock(&pMgmt->lock);
  SArbitratorObj *pOld = NULL;
  taosHashGetDup(pMgmt->hash, &pArbitrator->arbitratorId, sizeof(int32_t), (void *)&pOld);
  if (pOld) {
    ASSERT(pOld->failed);
    arbmFreeArbitratorObj(&pOld);
  }
  int32_t code =
      taosHashPut(pMgmt->hash, &pArbitrator->arbitratorId, sizeof(int32_t), &pArbitrator, sizeof(SArbitratorObj *));
  taosThreadRwlockUnlock(&pMgmt->lock);

  return code;
}

void arbmCloseArbitrator(SArbitratorMgmt *pMgmt, SArbitratorObj *pArbitrator) {
  char path[TSDB_FILENAME_LEN] = {0};

  taosThreadRwlockWrlock(&pMgmt->lock);
  taosHashRemove(pMgmt->hash, &pArbitrator->arbitratorId, sizeof(int32_t));
  taosThreadRwlockUnlock(&pMgmt->lock);
  arbmReleaseArbitrator(pMgmt, pArbitrator);

  if (pArbitrator->failed) {
    ASSERT(pArbitrator->pImpl == NULL);
    goto _closed;
  }

  dInfo("arbitratorId:%d, wait for arbitrator ref become 0", pArbitrator->arbitratorId);
  while (pArbitrator->refCount > 0) taosMsleep(10);

  dInfo("arbitratorId:%d, wait for arbitrator write queue:%p is empty, thread:%08" PRId64, pArbitrator->arbitratorId,
        pArbitrator->pWriteW.queue, pArbitrator->pWriteW.queue->threadId);
  arbmFreeQueue(pMgmt, pArbitrator);

  dInfo("arbitratorId:%d, all arbitrator queues is empty", pArbitrator->arbitratorId);


  arbitratorClose(pArbitrator->pImpl);
  pArbitrator->pImpl = NULL;

_closed:
  dInfo("arbitratorId:%d, arbitrator is closed", pArbitrator->arbitratorId);

  if (pArbitrator->dropped) {
    dInfo("arbitratorId:%d, arbitrator is destroyed, dropped:%d", pArbitrator->arbitratorId, pArbitrator->dropped);
    snprintf(path, TSDB_FILENAME_LEN, "arbitrator%sarbitrator%d", TD_DIRSEP, pArbitrator->arbitratorId);
    arbitratorDestroy(path);
  }

  arbmFreeArbitratorObj(&pArbitrator);
}

static void *arbmOpenArbitratorInThread(void *param) {
  SArbitratorThread *pThread = param;
  SArbitratorMgmt   *pMgmt = pThread->pMgmt;
  char               path[TSDB_FILENAME_LEN];

  dInfo("thread:%d, start to open arbitrator", pThread->threadIndex);
  setThreadName("open-arbitrators");

  SArbWrapperCfg *pCfg = pThread->pCfg;

  char stepDesc[TSDB_STEP_DESC_LEN] = {0};
  snprintf(stepDesc, TSDB_STEP_DESC_LEN, "arbitratorId:%d, start to restore, %d of %d have been opened",
           pCfg->arbitratorId, pMgmt->state.openArbitrators, pMgmt->state.totalArbitrators);
  tmsgReportStartup("arbitrator-open", stepDesc);

  snprintf(path, TSDB_FILENAME_LEN, "arbitrator%sarbitrator%d", TD_DIRSEP, pCfg->arbitratorId);

  SArbitrator *pImpl = arbitratorOpen(path, pMgmt->msgCb);
  if (pImpl == NULL) {
    dError("arbitratorId:%d, failed to open arbitrator by thread:%d since %s", pCfg->arbitratorId, pThread->threadIndex,
           terrstr());
    if (terrno != TSDB_CODE_NEED_RETRY) {
      pThread->failed++;
      goto _err_out;
    }
  }

  // if (arbmOpenArbitrator(pMgmt, pCfg, pImpl) != 0) {
  //   dError("arbitratorId:%d, failed to open arbitrator by thread:%d", pCfg->arbitratorId, pThread->threadIndex);
  //   pThread->failed++;
  //   goto _err_out;
  // }

  dInfo("arbitratorId:%d, is opened by thread:%d", pCfg->arbitratorId, pThread->threadIndex);
  pThread->opened++;
  atomic_add_fetch_32(&pMgmt->state.openArbitrators, 1);

_err_out:
  dInfo("thread:%d, opened:%d failed:%d", pThread->threadIndex, pThread->opened, pThread->failed);
  return NULL;
}

static int32_t arbmOpenArbitrators(SArbitratorMgmt *pMgmt) {
  pMgmt->hash = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pMgmt->hash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    dError("failed to init Arbitrator hash since %s", terrstr());
    return -1;
  }

  SArbWrapperCfg *pCfgs = NULL;
  int32_t         numOfArbitrators = 0;
  if (arbmGetArbitratorListFromFile(pMgmt, &pCfgs, &numOfArbitrators) != 0) {
    dInfo("failed to get Arbitrator list from disk since %s", terrstr());
    return -1;
  }

  pMgmt->state.totalArbitrators = numOfArbitrators;

  SArbitratorThread *threads = taosMemoryCalloc(numOfArbitrators, sizeof(SArbitratorThread));
  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].pCfg = taosMemoryCalloc(1, sizeof(SArbWrapperCfg));
  }

  for (int32_t v = 0; v < numOfArbitrators; ++v) {
    SArbitratorThread *pThread = &threads[v];
    pThread->pCfg = &pCfgs[v];
  }

  dInfo("open %d Arbitrators", numOfArbitrators);

  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    SArbitratorThread *pThread = &threads[t];

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&pThread->thread, &thAttr, arbmOpenArbitratorInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to open Arbitrator, reason:%s", pThread->threadIndex, strerror(errno));
    }

    taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    SArbitratorThread *pThread = &threads[t];
    if (taosCheckPthreadValid(pThread->thread)) {
      taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->pCfg);
  }
  taosMemoryFree(threads);
  taosMemoryFree(pCfgs);

  if (pMgmt->state.openArbitrators != pMgmt->state.totalArbitrators) {
    dError("there are total Arbitrators:%d, opened:%d", pMgmt->state.totalArbitrators, pMgmt->state.openArbitrators);
    terrno = TSDB_CODE_ARB_INIT_FAILED;
    return -1;
  }

  if (arbmWriteArbitratorListToFile(pMgmt) != 0) {
    dError("failed to write Arbitrator list since %s", terrstr());
    return -1;
  }

  dInfo("successfully opened %d Arbitrators", pMgmt->state.totalArbitrators);
  return 0;
}

static void *arbmThreadFp(void *param) {
  SArbitratorMgmt *pMgmt = param;
  int64_t          lastTime = 0;
  setThreadName("arb-timer");

  while (1) {
    lastTime++;
    taosMsleep(100);
    if (pMgmt->stop) break;
    if (lastTime % 10 != 0) continue;

    int64_t sec = lastTime / 10;
    if (sec % (ARBITRATOR_TIMEOUT_SEC / 2) == 0) {
      // arbmCheckSyncTimeout(pMgmt);
    }
  }

  return NULL;
}

static int32_t arbmInitTimer(SArbitratorMgmt *pMgmt) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->thread, &thAttr, arbmThreadFp, pMgmt) != 0) {
    dError("failed to create arbitrator timer thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void arbmCleanupTimer(SArbitratorMgmt *pMgmt) {
  pMgmt->stop = true;
  if (taosCheckPthreadValid(pMgmt->thread)) {
    taosThreadJoin(pMgmt->thread, NULL);
    taosThreadClear(&pMgmt->thread);
  }
}

static int32_t arbmInit(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t code = -1;

  SArbitratorMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SArbitratorMgmt));
  if (pMgmt == NULL) goto _OVER;

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)arbmPutRpcMsgToQueue;
  pMgmt->msgCb.qsizeFp = (GetQueueSizeFp)arbmGetQueueSize;
  pMgmt->msgCb.mgmt = pMgmt;

  if (arbmStartWorker(pMgmt) != 0) {
    dError("failed to start arbitrator worker since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("arbitrator-worker", "initialized");

  if (arbmOpenArbitrators(pMgmt) != 0) {
    dError("failed to open all arbitrators since %s", terrstr());
    goto _OVER;
  }
  tmsgReportStartup("arbitrator-arbitrators", "initialized");

  if (udfcOpen() != 0) {
    dError("failed to open udfc in arbitrator");
    goto _OVER;
  }

  code = 0;

_OVER:
  pOutput->pMgmt = pMgmt;
  if (code == 0) {
    pOutput->pMgmt = pMgmt;
  } else {
    dError("failed to init arbitrator-mgmt since %s", terrstr());
    arbmCleanup(pMgmt);
  }

  return code;
}

static void *arbmRestoreArbitratorInThread(void *param) {
  SArbitratorThread *pThread = param;
  SArbitratorMgmt   *pMgmt = pThread->pMgmt;

  dInfo("thread:%d, start to restore arbitrator", pThread->threadIndex);
  setThreadName("restore-arbitrators");

  SArbitratorObj *pArbitrator = pThread->pArbitrator;
  if (pArbitrator->failed) {
    dError("arbitratorId:%d, cannot restore a arbitrator in failed mode.", pArbitrator->arbitratorId);
    goto _err_out;
  }

  ASSERT(pArbitrator->pImpl);

  char stepDesc[TSDB_STEP_DESC_LEN] = {0};
  snprintf(stepDesc, TSDB_STEP_DESC_LEN, "arbitratorId:%d, start to restore, %d of %d have been restored",
           pArbitrator->arbitratorId, pMgmt->state.openArbitrators, pMgmt->state.totalArbitrators);
  tmsgReportStartup("arbitrator-restore", stepDesc);

  int32_t code = arbitratorStart(pArbitrator->pImpl);
  if (code != 0) {
    dError("arbitratorId:%d, failed to restore arbitrator by thread:%d", pArbitrator->arbitratorId,
           pThread->threadIndex);
    pThread->failed++;
  } else {
    dInfo("arbitratorId:%d, is restored by thread:%d", pArbitrator->arbitratorId, pThread->threadIndex);
    pThread->opened++;
    atomic_add_fetch_32(&pMgmt->state.openArbitrators, 1);
  }

_err_out:
  dInfo("thread:%d, restored:%d failed:%d", pThread->threadIndex, pThread->opened, pThread->failed);
  return NULL;
}

static int32_t arbmStartArbitrators(SArbitratorMgmt *pMgmt) {
  int32_t          numOfArbitrators = 0;
  SArbitratorObj **ppArbitrators = arbmGetArbitratorListFromHash(pMgmt, &numOfArbitrators);

  SArbitratorThread *threads = taosMemoryCalloc(numOfArbitrators, sizeof(SArbitratorThread));
  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].pArbitrator = taosMemoryCalloc(1, sizeof(SArbitrator *));
  }

  for (int32_t v = 0; v < numOfArbitrators; ++v) {
    SArbitratorThread *pThread = &threads[v];
    if (pThread->pArbitrator != NULL && ppArbitrators != NULL) {
      pThread->pArbitrator = ppArbitrators[v];
    }
  }

  dInfo("restore %d Arbitrators", numOfArbitrators);

  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    SArbitratorThread *pThread = &threads[t];
    TdThreadAttr       thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&pThread->thread, &thAttr, arbmRestoreArbitratorInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to restore Arbitrator since %s", pThread->threadIndex,
             strerror(errno));
    }

    taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < numOfArbitrators; ++t) {
    SArbitratorThread *pThread = &threads[t];
    if (taosCheckPthreadValid(pThread->thread)) {
      taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->pArbitrator);
  }
  taosMemoryFree(threads);

  for (int32_t i = 0; i < numOfArbitrators; ++i) {
    if (ppArbitrators == NULL || ppArbitrators[i] == NULL) continue;
    arbmReleaseArbitrator(pMgmt, ppArbitrators[i]);
  }

  if (ppArbitrators != NULL) {
    taosMemoryFree(ppArbitrators);
  }

  return arbmInitTimer(pMgmt);
}

static void arbmStop(SArbitratorMgmt *pMgmt) { arbmCleanupTimer(pMgmt); }

SMgmtFunc arbmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = arbmInit;
  mgmtFunc.closeFp = (NodeCloseFp)arbmCleanup;
  mgmtFunc.startFp = (NodeStartFp)arbmStartArbitrators;
  mgmtFunc.stopFp = (NodeStopFp)arbmStop;
  mgmtFunc.requiredFp = arbmRequire;
  mgmtFunc.getHandlesFp = arbmGetMsgHandles;

  return mgmtFunc;
}
