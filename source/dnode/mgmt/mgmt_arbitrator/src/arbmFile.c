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
#include "tjson.h"

#define MAX_CONTENT_LEN 2 * 1024 * 1024

SArbitratorObj **arbmGetArbitratorListFromHash(SArbitratorMgmt *pMgmt, int32_t *numOfArbitrators) {
  taosThreadRwlockRdlock(&pMgmt->lock);

  int32_t          num = 0;
  int32_t          size = taosHashGetSize(pMgmt->hash);
  SArbitratorObj **pArbitrators = taosMemoryCalloc(size, sizeof(SArbitratorObj *));

  void *pIter = taosHashIterate(pMgmt->hash, NULL);
  while (pIter) {
    SArbitratorObj **ppArbitrator = pIter;
    SArbitratorObj  *pArbitrator = *ppArbitrator;
    if (pArbitrator && num < size) {
      int32_t refCount = atomic_add_fetch_32(&pArbitrator->refCount, 1);
      // dTrace("vgId:%d, acquire arbitrator list, ref:%d", pArbitrator->vgId, refCount);
      pArbitrators[num++] = (*ppArbitrator);
      pIter = taosHashIterate(pMgmt->hash, pIter);
    } else {
      taosHashCancelIterate(pMgmt->hash, pIter);
    }
  }

  taosThreadRwlockUnlock(&pMgmt->lock);
  *numOfArbitrators = num;

  return pArbitrators;
}

static int32_t arbmDecodeArbitratorList(SJson *pJson, SArbitratorMgmt *pMgmt, SArbWrapperCfg **ppCfgs,
                                        int32_t *numOfArbitrators) {
  int32_t         code = -1;
  SArbWrapperCfg *pCfgs = NULL;
  *ppCfgs = NULL;

  SJson *arbitrators = tjsonGetObjectItem(pJson, "arbitrators");
  if (arbitrators == NULL) return -1;

  int32_t arbitratorsNum = cJSON_GetArraySize(arbitrators);
  if (arbitratorsNum > 0) {
    pCfgs = taosMemoryCalloc(arbitratorsNum, sizeof(SArbWrapperCfg));
    if (pCfgs == NULL) return -1;
  }

  for (int32_t i = 0; i < arbitratorsNum; ++i) {
    SJson *arbitrator = tjsonGetArrayItem(arbitrators, i);
    if (arbitrator == NULL) goto _OVER;

    SArbWrapperCfg *pCfg = &pCfgs[i];
    tjsonGetInt32ValueFromDouble(arbitrator, "arbitratorId", pCfg->arbitratorId, code);
    if (code < 0) goto _OVER;
    tjsonGetInt32ValueFromDouble(arbitrator, "dropped", pCfg->dropped, code);
    if (code < 0) goto _OVER;

    snprintf(pCfg->path, sizeof(pCfg->path), "%s%sarbitrator%d", pMgmt->path, TD_DIRSEP, pCfg->arbitratorId);
  }

  code = 0;
  *ppCfgs = pCfgs;
  *numOfArbitrators = arbitratorsNum;

_OVER:
  if (*ppCfgs == NULL) taosMemoryFree(pCfgs);
  return code;
}

int32_t arbmGetArbitratorListFromFile(SArbitratorMgmt *pMgmt, SArbWrapperCfg **ppCfgs, int32_t *numOfArbitrators) {
  int32_t         code = -1;
  TdFilePtr       pFile = NULL;
  char           *pData = NULL;
  SJson          *pJson = NULL;
  char            file[PATH_MAX] = {0};
  SArbWrapperCfg *pCfgs = NULL;
  snprintf(file, sizeof(file), "%s%s%s", pMgmt->path, TD_DIRSEP, ARB_MGMT_INFO_FNAME);

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("arbitrator file:%s not exist", file);
    return 0;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to open arbitrator file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to fstat mnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (taosReadFile(pFile, pData, size) != size) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to read arbitrator file:%s since %s", file, terrstr());
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (arbmDecodeArbitratorList(pJson, pMgmt, ppCfgs, numOfArbitrators) < 0) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read arbitrator file %s", file);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read arbitrator file:%s since %s", file, terrstr());
  }
  return code;
}

static int32_t arbmEncodeArbitratorList(SJson *pJson, SArbitratorObj **ppArbitrators, int32_t numOfArbitrators) {
  SJson *arbitrators = tjsonCreateArray();
  if (arbitrators == NULL) return -1;
  if (tjsonAddItemToObject(pJson, "arbitrators", arbitrators) < 0) return -1;

  for (int32_t i = 0; i < numOfArbitrators; ++i) {
    SArbitratorObj *pArbitrator = ppArbitrators[i];
    if (pArbitrator == NULL) continue;

    SJson *arbitrator = tjsonCreateObject();
    if (arbitrator == NULL) return -1;
    if (tjsonAddDoubleToObject(arbitrator, "arbitratorId", pArbitrator->arbitratorId) < 0) return -1;
    if (tjsonAddDoubleToObject(arbitrator, "dropped", pArbitrator->dropped) < 0) return -1;
    if (tjsonAddItemToArray(arbitrators, arbitrator) < 0) return -1;
  }

  return 0;
}

int32_t arbmWriteArbitratorListToFile(SArbitratorMgmt *pMgmt) {
  int32_t          code = -1;
  char            *buffer = NULL;
  SJson           *pJson = NULL;
  TdFilePtr        pFile = NULL;
  SArbitratorObj **ppArbitrators = NULL;
  char             file[PATH_MAX] = {0};
  char             realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%s%s", pMgmt->path, TD_DIRSEP, ARB_MGMT_INFO_FNAME_TMP);
  snprintf(realfile, sizeof(realfile), "%s%s%s", pMgmt->path, TD_DIRSEP, ARB_MGMT_INFO_FNAME);

  int32_t numOfArbitrators = 0;
  ppArbitrators = arbmGetArbitratorListFromHash(pMgmt, &numOfArbitrators);
  if (ppArbitrators == NULL) goto _OVER;

  terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) goto _OVER;
  if (arbmEncodeArbitratorList(pJson, ppArbitrators, numOfArbitrators) != 0) goto _OVER;
  buffer = tjsonToString(pJson);
  if (buffer == NULL) goto _OVER;
  terrno = 0;

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) goto _OVER;

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) goto _OVER;
  if (taosFsyncFile(pFile) < 0) goto _OVER;

  taosCloseFile(&pFile);
  if (taosRenameFile(file, realfile) != 0) goto _OVER;

  code = 0;
  dInfo("succeed to write arbitrators file:%s, arbitrators:%d", realfile, numOfArbitrators);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) taosCloseFile(&pFile);
  if (ppArbitrators != NULL) {
    for (int32_t i = 0; i < numOfArbitrators; ++i) {
      SArbitratorObj *pArbitrator = ppArbitrators[i];
      if (pArbitrator != NULL) {
        arbmReleaseArbitrator(pMgmt, pArbitrator);
      }
    }
    taosMemoryFree(ppArbitrators);
  }

  if (code != 0) {
    if (terrno == 0) terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write arbitrators file:%s since %s, arbitrators:%d", realfile, terrstr(), numOfArbitrators);
  }
  return code;
}
