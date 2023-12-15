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
#include "arbitrator.h"

static int arbitratorEncodeInfo(const SArbitratorInfo *pInfo, char **ppData) {
  SJson *pJson;
  char  *pData;

  *ppData = NULL;

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    return -1;
  }

  if (tjsonAddIntegerToObject(pJson, "arbitratorId", pInfo->arbitratorId) < 0) return -1;
  // TODO(LSG): VGROUPS

  pData = tjsonToString(pJson);
  if (pData == NULL) {
    goto _err;
  }

  tjsonDelete(pJson);

  *ppData = pData;
  return 0;

_err:
  tjsonDelete(pJson);
  return -1;
}

static int arbitratorDecodeInfo(uint8_t *pData, SArbitratorInfo *pInfo) {
  SJson *pJson = NULL;

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    return -1;
  }

  int32_t code;
  tjsonGetNumberValue(pJson, "arbitratorId", pInfo->arbitratorId, code);
  if (code < 0) {
    goto _err;
  }
  // TODO(LSG): VGROUPS

  tjsonDelete(pJson);

  return 0;

_err:
  tjsonDelete(pJson);
  return -1;
}

static int32_t arbitratorLoadInfo(const char *dir, SArbitratorInfo *pInfo) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile = NULL;
  char     *pData = NULL;
  int64_t   size;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME);

  // read info
  pFile = taosOpenFile(fname, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  if (taosReadFile(pFile, pData, size) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pData[size] = '\0';

  taosCloseFile(&pFile);

  // decode info
  if (arbitratorDecodeInfo(pData, pInfo) < 0) {
    taosMemoryFree(pData);
    return -1;
  }

  taosMemoryFree(pData);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(pData);
  return -1;
}

static int32_t arbitratorSaveInfo(const char *dir, SArbitratorInfo *pInfo) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile;
  char     *data;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME_TMP);

  // encode info
  data = NULL;

  if (arbitratorEncodeInfo(pInfo, &data) < 0) {
    arbError("failed to encode json info.");
    return -1;
  }

  // save info to a arbitrator_tmp.json
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    arbError("failed to open info file:%s for write:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, data, strlen(data)) < 0) {
    arbError("failed to write info file:%s error:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pFile) < 0) {
    arbError("failed to fsync info file:%s error:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosCloseFile(&pFile);

  // free info binary
  taosMemoryFree(data);

  arbInfo("arbitratorId:%d, arbitrator info is saved, fname:%s", pInfo->arbitratorId, fname);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(data);
  return -1;
}

static int32_t arbitratorCommitInfo(const char *dir) {
  char fname[TSDB_FILENAME_LEN];
  char tfname[TSDB_FILENAME_LEN];

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME);
  snprintf(tfname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, ARB_INFO_FNAME_TMP);

  if (taosRenameFile(tfname, fname) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  arbInfo("arbitrator info is committed, dir:%s", dir);
  return 0;
}

int32_t arbitratorCreate(const char *path, int32_t arbitratorId) {
  SArbitratorInfo info = {0};
  char            dir[TSDB_FILENAME_LEN] = {0};

  // create arbitrator env
  if (taosMkDir(path)) {
    arbError("arbitratorId:%d, failed to prepare arbitrator dir since %s, path: %s", arbitratorId, strerror(errno),
             path);
    return TAOS_SYSTEM_ERROR(errno);
  }

  info.arbitratorId = arbitratorId;

  SArbitratorInfo oldInfo = {0};
  oldInfo.arbitratorId = -1;
  if (arbitratorLoadInfo(path, &oldInfo) == 0) {
    arbWarn("vgId:%d, arbitrator config info already exists at %s.", oldInfo.arbitratorId, path);
    return (oldInfo.arbitratorId == info.arbitratorId) ? 0 : -1;
  }

  arbInfo("arbitratorId:%d, save config while create", info.arbitratorId);
  if (arbitratorSaveInfo(path, &info) < 0 || arbitratorCommitInfo(path) < 0) {
    arbError("arbitratorId:%d, failed to save arbitrator config since %s", arbitratorId, tstrerror(terrno));
    return -1;
  }

  arbInfo("arbitratorId:%d, arbitrator is created", info.arbitratorId);
  return 0;
}

void arbitratorDestroy(const char *path) {
  arbInfo("path:%s is removed while destroy arbitrator", path);
  taosRemoveDir(path);
}

SArbitrator *arbitratorOpen(const char *path, SMsgCb msgCb) {
  SArbitrator    *pArbitrator = NULL;
  SArbitratorInfo info = {0};
  char            dir[TSDB_FILENAME_LEN] = {0};
  int32_t         ret = 0;
  terrno = TSDB_CODE_SUCCESS;

  info.arbitratorId = -1;

  // load arbitrator info
  ret = arbitratorLoadInfo(path, &info);
  if (ret < 0) {
    arbError("failed to open arbitrator from %s since %s", path, tstrerror(terrno));
    terrno = TSDB_CODE_NEED_RETRY;
    return NULL;
  }

  if (taosMkDir(path)) {
    arbError("arbitratorId:%d, failed to prepare arbitrator dir since %s, path: %s", info.arbitratorId, strerror(errno),
             path);
    return NULL;
  }

  // create handle
  pArbitrator = taosMemoryCalloc(1, sizeof(*pArbitrator) + strlen(path) + 1);
  if (pArbitrator == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    arbError("arbitratorId:%d, failed to open arbitrator since %s", info.arbitratorId, tstrerror(terrno));
    return NULL;
  }

  pArbitrator->arbitratorId = info.arbitratorId;

  return pArbitrator;

_err:
  taosMemoryFree(pArbitrator);
  return NULL;
}

void arbitratorClose(SArbitrator *pArbitrator) {
  if (pArbitrator) {
    taosMemoryFree(pArbitrator);
  }
}

// start the sync timer after the queue is ready
int32_t arbitratorStart(SArbitrator *pArbitrator) {
  return -1;
  // ASSERT(pArbitrator);
  // return arbitratorSyncStart(pArbitrator);
}

void arbitratorStop(SArbitrator *pArbitrator) {}
