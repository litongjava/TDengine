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

static void arbitratorGenerateArbToken(int32_t arbId, char *buf);

static int arbitratorEncodeInfo(const SArbitratorInfo *pInfo, char **ppData) {
  SJson *pJson;
  char  *pData;

  *ppData = NULL;

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    return -1;
  }

  if (tjsonAddIntegerToObject(pJson, "arbId", pInfo->arbId) < 0) goto _err;

  SJson *vgroups = tjsonCreateArray();
  if (vgroups == NULL) goto _err;
  if (tjsonAddItemToObject(pJson, "vgroups", vgroups) < 0) goto _err;

  int32_t vgNum = taosArrayGetSize(pInfo->vgroups);
  for (int i = 0; i < vgNum; i++) {
    SJson *info = tjsonCreateObject();
    if (info == NULL) goto _err;
    if (tjsonAddItemToArray(vgroups, info) < 0) goto _err;
    SArbitratorVgroupInfo *pVgInfo = taosArrayGet(pInfo->vgroups, i);
    if (tjsonAddIntegerToObject(info, "vgId", pVgInfo->vgId) < 0) goto _err;
    if (tjsonAddIntegerToObject(info, "replica", pVgInfo->replica) < 0) goto _err;

    SJson *replicas = tjsonCreateArray();
    if (replicas == NULL) goto _err;
    if (tjsonAddItemToObject(info, "replicas", replicas) < 0) goto _err;
    for (int j = 0; j < pVgInfo->replica; j++) {
      SJson *replica = tjsonCreateObject();
      if (info == NULL) goto _err;
      if (tjsonAddItemToArray(replicas, replica) < 0) goto _err;
      SReplica *pReplica = &pVgInfo->replicas[j];
      if (tjsonAddIntegerToObject(replica, "id", pReplica->id) < 0) goto _err;
      if (tjsonAddIntegerToObject(replica, "port", pReplica->port) < 0) goto _err;
      if (tjsonAddStringToObject(replica, "fqdn", pReplica->fqdn) < 0) goto _err;
    }
  }

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

  if (tjsonGetIntValue(pJson, "arbId", &pInfo->arbId) < 0) goto _err;

  SJson *vgroups = tjsonGetObjectItem(pJson, "vgroups");
  int    vgNum = tjsonGetArraySize(vgroups);
  pInfo->vgroups = taosArrayInit(vgNum, sizeof(SArbitratorVgroupInfo));
  for (int i = 0; i < vgNum; i++) {
    SJson *info = tjsonGetArrayItem(vgroups, i);
    if (info == NULL) goto _err;
    SArbitratorVgroupInfo vgInfo = {0};
    if (tjsonGetIntValue(info, "vgId", &vgInfo.vgId) < 0) goto _err;
    if (tjsonGetTinyIntValue(info, "replica", &vgInfo.replica) < 0) goto _err;
    SJson *replicas = tjsonGetObjectItem(info, "replicas");
    for (int j = 0; j < vgInfo.replica; j++) {
      SJson *replica = tjsonGetArrayItem(replicas, j);
      if (info == NULL) goto _err;
      SReplica *pReplica = &vgInfo.replicas[j];
      if (tjsonGetIntValue(replica, "id", &pReplica->id) < 0) goto _err;
      if (tjsonGetSmallIntValue(replica, "port", &pReplica->port) < 0) goto _err;
      if (tjsonGetStringValue(replica, "fqdn", pReplica->fqdn) < 0) goto _err;
    }
    taosArrayPush(pInfo->vgroups, &vgInfo);
  }

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

  arbInfo("arbId:%d, arbitrator info is saved, fname:%s", pInfo->arbId, fname);

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

int32_t arbitratorUpdateInfo(const char *dir, SArbitratorInfo *pInfo) {
  if (arbitratorSaveInfo(dir, pInfo) < 0 || arbitratorCommitInfo(dir) < 0) {
    arbError("arbId:%d, failed to save arbitrator config since %s", pInfo->arbId, tstrerror(terrno));
    return -1;
  }
  return 0;
}

int32_t arbitratorCreate(const char *path, int32_t arbId) {
  SArbitratorInfo info = {0};
  char            dir[TSDB_FILENAME_LEN] = {0};

  // create arbitrator env
  if (taosMkDir(path)) {
    arbError("arbId:%d, failed to prepare arbitrator dir since %s, path: %s", arbId, strerror(errno), path);
    return TAOS_SYSTEM_ERROR(errno);
  }

  info.arbId = arbId;

  SArbitratorInfo oldInfo = {0};
  oldInfo.arbId = -1;
  if (arbitratorLoadInfo(path, &oldInfo) == 0) {
    arbWarn("vgId:%d, arbitrator config info already exists at %s.", oldInfo.arbId, path);
    return (oldInfo.arbId == info.arbId) ? 0 : -1;
  }

  arbInfo("arbId:%d, save config while create", info.arbId);
  if (arbitratorUpdateInfo(path, &info) < 0) {
    return -1;
  }

  arbInfo("arbId:%d, arbitrator is created", info.arbId);
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

  info.arbId = -1;

  // load arbitrator info
  ret = arbitratorLoadInfo(path, &info);
  if (ret < 0) {
    arbError("failed to open arbitrator from %s since %s", path, tstrerror(terrno));
    terrno = TSDB_CODE_NEED_RETRY;
    return NULL;
  }

  // create handle
  pArbitrator = taosMemoryCalloc(1, sizeof(*pArbitrator) + strlen(path) + 1);
  if (pArbitrator == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    arbError("arbId:%d, failed to open arbitrator since %s", info.arbId, tstrerror(terrno));
    return NULL;
  }

  pArbitrator->arbInfo.arbId = info.arbId;
  pArbitrator->arbInfo.vgroups = info.vgroups;
  arbitratorGenerateArbToken(pArbitrator->arbInfo.arbId, pArbitrator->arbToken);
  pArbitrator->msgCb = msgCb;
  strcpy(pArbitrator->path, path);

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

static void arbitratorGenerateArbToken(int32_t arbId, char *buf) {
  int32_t randVal = taosSafeRand() % 1000;
  int64_t currentMs = taosGetTimestampMs();
  sprintf(buf, "a%d#%"PRId64"#%d" , arbId, currentMs, randVal);
}
