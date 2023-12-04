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

#ifndef _TD_MND_ARBITRATOR_H_
#define _TD_MND_ARBITRATOR_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// #define ARBITRATOR_LOAD_VALUE(pArbitrator) \
//   (pArbitrator ? (pArbitrator->load.numOfQueryInQueue + pArbitrator->load.numOfFetchInQueue) : 0)

int32_t         mndInitArbitrator(SMnode *pMnode);
void            mndCleanupArbitrator(SMnode *pMnode);
SArbitratorObj *mndAcquireArbitrator(SMnode *pMnode, int32_t arbitratorId);
void            mndReleaseArbitrator(SMnode *pMnode, SArbitratorObj *pObj);
// int32_t         mndCreateArbitratorList(SMnode *pMnode, SArray **pList, int32_t limit);
int32_t         mndSetDropArbitratorInfoToTrans(SMnode *pMnode, STrans *pTrans, SArbitratorObj *pObj, bool force);
bool            mndArbitratorInDnode(SArbitratorObj *pArbitrator, int32_t dnodeId);
int32_t         mndSetCreateArbitratorCommitLogs(STrans *pTrans, SArbitratorObj *pObj);
int32_t         mndSetCreateArbitratorRedoActions(STrans *pTrans, SDnodeObj *pDnode, SArbitratorObj *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ARBITRATOR_H_*/
