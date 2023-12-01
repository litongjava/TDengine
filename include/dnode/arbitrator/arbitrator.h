/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, arb/or modify
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

#ifndef _TD_ARBITRATOR_H_
#define _TD_ARBITRATOR_H_

#include "tmsgcb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SArbitrator SArbitrator;

typedef struct {
  SMsgCb msgCb;
} SArbitratorOpt;

/* ------------------------ SArbitrator ------------------------ */
/**
 * @brief Start one Arbitrator in Dnode.
 *
 * @param pOption Option of the arbitrator.
 * @return SArbitrator* The arbitrator object.
 */
SArbitrator *arbOpen(const SArbitratorOpt *pOption);

/**
 * @brief Stop Arbitrator in Dnode.
 *
 * @param pArbitrator The arbitrator object to close.
 */
void arbClose(SArbitrator *pArbitrator);

/**
 * @brief Process a query or fetch message.
 *
 * @param pArbitrator The arbitrator object.
 * @param pMsg The request message
 */
int32_t arbProcessQueryMsg(SArbitrator *pArbitrator, int64_t ts, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_ARBITRATOR_H_*/
