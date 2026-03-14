/**
 * @file orvos_simple8b.h
 * @brief Simple-8b encoding interface for Orvos.
 *
 * This header delegates to the shared Simple-8b implementation in
 * lib/simple8b.h.  It is kept for backward compatibility so that existing
 * Orvos code that includes "access/orvos_simple8b.h" continues to work.
 *
 * Simple-8b is used throughout Orvos to pack TID deltas into 64-bit
 * codewords.  Each codeword's 4-bit selector determines how many
 * integers are packed and their bit width, enabling efficient storage
 * of small gaps between consecutive TIDs.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_simple8b.h
 */
#ifndef ORVOS_SIMPLE8B_H
#define ORVOS_SIMPLE8B_H

#include "lib/simple8b.h"

#endif							/* ORVOS_SIMPLE8B_H */
