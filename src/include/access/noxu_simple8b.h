/**
 * @file noxu_simple8b.h
 * @brief Simple-8b encoding interface for Noxu.
 *
 * This header delegates to the shared Simple-8b implementation in
 * lib/simple8b.h.  It is kept for backward compatibility so that existing
 * Noxu code that includes "access/noxu_simple8b.h" continues to work.
 *
 * Simple-8b is used throughout Noxu to pack TID deltas into 64-bit
 * codewords.  Each codeword's 4-bit selector determines how many
 * integers are packed and their bit width, enabling efficient storage
 * of small gaps between consecutive TIDs.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_simple8b.h
 */
#ifndef NOXU_SIMPLE8B_H
#define NOXU_SIMPLE8B_H

#include "lib/simple8b.h"

#endif							/* NOXU_SIMPLE8B_H */
