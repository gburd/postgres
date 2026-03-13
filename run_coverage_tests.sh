#!/bin/bash
#
# Orvos Coverage Test Suite
# Runs comprehensive tests and generates coverage reports
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== Orvos Coverage Test Suite ==="
echo

# Configuration
REPO_ROOT="$(pwd)"
INSTALL_DIR="$REPO_ROOT/inst"
DATA_DIR="$REPO_ROOT/test-data"
LOG_FILE="$DATA_DIR/postgres.log"

# Step 1: Clean and configure
echo "Step 1: Configuring PostgreSQL with coverage support..."
./configure \
  --with-lz4 \
  --enable-cassert \
  --enable-debug \
  --enable-coverage \
  --prefix="$INSTALL_DIR" \
  > configure.log 2>&1

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Configuration complete${NC}"
else
  echo -e "${RED}✗ Configuration failed${NC}"
  cat configure.log
  exit 1
fi

# Step 2: Build
echo "Step 2: Building PostgreSQL..."
make -j$(nproc) > build.log 2>&1

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Build complete${NC}"
else
  echo -e "${RED}✗ Build failed${NC}"
  tail -50 build.log
  exit 1
fi

# Step 3: Install
echo "Step 3: Installing PostgreSQL..."
make install > install.log 2>&1

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Installation complete${NC}"
else
  echo -e "${RED}✗ Installation failed${NC}"
  cat install.log
  exit 1
fi

# Step 4: Initialize database
echo "Step 4: Initializing test database..."
rm -rf "$DATA_DIR"
"$INSTALL_DIR/bin/initdb" -D "$DATA_DIR" > initdb.log 2>&1

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Database initialized${NC}"
else
  echo -e "${RED}✗ Database initialization failed${NC}"
  cat initdb.log
  exit 1
fi

# Step 5: Start PostgreSQL
echo "Step 5: Starting PostgreSQL..."
"$INSTALL_DIR/bin/pg_ctl" -D "$DATA_DIR" -l "$LOG_FILE" start
sleep 2

if "$INSTALL_DIR/bin/pg_ctl" -D "$DATA_DIR" status > /dev/null 2>&1; then
  echo -e "${GREEN}✓ PostgreSQL started${NC}"
else
  echo -e "${RED}✗ PostgreSQL failed to start${NC}"
  cat "$LOG_FILE"
  exit 1
fi

# Step 6: Create test database
echo "Step 6: Creating test database..."
"$INSTALL_DIR/bin/createdb" test

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Test database created${NC}"
else
  echo -e "${RED}✗ Database creation failed${NC}"
  exit 1
fi

# Step 7: Run base regression tests
echo "Step 7: Running base orvos regression tests..."
cd src/test/regress

# Run base orvos test
./pg_regress \
  --use-existing \
  --dbname=test \
  --inputdir=. \
  --bindir="$INSTALL_DIR/bin" \
  orvos > orvos_test.log 2>&1

BASE_RESULT=$?
if [ $BASE_RESULT -eq 0 ]; then
  echo -e "${GREEN}✓ Base orvos tests PASSED${NC}"
  BASE_PASS=1
else
  echo -e "${YELLOW}⚠ Base orvos tests had failures (expected: bitmap scan, analyze)${NC}"
  BASE_PASS=0
  # Show diffs but don't fail
  if [ -f regression.diffs ]; then
    echo "Differences:"
    cat regression.diffs | head -100
  fi
fi

# Step 8: Run coverage tests
echo "Step 8: Running additional coverage tests..."
./pg_regress \
  --use-existing \
  --dbname=test \
  --inputdir=. \
  --bindir="$INSTALL_DIR/bin" \
  orvos_coverage > orvos_coverage_test.log 2>&1

COV_RESULT=$?
if [ $COV_RESULT -eq 0 ]; then
  echo -e "${GREEN}✓ Coverage tests PASSED${NC}"
  COV_PASS=1
else
  echo -e "${YELLOW}⚠ Coverage tests had failures${NC}"
  COV_PASS=0
  if [ -f regression.diffs ]; then
    echo "Differences:"
    cat regression.diffs | head -100
  fi
fi

cd "$REPO_ROOT"

# Step 9: Stop PostgreSQL
echo "Step 9: Stopping PostgreSQL..."
"$INSTALL_DIR/bin/pg_ctl" -D "$DATA_DIR" stop
sleep 1
echo -e "${GREEN}✓ PostgreSQL stopped${NC}"

# Step 10: Generate coverage report
echo "Step 10: Generating coverage report..."
make coverage-html > coverage.log 2>&1

if [ -f coverage/index.html ]; then
  echo -e "${GREEN}✓ Coverage report generated: coverage/index.html${NC}"
else
  echo -e "${YELLOW}⚠ Coverage report not generated${NC}"
fi

# Step 11: Extract coverage statistics for orvos files
echo
echo "=== Coverage Statistics for Orvos Files ==="
echo

if [ -d coverage ]; then
  cd coverage

  echo "File                        | Lines    | Functions | Branches"
  echo "---------------------------|----------|-----------|----------"

  for file in src/backend/access/orvos/*.c.gcov.html 2>/dev/null; do
    if [ -f "$file" ]; then
      filename=$(basename "$file" .c.gcov.html)

      # Extract coverage percentages (this is a simplified approach)
      line_cov=$(grep -o '[0-9.]*%' "$file" | head -1 || echo "N/A")
      func_cov=$(grep -o '[0-9.]*%' "$file" | sed -n '2p' || echo "N/A")
      branch_cov=$(grep -o '[0-9.]*%' "$file" | sed -n '3p' || echo "N/A")

      printf "%-26s | %-8s | %-9s | %s\n" "$filename" "$line_cov" "$func_cov" "$branch_cov"
    fi
  done

  cd "$REPO_ROOT"
else
  echo "Coverage directory not found"
fi

# Step 12: Use gcov for detailed per-file analysis
echo
echo "=== Detailed Coverage with gcov ==="
echo

cd src/backend/access/orvos
for file in *.c; do
  if [ -f "${file%.c}.gcda" ]; then
    echo "--- $file ---"
    gcov "$file" 2>/dev/null | grep -E "File|Lines executed|Branches executed|Taken at least once"
    echo
  fi
done
cd "$REPO_ROOT"

# Summary
echo
echo "=== Test Summary ==="
echo
if [ $BASE_PASS -eq 1 ]; then
  echo -e "${GREEN}✓ Base tests: PASSED${NC}"
else
  echo -e "${YELLOW}⚠ Base tests: PARTIAL (expected)${NC}"
fi

if [ $COV_PASS -eq 1 ]; then
  echo -e "${GREEN}✓ Coverage tests: PASSED${NC}"
else
  echo -e "${YELLOW}⚠ Coverage tests: PARTIAL${NC}"
fi

# Calculate overall coverage from gcov
echo
echo "=== Overall Coverage Summary ==="

cd src/backend/access/orvos
total_lines=0
covered_lines=0
total_branches=0
covered_branches=0

for file in *.c; do
  if [ -f "${file%.c}.gcda" ]; then
    # Parse gcov output for this file
    gcov_output=$(gcov "$file" 2>/dev/null)

    # Extract lines
    lines=$(echo "$gcov_output" | grep "Lines executed" | sed 's/.*:\([0-9.]*\)%.*/\1/')
    if [ -n "$lines" ]; then
      # This is percentage, we need to get actual counts from .gcov file
      if [ -f "${file}.gcov" ]; then
        total=$(grep -c "^[[:space:]]*[0-9#=-]" "${file}.gcov" || echo 0)
        covered=$(grep -c "^[[:space:]]*[1-9]" "${file}.gcov" || echo 0)
        total_lines=$((total_lines + total))
        covered_lines=$((covered_lines + covered))
      fi
    fi
  fi
done

cd "$REPO_ROOT"

if [ $total_lines -gt 0 ]; then
  overall_pct=$(echo "scale=2; $covered_lines * 100 / $total_lines" | bc)
  echo "Lines: $covered_lines / $total_lines ($overall_pct%)"

  if (( $(echo "$overall_pct > 90" | bc -l) )); then
    echo -e "${GREEN}✓ Line coverage goal achieved: >90%${NC}"
  elif (( $(echo "$overall_pct > 85" | bc -l) )); then
    echo -e "${YELLOW}⚠ Line coverage: $overall_pct% (goal: >90%)${NC}"
  else
    echo -e "${RED}✗ Line coverage below target: $overall_pct%${NC}"
  fi
else
  echo "Unable to calculate coverage statistics"
fi

echo
echo "=== Complete ==="
echo "Coverage report: coverage/index.html"
echo "Test logs: src/test/regress/*.log"
echo "PostgreSQL log: $LOG_FILE"
echo

exit 0
