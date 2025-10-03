#!/bin/bash

# Feature Branch Testing Script
# Run this locally to test your feature branch before pushing

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🧪 Feature Branch Testing Script${NC}"

# Get current branch name
BRANCH_NAME=$(git branch --show-current)
echo -e "${YELLOW}Current branch: ${BRANCH_NAME}${NC}"

# Check if we're on a feature branch
if [[ $BRANCH_NAME != feature/* ]]; then
    echo -e "${RED}⚠️  Not on a feature branch. Create a feature branch first.${NC}"
    echo "Example: git checkout -b feature/medallion-architecture"
    exit 1
fi

# Validate code quality locally
echo -e "${GREEN}📝 Running code quality checks...${NC}"

# Check if black is installed
if command -v black &> /dev/null; then
    echo "Checking code formatting..."
    black --check src/ || {
        echo -e "${YELLOW}Code formatting issues found. Run: black src/${NC}"
    }
else
    echo "Installing black..."
    pip install black
    black --check src/
fi

# Check if flake8 is installed  
if command -v flake8 &> /dev/null; then
    echo "Running linter..."
    flake8 src/ --max-line-length=88 --ignore=E203,W503 || {
        echo -e "${YELLOW}Linting issues found. Check output above.${NC}"
    }
else
    echo "Installing flake8..."
    pip install flake8
    flake8 src/ --max-line-length=88 --ignore=E203,W503
fi

# Test local deployment (if Databricks CLI is configured)
if command -v databricks &> /dev/null; then
    echo -e "${GREEN}🚀 Testing local deployment...${NC}"
    
    # Create feature environment suffix
    FEATURE_SUFFIX=$(echo $BRANCH_NAME | sed 's/feature\///' | sed 's/[^a-zA-Z0-9]/-/g')
    
    echo "Testing deployment with suffix: -feature-${FEATURE_SUFFIX}"
    
    # Test deployment
    databricks bundle deploy --target dev --var="environment_suffix=-feature-${FEATURE_SUFFIX}" || {
        echo -e "${RED}❌ Deployment failed${NC}"
        exit 1
    }
    
    echo -e "${GREEN}✅ Deployment successful!${NC}"
    
    # Test jobs individually
    echo -e "${GREEN}🧪 Testing individual jobs...${NC}"
    
    echo "Testing hello job..."
    databricks bundle run hello_job --target dev || {
        echo -e "${RED}❌ Hello job failed${NC}"
        exit 1
    }
    
    echo "Testing bronze ingestion job..."
    databricks bundle run bronze_ingestion_job --target dev || {
        echo -e "${RED}❌ Bronze ingestion job failed${NC}"
        exit 1
    }
    
    echo -e "${GREEN}✅ All jobs tested successfully!${NC}"
    
else
    echo -e "${YELLOW}⚠️  Databricks CLI not configured. Skipping deployment test.${NC}"
    echo "Configure with: databricks auth configure"
fi

echo -e "${GREEN}🎉 Feature branch testing completed!${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Review changes: git diff master"
echo "2. Commit changes: git add . && git commit -m 'Add bronze ingestion'"
echo "3. Push feature branch: git push origin ${BRANCH_NAME}"
echo "4. Watch GitHub Actions deploy and test automatically"
echo "5. Create Pull Request when ready"
