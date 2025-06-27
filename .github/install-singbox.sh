#!/bin/bash
# Install sing-box in GitHub Actions (similar to your bot.py pattern)

set -e

echo "🔧 Installing sing-box for GitHub Actions..."

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64)
        ARCH="amd64"
        ;;
    aarch64)
        ARCH="arm64"
        ;;
    *)
        echo "❌ Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Get latest version with error handling
LATEST_VERSION=$(curl -s https://api.github.com/repos/SagerNet/sing-box/releases/latest | grep tag_name | cut -d '"' -f 4)

if [ -z "$LATEST_VERSION" ]; then
    echo "⚠️ Could not fetch latest version from GitHub API, using fallback version"
    # Use a known working version as fallback
    LATEST_VERSION="v1.8.0"
fi

echo "📦 Latest sing-box version: $LATEST_VERSION"

# Download and install
DOWNLOAD_URL="https://github.com/SagerNet/sing-box/releases/download/${LATEST_VERSION}/sing-box-${LATEST_VERSION#v}-linux-${ARCH}.tar.gz"
echo "⬇️ Downloading from: $DOWNLOAD_URL"

# Create temp directory
TEMP_DIR=$(mktemp -d)
cd $TEMP_DIR

# Download and extract
curl -L -o sing-box.tar.gz "$DOWNLOAD_URL"
tar -xzf sing-box.tar.gz

# Find the extracted directory (it may have version in name)
EXTRACTED_DIR=$(find . -type d -name "sing-box-*" | head -1)
if [ -z "$EXTRACTED_DIR" ]; then
    echo "❌ Could not find extracted sing-box directory"
    exit 1
fi

# Install binary
sudo cp "${EXTRACTED_DIR}/sing-box" /usr/local/bin/sing-box
sudo chmod +x /usr/local/bin/sing-box

# Verify installation
if /usr/local/bin/sing-box version; then
    echo "✅ sing-box installed successfully"
else
    echo "❌ sing-box installation verification failed"
    exit 1
fi

# Cleanup
cd /
rm -rf $TEMP_DIR

echo "🎉 sing-box installation complete!"