#!/bin/bash
# Setup script using uv for GX project

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GX_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🚀 Setting up GX project with uv..."
echo "   Project root: $GX_ROOT"
echo ""

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed"
    echo ""
    echo "Install uv with:"
    echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo ""
    echo "Or with pip:"
    echo "   pip install uv"
    exit 1
fi

echo "✅ uv found: $(uv --version)"
echo ""

# Create virtual environment with uv
cd "$GX_ROOT"
echo "📦 Creating virtual environment with uv..."
if [ -d ".venv" ]; then
    echo "   ⚠️  .venv already exists, skipping creation"
else
    uv venv
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
    source .venv/Scripts/activate
else
    echo "   ⚠️  Could not find activation script, continuing anyway"
fi

# Install dependencies
echo "📥 Installing dependencies..."
uv pip install -e .

echo ""
echo "✅ Setup complete!"
echo ""
echo "To activate the environment:"
echo "   source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate"
echo ""
echo "Or use uv run directly (no activation needed):"
echo "   uv run python scripts/test_gx_setup.py"
echo ""
echo "See UV_SETUP.md for more details on using uv."
