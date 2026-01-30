#!/bin/bash
set -euo pipefail

echo "=== S&P 500 Stock Streaming Platform - Conda Setup ==="

# Conda aktivieren
source "$(conda info --base)/etc/profile.d/conda.sh"

echo "0. Starte Conda..."
conda activate

# Prüfen ob Environment bereits existiert
if conda env list | grep -q "spx-stock-streaming"; then
    echo "Environment 'spx-stock-streaming' existiert bereits."
    read -p "Möchten Sie es neu erstellen? (j/n): " answer
    if [ "$answer" = "j" ]; then
        echo "Entferne altes Environment..."
        conda env remove -n spx-stock-streaming -y
    else
        echo "Überspringe Erstellung. Aktiviere bestehendes Environment..."
        conda activate spx-stock-streaming
        exit 0
    fi
fi

# Environment erstellen
echo "1. Erstelle Conda Environment..."
conda env create -f environment.yml

# Environment aktivieren
echo "2. Aktiviere Environment..."
conda activate spx-stock-streaming

# Python Version prüfen
#echo "3. Python Version:"
#python --version

# PySpark testen
#echo "4. Teste PySpark Installation..."
#python -c "import pyspark; print(f'PySpark Version: {pyspark.__version__}')"

# Redis testen
#echo "5. Teste Redis Client..."
#python -c "import redis; print('Redis Client: OK')"

# Dash testen
#echo "6. Teste Dash Installation..."
#python -c "import dash; print(f'Dash Version: {dash.__version__}')"

# Massive API testen
#echo "7. Teste Massive API Client..."
#python -c "import massive; print('Massive API Client: OK')"

# Optional: Spyder installieren (separat wegen möglicher Konflikte)
read -p "Möchten Sie Spyder IDE installieren? (j/n): " install_spyder
if [ "$install_spyder" = "j" ]; then
    echo "2.1 Installiere Spyder..."
    conda install -c anaconda spyder -y
fi

# Verzeichnisstruktur erstellen
echo "8. Erstelle Projektstruktur..."
mkdir -p src/{streaming,database,api,visualization/{layouts,components,callbacks},utils}
mkdir -p tests
mkdir -p config
mkdir -p kubernetes
mkdir -p logs

# .env Template erstellen
if [ ! -f .env ]; then
    echo "9. Erstelle .env Template..."
    cat > .env << 'EOF'
# Massive.io API Credentials
MASSIVE_API_KEY=your_api_key_here

# Database Configuration
DATABASE_URL=postgresql://sp500user:sp500pass@localhost:5432/sp500_data

# Redis Configuration
REDIS_URL=redis://localhost:6379

# Application Settings
ENVIRONMENT=development
LOG_LEVEL=INFO

# Spark Configuration
SPARK_MASTER_URL=spark://localhost:7077
EOF
fi

# Requirements Export (für Docker später)
echo "10. Exportiere requirements.txt..."
pip list --format=freeze > requirements-frozen.txt

echo ""
echo "=== Setup abgeschlossen! ==="
echo ""
echo "Nächste Schritte:"
echo "1. Bearbeite .env Datei mit deinen API Keys"
echo "2. Starte Docker Services: docker-compose up -d"
echo "3. Führe Tests aus: pytest tests/"
echo "4. Starte Entwicklung: python src/visualization/app.py"
echo ""
echo "Für Spyder: spyder"
echo "Für Jupyter: jupyter notebook"
echo ""
