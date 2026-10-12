#!/usr/bin/env bash
# =============================================================================
# Construye el LAYER de WeasyPrint para Lambda (python3.12, Amazon Linux 2023)
# =============================================================================
# WeasyPrint (decisión D20) necesita librerías nativas (pango, cairo, gdk-pixbuf,
# harfbuzz...) que NO vienen en el runtime de Lambda. Un "layer" es un zip con esas
# librerías + el paquete Python. Este script lo construye DENTRO de un contenedor
# Amazon Linux 2023 para que los binarios sean compatibles con Lambda.
#
# Requisito: Docker instalado (Docker Desktop en Windows, o ejecútalo en WSL2).
#
# Uso:
#   bash deploy/build_weasyprint_layer.sh
# Genera:  deploy/weasyprint-layer.zip
#
# NOTA: el pipeline funciona SIN este layer — generate_pdf usa reportlab como
# respaldo automático. Este layer es solo para el motor WeasyPrint (D20).
# =============================================================================
set -euo pipefail

OUT_DIR="$(cd "$(dirname "$0")" && pwd)"
ZIP="$OUT_DIR/weasyprint-layer.zip"

echo "Construyendo el layer de WeasyPrint en Amazon Linux 2023 (python3.12)..."

docker run --rm --entrypoint /bin/bash \
  -v "$OUT_DIR":/out \
  amazonlinux:2023 -c '
    set -e
    dnf -y install python3.12 python3.12-pip zip \
        pango cairo gdk-pixbuf2 libffi harfbuzz fontconfig \
        google-noto-sans-fonts > /dev/null

    mkdir -p /build/python /build/lib
    python3.12 -m pip install --upgrade pip > /dev/null
    python3.12 -m pip install weasyprint -t /build/python > /dev/null

    # Copiar las librerías nativas que WeasyPrint carga en runtime.
    # Lambda añade /opt/lib a LD_LIBRARY_PATH, así que van en lib/.
    for lib in libgobject-2.0 libglib-2.0 libpango-1.0 libpangocairo-1.0 \
               libpangoft2-1.0 libcairo libgdk_pixbuf-2.0 libffi \
               libharfbuzz libfontconfig libfreetype libpixman-1 \
               libgio-2.0 libgmodule-2.0 libpng16 libz; do
      find /usr/lib64 -name "${lib}*.so*" -exec cp -P {} /build/lib/ \; 2>/dev/null || true
    done

    cd /build && zip -r9 /out/weasyprint-layer.zip python lib > /dev/null
    echo "Layer construido: $(du -h /out/weasyprint-layer.zip | cut -f1)"
  '

echo ""
echo "LISTO -> $ZIP"
echo ""
echo "Publica el layer y anótalo (lo usa deploy_pipeline.ps1):"
echo "  aws lambda publish-layer-version \\"
echo "    --layer-name tfm-weasyprint \\"
echo "    --zip-file fileb://deploy/weasyprint-layer.zip \\"
echo "    --compatible-runtimes python3.12 \\"
echo "    --region eu-west-1"
echo ""
echo "Copia el LayerVersionArn que devuelve y pásalo a la Lambda generate-pdf:"
echo "  aws lambda update-function-configuration \\"
echo "    --function-name tfm-energia-generate-pdf \\"
echo "    --layers <LayerVersionArn> \\"
echo "    --region eu-west-1"
